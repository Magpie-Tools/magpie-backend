package database

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"magpie/internal/domain"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func queueProxyReputationRefresh(tx *gorm.DB, proxyIDs []uint64) error {
	if len(proxyIDs) == 0 || !tx.Migrator().HasTable(&domain.ProxyReputationRefresh{}) {
		return nil
	}
	proxyIDs = append([]uint64(nil), proxyIDs...)
	sort.Slice(proxyIDs, func(i, j int) bool { return proxyIDs[i] < proxyIDs[j] })
	now := time.Now().UTC()
	for start := 0; start < len(proxyIDs); start += 1000 {
		jobs := make([]domain.ProxyReputationRefresh, 0, min(1000, len(proxyIDs)-start))
		for _, id := range proxyIDs[start:min(start+1000, len(proxyIDs))] {
			jobs = append(jobs, domain.ProxyReputationRefresh{ProxyID: id, Version: 1, RequestedAt: now})
		}
		if err := tx.Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "proxy_id"}},
			DoUpdates: clause.Assignments(map[string]any{
				"version": gorm.Expr("proxy_reputation_refreshes.version + 1"),
			}),
		}).Create(&jobs).Error; err != nil {
			return err
		}
	}
	return nil
}

// ClaimProxyReputationRefreshes distributes persisted work across instances.
// Claims expire after worker crashes and do not hold a lock while calculating.
func ClaimProxyReputationRefreshes(ctx context.Context, limit int, lease time.Duration) ([]domain.ProxyReputationRefresh, error) {
	if DB == nil {
		return nil, fmt.Errorf("database not initialised")
	}
	if limit < 1 {
		return nil, nil
	}
	db := DB.WithContext(ctx)
	now := time.Now().UTC().Truncate(time.Microsecond)
	until := now.Add(lease)
	var jobs []domain.ProxyReputationRefresh
	if isPostgresDialect(db) {
		err := db.Raw(`UPDATE proxy_reputation_refreshes SET lease_until = ?
			WHERE proxy_id IN (SELECT proxy_id FROM proxy_reputation_refreshes
				WHERE lease_until <= ? ORDER BY requested_at, proxy_id
				LIMIT ? FOR UPDATE SKIP LOCKED)
			RETURNING proxy_id, version, requested_at, lease_until`, until, now, limit).Scan(&jobs).Error
		return jobs, err
	}
	err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("lease_until <= ?", now).Order("requested_at, proxy_id").Limit(limit).Find(&jobs).Error; err != nil {
			return err
		}
		for i := range jobs {
			if err := tx.Model(&domain.ProxyReputationRefresh{}).Where("proxy_id = ?", jobs[i].ProxyID).Update("lease_until", until).Error; err != nil {
				return err
			}
			jobs[i].LeaseUntil = until
		}
		return nil
	})
	return jobs, err
}

func CompleteProxyReputationRefreshes(ctx context.Context, jobs []domain.ProxyReputationRefresh, succeeded bool) error {
	if DB == nil {
		return fmt.Errorf("database not initialised")
	}
	return DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for start := 0; start < len(jobs); start += 1000 {
			chunk := jobs[start:min(start+1000, len(jobs))]
			ownedRows, completedRows := make([]string, len(chunk)), make([]string, len(chunk))
			ownedArgs, completedArgs := make([]any, 0, len(chunk)*2), make([]any, 0, len(chunk)*3)
			for i, job := range chunk {
				ownedRows[i], completedRows[i] = "(?, ?)", "(?, ?, ?)"
				ownedArgs = append(ownedArgs, job.ProxyID, job.LeaseUntil)
				completedArgs = append(completedArgs, job.ProxyID, job.Version, job.LeaseUntil)
			}
			if succeeded {
				if err := tx.Exec("DELETE FROM proxy_reputation_refreshes WHERE (proxy_id, version, lease_until) IN ("+
					strings.Join(completedRows, ",")+")", completedArgs...).Error; err != nil {
					return err
				}
			}
			until := time.Time{}
			if !succeeded {
				until = time.Now().UTC().Add(5 * time.Second)
			}
			args := append([]any{until}, ownedArgs...)
			if err := tx.Exec("UPDATE proxy_reputation_refreshes SET lease_until = ? WHERE (proxy_id, lease_until) IN ("+
				strings.Join(ownedRows, ",")+")", args...).Error; err != nil {
				return err
			}
		}
		return nil
	})
}

func ProxyReputationRefreshBacklog(ctx context.Context) (int64, time.Time, error) {
	if DB == nil {
		return 0, time.Time{}, fmt.Errorf("database not initialised")
	}
	var count int64
	db := DB.WithContext(ctx)
	if err := db.Model(&domain.ProxyReputationRefresh{}).Count(&count).Error; err != nil {
		return 0, time.Time{}, err
	}
	var oldest []domain.ProxyReputationRefresh
	if err := db.Order("requested_at").Limit(1).Find(&oldest).Error; err != nil {
		return 0, time.Time{}, err
	}
	if len(oldest) == 0 {
		return count, time.Time{}, nil
	}
	return count, oldest[0].RequestedAt, nil
}
