package database

import (
	"context"
	"fmt"
	"time"

	"magpie/internal/platform/pg"
	"magpie/internal/platform/schema"
	"magpie/internal/support"
)

// MaintainProxyStatisticsPartitions runs for an already partitioned schema,
// including installations that disable application schema migrations.
func MaintainProxyStatisticsPartitions(ctx context.Context, now time.Time) error {
	if pg.DB == nil || !pg.IsPostgresDialector(pg.DB) {
		return nil
	}
	db := pg.DB.WithContext(ctx)
	partitioned, err := schema.IsProxyStatisticsPartitioned(db)
	if err != nil || !partitioned {
		return err
	}
	if err := schema.EnsureProxyStatisticsDefaultPartition(db); err != nil {
		return err
	}
	months := support.GetEnvInt(schema.EnvProxyStatisticsPartitionPrecreateMonths, schema.DefaultPartitionPrecreateMonths)
	if months < 1 {
		months = schema.DefaultPartitionPrecreateMonths
	}
	start := schema.MonthStartUTC(now)
	for month := 0; month <= months; month++ {
		if err := schema.EnsureProxyStatisticsMonthlyPartition(db, start.AddDate(0, month, 0)); err != nil {
			return err
		}
	}
	return nil
}

func DropProxyStatisticsPartitionsOlderThan(ctx context.Context, olderThan time.Time, maxDrops int) (int, error) {
	if pg.DB == nil || olderThan.IsZero() {
		return 0, nil
	}
	if maxDrops <= 0 {
		maxDrops = 1
	}

	db := pg.DB
	if ctx != nil {
		db = db.WithContext(ctx)
	}
	if !pg.IsPostgresDialector(db) {
		return 0, nil
	}

	partitioned, err := schema.IsProxyStatisticsPartitioned(db)
	if err != nil || !partitioned {
		return 0, err
	}

	partitions, err := schema.ListProxyStatisticsPartitions(db)
	if err != nil {
		return 0, err
	}

	dropped := 0
	for _, partition := range partitions {
		if partition.UpperBound.After(olderThan) {
			continue
		}
		if partition.Name == "proxy_statistics_default" {
			continue
		}

		hasLatest, err := schema.PartitionHasLatestStatisticReferences(db, partition.Name)
		if err != nil {
			return dropped, err
		}
		if hasLatest {
			continue
		}

		if err := db.Exec(`DROP TABLE IF EXISTS ` + pg.QuoteIdentifier(partition.Name)).Error; err != nil {
			return dropped, err
		}
		dropped++
		if dropped >= maxDrops {
			break
		}
	}

	return dropped, nil
}

func ValidateProxyScrapeSiteCascadeConstraints(ctx context.Context) error {
	if pg.DB == nil {
		return fmt.Errorf("database not initialised")
	}

	db := pg.DB
	if ctx != nil {
		db = db.WithContext(ctx)
	}
	if !pg.IsPostgresDialect(db) {
		return nil
	}

	const validationSQL = `
DO $$
DECLARE
	constraint_row RECORD;
BEGIN
	FOR constraint_row IN
		SELECT conname
		FROM pg_constraint
		WHERE conrelid = 'proxy_scrape_site'::regclass
		  AND contype = 'f'
		  AND confdeltype = 'c'
		  AND NOT convalidated
	LOOP
		EXECUTE format(
			'ALTER TABLE proxy_scrape_site VALIDATE CONSTRAINT %I',
			constraint_row.conname
		);
	END LOOP;
END $$;
`

	if err := db.Exec(validationSQL).Error; err != nil {
		return fmt.Errorf("validate proxy scrape-source cascade constraints: %w", err)
	}
	return nil
}
