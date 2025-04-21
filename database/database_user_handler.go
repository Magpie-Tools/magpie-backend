package database

import (
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"magpie/models"
	"magpie/models/routeModels"
	"time"
)

func GetUserFromId(id uint) models.User {
	var users models.User
	DB.Where("id = ?", id).First(&users)
	return users
}

func GetUsersThatDontHaveJudges() []models.User {
	var users []models.User
	DB.Where("id NOT IN (SELECT DISTINCT user_id FROM user_judges)").Find(&users)
	return users
}

// AddUserJudgesRelation cannot normally fail because of to many parameters because
// users start with the default judges anyway
func AddUserJudgesRelation(users []models.User, judges []*models.JudgeWithRegex) error {
	var userJudges []models.UserJudge

	for _, user := range users {
		for _, judge := range judges {
			userJudges = append(userJudges, models.UserJudge{
				UserID:  user.ID,
				JudgeID: judge.Judge.ID,
				Regex:   judge.Regex,
			})
		}
	}

	if len(userJudges) > 0 {
		if err := DB.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "user_id"}, {Name: "judge_id"}},
			DoNothing: true,
		}).Create(&userJudges).Error; err != nil {
			return err
		}
	}

	return nil
}

func GetAllUserJudgeRelations() ([]models.UserJudge, []models.JudgeWithRegex) {
	var userJudges []models.UserJudge
	if err := DB.Find(&userJudges).Error; err != nil {
		return nil, nil
	}

	var results []struct {
		ID         uint   `gorm:"column:id"`
		FullString string `gorm:"column:full_string"`
		CreatedAt  time.Time
		Regex      string `gorm:"column:regex"`
	}

	if err := DB.Table("user_judges").
		Select("judges.id, judges.full_string, judges.created_at, user_judges.regex").
		Joins("JOIN judges ON user_judges.judge_id = judges.id").
		Scan(&results).Error; err != nil {
		return nil, nil
	}

	var judgesWithRegex []models.JudgeWithRegex
	for _, result := range results {
		judge := &models.Judge{
			ID:         result.ID,
			FullString: result.FullString,
			CreatedAt:  result.CreatedAt,
		}
		judge.SetUp()
		judgesWithRegex = append(judgesWithRegex, models.JudgeWithRegex{
			Judge: judge,
			Regex: result.Regex,
		})
	}

	return userJudges, judgesWithRegex
}

func UpdateUserSettings(userID uint, settings routeModels.UserSettings) error {
	// Wrap everything in a single transaction so either all changes
	// happen or none do.
	return DB.Transaction(func(tx *gorm.DB) error {

		/* ─── 1.  Update primitive columns on the User row ─────────────────────── */
		updates := map[string]interface{}{
			"HTTPProtocol":     settings.HTTPProtocol,
			"HTTPSProtocol":    settings.HTTPSProtocol,
			"SOCKS4Protocol":   settings.SOCKS4Protocol,
			"SOCKS5Protocol":   settings.SOCKS5Protocol,
			"Timeout":          settings.Timeout,
			"Retries":          settings.Retries,
			"UseHttpsForSocks": settings.UseHttpsForSocks,
		}
		if err := tx.Model(&models.User{}).
			Where("id = ?", userID).
			Updates(updates).Error; err != nil {
			return err
		}

		keepIDs := make([]uint, 0, len(settings.SimpleUserJudges))

		for _, s := range settings.SimpleUserJudges {
			judge := models.Judge{FullString: s.Url}
			if err := tx.
				Clauses(clause.OnConflict{DoNothing: true}).
				FirstOrCreate(&judge, judge).Error; err != nil {
				return err
			}

			keepIDs = append(keepIDs, judge.ID)

			uj := models.UserJudge{
				UserID:  userID,
				JudgeID: judge.ID,
				Regex:   s.Regex,
			}
			if err := tx.
				Clauses(clause.OnConflict{
					Columns:   []clause.Column{{Name: "user_id"}, {Name: "judge_id"}},
					DoUpdates: clause.AssignmentColumns([]string{"regex"}),
				}).
				Create(&uj).Error; err != nil {
				return err
			}
		}

		if err := tx.
			Where("user_id = ? AND judge_id NOT IN ?", userID, keepIDs).
			Delete(&models.UserJudge{}).Error; err != nil {
			return err
		}

		return nil
	})
}

func GetUserJudges(userid uint) []routeModels.SimpleUserJudge {
	var results []routeModels.SimpleUserJudge

	if err := DB.Table("user_judges").
		Select("judges.full_string AS Url, user_judges.regex AS Regex").
		Joins("JOIN judges ON user_judges.judge_id = judges.id").
		Where("user_judges.user_id = ?", userid).
		Scan(&results).Error; err != nil {
		return nil
	}

	return results
}

func GetDashboardInfo(userid uint) routeModels.DashboardInfo {
	var info routeModels.DashboardInfo
	// cut‑off for “this week”
	weekAgo := time.Now().AddDate(0, 0, -7)

	// 1) TotalChecks
	DB.Model(&models.ProxyStatistic{}).
		Joins("JOIN user_proxies up ON up.proxy_id = proxy_statistics.proxy_id").
		Where("up.user_id = ?", userid).
		Count(&info.TotalChecks)

	// 2) TotalChecksWeek
	DB.Model(&models.ProxyStatistic{}).
		Joins("JOIN user_proxies up ON up.proxy_id = proxy_statistics.proxy_id").
		Where("up.user_id = ? AND proxy_statistics.created_at >= ?", userid, weekAgo).
		Count(&info.TotalChecksWeek)

	// 3) TotalScraped
	DB.Table("proxy_scrape_site AS ps").
		Joins("JOIN user_proxies up ON up.proxy_id = ps.proxy_id").
		Where("up.user_id = ?", userid).
		Count(&info.TotalScraped)

	// 4) TotalScrapedWeek
	DB.Table("proxy_scrape_site AS ps").
		Joins("JOIN user_proxies up ON up.proxy_id = ps.proxy_id").
		Where("up.user_id = ? AND ps.created_at >= ?", userid, weekAgo).
		Count(&info.TotalScrapedWeek)

	// 5) JudgeValidProxies – one row per judge, with counts by anonymity level
	type jvp struct {
		JudgeUrl           string `json:"judge_url"`
		EliteProxies       uint   `json:"elite_proxies"`
		AnonymousProxies   uint   `json:"anonymous_proxies"`
		TransparentProxies uint   `json:"transparent_proxies"`
	}
	var tmp []jvp

	DB.Model(&models.ProxyStatistic{}).
		Select(
			"j.full_string AS judge_url, "+
				"SUM(CASE WHEN al.name = 'elite' THEN 1 ELSE 0 END)       AS elite_proxies, "+
				"SUM(CASE WHEN al.name = 'anonymous' THEN 1 ELSE 0 END)   AS anonymous_proxies, "+
				"SUM(CASE WHEN al.name = 'transparent' THEN 1 ELSE 0 END) AS transparent_proxies",
		).
		Joins("JOIN user_judges uj ON uj.judge_id = proxy_statistics.judge_id").
		Joins("JOIN judges j ON j.id = proxy_statistics.judge_id").
		Joins("JOIN anonymity_levels al ON al.id = proxy_statistics.level_id").
		Where("uj.user_id = ? AND proxy_statistics.alive = TRUE", userid).
		Group("j.id, j.full_string").
		Scan(&tmp)

	// assign into the routeModels struct
	for _, row := range tmp {
		info.JudgeValidProxies = append(info.JudgeValidProxies, struct {
			JudgeUrl           string `json:"judge_url"`
			EliteProxies       uint   `json:"elite_proxies"`
			AnonymousProxies   uint   `json:"anonymous_proxies"`
			TransparentProxies uint   `json:"transparent_proxies"`
		}{
			JudgeUrl:           row.JudgeUrl,
			EliteProxies:       row.EliteProxies,
			AnonymousProxies:   row.AnonymousProxies,
			TransparentProxies: row.TransparentProxies,
		})
	}

	return info
}
