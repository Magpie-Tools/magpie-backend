package database

import (
	"magpie/models"
	"time"
)

func GetJudgesRegexFromString(judges []*models.Judge) []*models.JudgeWithRegex {
	fullStrings := getStringListFromJudges(judges)

	var results []struct {
		ID         uint      `gorm:"column:id"`
		FullString string    `gorm:"column:full_string"`
		CreatedAt  time.Time `gorm:"column:created_at"`
		Regex      string    `gorm:"column:regex"`
	}

	if err := DB.Table("user_judges").
		Select("judges.id, judges.full_string, judges.created_at, user_judges.regex").
		Joins("JOIN judges ON user_judges.judge_id = judges.id").
		Where("judges.full_string IN (?)", fullStrings).
		Scan(&results).Error; err != nil {
		return nil
	}

	ptrResults := make([]*models.JudgeWithRegex, len(results))
	for i, item := range results {
		judge := &models.Judge{
			ID:         item.ID,
			FullString: item.FullString,
			CreatedAt:  item.CreatedAt,
		}

		judge.SetUp()
		judge.UpdateIp()
		ptrResults[i] = &models.JudgeWithRegex{
			Judge: judge,
			Regex: item.Regex,
		}
	}

	return ptrResults
}

func GetJudgesFromString(judges []*models.Judge) []*models.Judge {
	fullStrings := getStringListFromJudges(judges)

	DB.Where("full_string IN (?)", fullStrings).Find(&judges)
	return judges
}

func GetJudgeFromString(judge string) *models.Judge {
	var realJudge *models.Judge

	DB.Where("full_string = ?", judge).First(&realJudge)

	return realJudge
}

func AddJudges(judges []*models.Judge) error {
	result := DB.Create(judges)
	return result.Error
}

func getStringListFromJudges(judges []*models.Judge) []string {
	fullStrings := make([]string, len(judges))
	for i, j := range judges {
		fullStrings[i] = j.FullString
	}
	return fullStrings
}
