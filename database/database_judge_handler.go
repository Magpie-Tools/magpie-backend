package database

import (
	"magpie/models"
)

func GetJudgesRegexFromString(judges []*models.Judge) []*models.JudgeWithRegex {
	fullStrings := getStringListFromJudges(judges)

	var results []struct {
		judge models.Judge
		regex string
	}

	if err := DB.Table("user_judges").
		Select("judges.*, user_judges.regex").
		Joins("JOIN judges ON user_judges.judge_id = judges.id").
		Where("full_string IN ?", fullStrings).
		Scan(&results).Error; err != nil {
		return nil
	}

	ptrResults := make([]*models.JudgeWithRegex, len(results))
	for i, item := range results {
		ptrResults[i] = &models.JudgeWithRegex{
			Judge: &item.judge,
			Regex: item.regex,
		}
	}

	return ptrResults
}

func GetJudgesFromString(judges []*models.Judge) []*models.Judge {
	fullStrings := getStringListFromJudges(judges)

	DB.Where("full_string IN (?)", fullStrings).Find(&judges)
	return judges
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
