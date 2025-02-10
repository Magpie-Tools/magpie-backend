package database

import (
	"magpie/models"
)

func GetJudgesRegexFromString(judges []*models.Judge) []*models.JudgeWithRegex {
	fullStrings := getStringListFromJudges(judges)

	var results []*models.JudgeWithRegex
	DB.Raw(`
		SELECT j.*, uj.regex 
		FROM judges j
		JOIN user_judges uj ON j.id = uj.judge_id
		WHERE j.full_string IN (?)
	`, fullStrings).Scan(&results)

	return results
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
