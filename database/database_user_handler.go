package database

import (
	"gorm.io/gorm/clause"
	"magpie/models"
)

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
		judge models.Judge
		regex string
	}

	if err := DB.Table("user_judges").
		Select("judges.*, user_judges.regex").
		Joins("JOIN judges ON user_judges.judge_id = judges.id").
		Scan(&results).Error; err != nil {
		return nil, nil
	}

	var judgesWithRegex []models.JudgeWithRegex
	for _, result := range results {
		judgesWithRegex = append(judgesWithRegex, models.JudgeWithRegex{
			Judge: &result.judge,
			Regex: result.regex,
		})
	}

	return userJudges, judgesWithRegex
}
