package judgestore

import (
	"gorm.io/gorm/clause"
	"time"

	"magpie/internal/api/dto"
	"magpie/internal/domain"
	"magpie/internal/platform/pg"
)

func GetWorkspacesThatDontHaveJudges() []domain.Workspace {
	var workspaces []domain.Workspace
	pg.DB.Where("id NOT IN (SELECT DISTINCT workspace_id FROM user_judges)").Find(&workspaces)
	return workspaces
}

func AddWorkspaceJudgesRelation(workspaces []domain.Workspace, judges []*domain.JudgeWithRegex) error {
	var workspaceJudges []domain.WorkspaceJudge

	for _, workspace := range workspaces {
		for _, judge := range judges {
			workspaceJudges = append(workspaceJudges, domain.WorkspaceJudge{
				WorkspaceID: workspace.ID,
				JudgeID:     judge.Judge.ID,
				Regex:       judge.Regex,
			})
		}
	}

	if len(workspaceJudges) > 0 {
		if err := pg.DB.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "workspace_id"}, {Name: "judge_id"}},
			DoNothing: true,
		}).Create(&workspaceJudges).Error; err != nil {
			return err
		}
	}

	return nil
}

func GetAllWorkspaceJudgeRelations() ([]domain.WorkspaceJudge, []domain.JudgeWithRegex) {
	var workspaceJudges []domain.WorkspaceJudge
	if err := pg.DB.Find(&workspaceJudges).Error; err != nil {
		return nil, nil
	}

	var results []struct {
		ID         uint   `gorm:"column:id"`
		FullString string `gorm:"column:full_string"`
		CreatedAt  time.Time
		Regex      string `gorm:"column:regex"`
	}

	if err := pg.DB.Table("user_judges").
		Select("judges.id, judges.full_string, judges.created_at, user_judges.regex").
		Joins("JOIN judges ON user_judges.judge_id = judges.id").
		Scan(&results).Error; err != nil {
		return nil, nil
	}

	var judgesWithRegex []domain.JudgeWithRegex
	for _, result := range results {
		judge := &domain.Judge{
			ID:         result.ID,
			FullString: result.FullString,
			CreatedAt:  result.CreatedAt,
		}
		judge.SetUp()
		judgesWithRegex = append(judgesWithRegex, domain.JudgeWithRegex{
			Judge: judge,
			Regex: result.Regex,
		})
	}

	return workspaceJudges, judgesWithRegex
}

func GetWorkspaceJudges(workspaceID uint) []dto.SimpleWorkspaceJudge {
	var results []dto.SimpleWorkspaceJudge

	if err := pg.DB.Table("user_judges").
		Select("judges.full_string AS Url, user_judges.regex AS Regex").
		Joins("JOIN judges ON user_judges.judge_id = judges.id").
		Where("user_judges.workspace_id = ?", workspaceID).
		Scan(&results).Error; err != nil {
		return nil
	}

	return results
}

func GetWorkspaceJudgesWithRegex(workspaceID uint) ([]domain.JudgeWithRegex, error) {
	var rows []struct {
		ID         uint      `gorm:"column:id"`
		FullString string    `gorm:"column:full_string"`
		CreatedAt  time.Time `gorm:"column:created_at"`
		Regex      string    `gorm:"column:regex"`
	}

	if err := pg.DB.Table("user_judges").
		Select("judges.id, judges.full_string, judges.created_at, user_judges.regex").
		Joins("JOIN judges ON user_judges.judge_id = judges.id").
		Where("user_judges.workspace_id = ?", workspaceID).
		Scan(&rows).Error; err != nil {
		return nil, err
	}

	result := make([]domain.JudgeWithRegex, 0, len(rows))
	for _, row := range rows {
		judge := &domain.Judge{
			ID:         row.ID,
			FullString: row.FullString,
			CreatedAt:  row.CreatedAt,
		}
		judge.SetUp()
		judge.UpdateIp()
		result = append(result, domain.JudgeWithRegex{
			Judge: judge,
			Regex: row.Regex,
		})
	}

	return result, nil
}
