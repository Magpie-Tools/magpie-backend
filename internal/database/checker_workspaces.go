package database

import (
	"magpie/internal/domain"
	"magpie/internal/platform/pg"
)

func GetWorkspacesByIDsForChecker(ids []uint) (map[uint]domain.Workspace, error) {
	ids = normalizeWorkspaceIDs(ids)
	if len(ids) == 0 {
		return nil, nil
	}
	var workspaces []domain.Workspace
	if err := pg.DB.Where("id IN ?", ids).Find(&workspaces).Error; err != nil {
		return nil, err
	}
	result := make(map[uint]domain.Workspace, len(workspaces))
	for _, workspace := range workspaces {
		result[workspace.ID] = workspace
	}
	return result, nil
}
