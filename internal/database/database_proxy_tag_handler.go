package database

import (
	"errors"
	"fmt"
	"sort"

	"magpie/internal/api/dto"
	"magpie/internal/domain"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	ErrProxyTagNotFound     = errors.New("proxy tag not found")
	ErrProxyTagNameConflict = errors.New("proxy tag name already exists")
	ErrProxyAccessNotFound  = errors.New("proxy access not found")
)

func GetProxyTags(userID uint) ([]dto.ProxyTag, error) {
	if DB == nil {
		return nil, fmt.Errorf("database connection was not initialised")
	}

	var tags []domain.ProxyTag
	if err := DB.
		Where("user_id = ?", userID).
		Order("name_key ASC, id ASC").
		Find(&tags).Error; err != nil {
		return nil, err
	}

	return proxyTagsToDTO(tags), nil
}

func ValidateProxyTags(userID uint, tagIDs []uint64) error {
	if DB == nil {
		return fmt.Errorf("database connection was not initialised")
	}
	return requireProxyTags(DB, userID, normalizeUint64IDs(tagIDs))
}

func CreateProxyTag(userID uint, name, color string) (dto.ProxyTag, error) {
	if DB == nil {
		return dto.ProxyTag{}, fmt.Errorf("database connection was not initialised")
	}

	tag := domain.ProxyTag{UserID: userID, Name: name, Color: color}
	if err := tag.Normalize(); err != nil {
		return dto.ProxyTag{}, err
	}

	var existing int64
	if err := DB.Model(&domain.ProxyTag{}).
		Where("user_id = ? AND name_key = ?", userID, tag.NameKey).
		Count(&existing).Error; err != nil {
		return dto.ProxyTag{}, err
	}
	if existing > 0 {
		return dto.ProxyTag{}, ErrProxyTagNameConflict
	}

	if err := DB.Create(&tag).Error; err != nil {
		if isUniqueConstraintError(err) {
			return dto.ProxyTag{}, ErrProxyTagNameConflict
		}
		return dto.ProxyTag{}, err
	}

	return proxyTagToDTO(tag), nil
}

func UpdateProxyTag(userID uint, tagID uint64, name, color string) (dto.ProxyTag, error) {
	if DB == nil {
		return dto.ProxyTag{}, fmt.Errorf("database connection was not initialised")
	}
	if tagID == 0 {
		return dto.ProxyTag{}, ErrProxyTagNotFound
	}

	var updated domain.ProxyTag
	err := DB.Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("id = ? AND user_id = ?", tagID, userID).First(&updated).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrProxyTagNotFound
			}
			return err
		}

		updated.Name = name
		updated.Color = color
		if err := updated.Normalize(); err != nil {
			return err
		}

		var conflict int64
		if err := tx.Model(&domain.ProxyTag{}).
			Where("user_id = ? AND name_key = ? AND id <> ?", userID, updated.NameKey, tagID).
			Count(&conflict).Error; err != nil {
			return err
		}
		if conflict > 0 {
			return ErrProxyTagNameConflict
		}

		if err := tx.Save(&updated).Error; err != nil {
			if isUniqueConstraintError(err) {
				return ErrProxyTagNameConflict
			}
			return err
		}
		return nil
	})
	if err != nil {
		return dto.ProxyTag{}, err
	}

	return proxyTagToDTO(updated), nil
}

func DeleteProxyTag(userID uint, tagID uint64) error {
	if DB == nil {
		return fmt.Errorf("database connection was not initialised")
	}
	if tagID == 0 {
		return ErrProxyTagNotFound
	}

	result := DB.Where("id = ? AND user_id = ?", tagID, userID).Delete(&domain.ProxyTag{})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return ErrProxyTagNotFound
	}
	return nil
}

func ReplaceProxyTags(userID uint, proxyID uint64, tagIDs []uint64) ([]dto.ProxyTag, error) {
	if DB == nil {
		return nil, fmt.Errorf("database connection was not initialised")
	}
	if proxyID == 0 {
		return nil, ErrProxyAccessNotFound
	}

	normalizedTagIDs := normalizeUint64IDs(tagIDs)
	err := DB.Transaction(func(tx *gorm.DB) error {
		if err := requireProxyAccess(tx, userID, []uint64{proxyID}); err != nil {
			return err
		}
		if err := requireProxyTags(tx, userID, normalizedTagIDs); err != nil {
			return err
		}

		if err := tx.Where("user_id = ? AND proxy_id = ?", userID, proxyID).
			Delete(&domain.ProxyTagAssignment{}).Error; err != nil {
			return err
		}
		return createProxyTagAssignments(tx, userID, []uint64{proxyID}, normalizedTagIDs)
	})
	if err != nil {
		return nil, err
	}

	return getProxyTagsForProxy(userID, proxyID)
}

// AddProxyTagsToProxies adds import-selected tags without removing assignments
// already present on proxy accesses that the user imported before.
func AddProxyTagsToProxies(userID uint, proxyIDs, tagIDs []uint64) error {
	if DB == nil {
		return fmt.Errorf("database connection was not initialised")
	}

	normalizedProxyIDs := normalizeUint64IDs(proxyIDs)
	normalizedTagIDs := normalizeUint64IDs(tagIDs)
	if len(normalizedProxyIDs) == 0 || len(normalizedTagIDs) == 0 {
		return nil
	}

	return DB.Transaction(func(tx *gorm.DB) error {
		if err := requireProxyAccess(tx, userID, normalizedProxyIDs); err != nil {
			return err
		}
		if err := requireProxyTags(tx, userID, normalizedTagIDs); err != nil {
			return err
		}
		return createProxyTagAssignments(tx, userID, normalizedProxyIDs, normalizedTagIDs)
	})
}

func AttachProxyTagsToInfos(userID uint, proxies []dto.ProxyInfo) error {
	if len(proxies) == 0 {
		return nil
	}

	proxyIDs := make([]uint64, 0, len(proxies))
	for index := range proxies {
		proxies[index].Tags = []dto.ProxyTag{}
		if proxies[index].Id > 0 {
			proxyIDs = append(proxyIDs, uint64(proxies[index].Id))
		}
	}

	tagsByProxy, err := loadProxyTagsByProxyID(userID, proxyIDs)
	if err != nil {
		return err
	}
	for index := range proxies {
		if tags, ok := tagsByProxy[uint64(proxies[index].Id)]; ok {
			proxies[index].Tags = tags
		}
	}
	return nil
}

func getProxyTagsForProxy(userID uint, proxyID uint64) ([]dto.ProxyTag, error) {
	tagsByProxy, err := loadProxyTagsByProxyID(userID, []uint64{proxyID})
	if err != nil {
		return nil, err
	}
	if tags, ok := tagsByProxy[proxyID]; ok {
		return tags, nil
	}
	return []dto.ProxyTag{}, nil
}

func loadProxyTagsByProxyID(userID uint, proxyIDs []uint64) (map[uint64][]dto.ProxyTag, error) {
	result := make(map[uint64][]dto.ProxyTag)
	proxyIDs = normalizeUint64IDs(proxyIDs)
	if len(proxyIDs) == 0 {
		return result, nil
	}

	var rows []struct {
		ProxyID uint64 `gorm:"column:proxy_id"`
		ID      uint64 `gorm:"column:id"`
		Name    string `gorm:"column:name"`
		Color   string `gorm:"column:color"`
	}
	err := DB.Table("proxy_tag_assignments pta").
		Select("pta.proxy_id, pt.id, pt.name, pt.color").
		Joins("JOIN proxy_tags pt ON pt.id = pta.proxy_tag_id AND pt.user_id = pta.user_id").
		Where("pta.user_id = ? AND pta.proxy_id IN ?", userID, proxyIDs).
		Order("pta.proxy_id ASC, pt.name_key ASC, pt.id ASC").
		Scan(&rows).Error
	if err != nil {
		return nil, err
	}

	for _, row := range rows {
		result[row.ProxyID] = append(result[row.ProxyID], dto.ProxyTag{
			ID: row.ID, Name: row.Name, Color: row.Color,
		})
	}
	return result, nil
}

func requireProxyAccess(tx *gorm.DB, userID uint, proxyIDs []uint64) error {
	if len(proxyIDs) == 0 {
		return nil
	}

	var count int64
	if err := tx.Model(&domain.UserProxy{}).
		Where("user_id = ? AND proxy_id IN ?", userID, proxyIDs).
		Count(&count).Error; err != nil {
		return err
	}
	if count != int64(len(proxyIDs)) {
		return ErrProxyAccessNotFound
	}
	return nil
}

func requireProxyTags(tx *gorm.DB, userID uint, tagIDs []uint64) error {
	if len(tagIDs) == 0 {
		return nil
	}

	var count int64
	if err := tx.Model(&domain.ProxyTag{}).
		Where("user_id = ? AND id IN ?", userID, tagIDs).
		Count(&count).Error; err != nil {
		return err
	}
	if count != int64(len(tagIDs)) {
		return ErrProxyTagNotFound
	}
	return nil
}

func createProxyTagAssignments(tx *gorm.DB, userID uint, proxyIDs, tagIDs []uint64) error {
	if len(proxyIDs) == 0 || len(tagIDs) == 0 {
		return nil
	}

	assignments := make([]domain.ProxyTagAssignment, 0, len(proxyIDs)*len(tagIDs))
	for _, proxyID := range proxyIDs {
		for _, tagID := range tagIDs {
			assignments = append(assignments, domain.ProxyTagAssignment{
				UserID: userID, ProxyID: proxyID, ProxyTagID: tagID,
			})
		}
	}

	return tx.Clauses(clause.OnConflict{DoNothing: true}).
		CreateInBatches(assignments, 500).Error
}

func normalizeUint64IDs(values []uint64) []uint64 {
	if len(values) == 0 {
		return nil
	}

	seen := make(map[uint64]struct{}, len(values))
	normalized := make([]uint64, 0, len(values))
	for _, value := range values {
		if value == 0 {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		normalized = append(normalized, value)
	}
	sort.Slice(normalized, func(i, j int) bool { return normalized[i] < normalized[j] })
	return normalized
}

func proxyTagsToDTO(tags []domain.ProxyTag) []dto.ProxyTag {
	result := make([]dto.ProxyTag, 0, len(tags))
	for _, tag := range tags {
		result = append(result, proxyTagToDTO(tag))
	}
	return result
}

func proxyTagToDTO(tag domain.ProxyTag) dto.ProxyTag {
	return dto.ProxyTag{ID: tag.ID, Name: tag.Name, Color: tag.Color}
}
