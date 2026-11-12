package account

import "magpie/internal/domain"
import "magpie/internal/platform/pg"

func GetUserFromId(id uint) domain.User {
	var users domain.User
	pg.DB.Where("id = ?", id).First(&users)
	return users
}

func ChangePassword(userID uint, password string) error {
	err := pg.DB.Model(&domain.User{}).Where("ID = ?", userID).Update("password", password).Error
	return err
}

func ChangePasswordIfCurrent(userID uint, expectedPassword string, newPassword string) (bool, error) {
	result := pg.DB.Model(&domain.User{}).
		Where("ID = ? AND password = ?", userID, expectedPassword).
		Update("password", newPassword)
	if result.Error != nil {
		return false, result.Error
	}
	return result.RowsAffected > 0, nil
}
