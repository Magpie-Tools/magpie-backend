package helper

import "magpie/models"

func GetUserIdsFromList(users []models.User) []uint {
	var userIds []uint

	for _, user := range users {
		userIds = append(userIds, user.ID)
	}

	return userIds
}
