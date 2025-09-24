package support

import "magpie/internal/domain"

func GetUserIdsFromList(users []domain.User) []uint {
	var userIds []uint

	for _, user := range users {
		userIds = append(userIds, user.ID)
	}

	return userIds
}
