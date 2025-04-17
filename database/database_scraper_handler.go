package database

import (
	"errors"
	"gorm.io/gorm"

	"magpie/models"
)

// GetScrapingSourcesOfUsers returns all URLs associated with the given user.
func GetScrapingSourcesOfUsers(userID uint) []string {
	var user models.User
	if err := DB.Preload("ScrapeSites").First(&user, userID).Error; err != nil {
		return nil
	}

	out := make([]string, 0, len(user.ScrapeSites))
	for _, s := range user.ScrapeSites {
		out = append(out, s.URL)
	}
	return out
}

// SaveScrapingSourcesOfUsers replaces the user’s current list with `sources`.
func SaveScrapingSourcesOfUsers(userID int, sources []string) error {
	if len(sources) == 0 {
		return errors.New("no sources provided")
	}

	return DB.Transaction(func(tx *gorm.DB) error {
		// 1. Load / create every ScrapeSite referenced in `sources`
		sites := make([]models.ScrapeSite, 0, len(sources))
		for _, url := range sources {
			if url == "" {
				continue
			}
			var site models.ScrapeSite
			if err := tx.Where("url = ?", url).
				FirstOrCreate(&site, &models.ScrapeSite{URL: url}).Error; err != nil {
				return err
			}
			sites = append(sites, site)
		}

		// 2. Attach them to the user, replacing any previous association
		var user models.User
		if err := tx.First(&user, userID).Error; err != nil {
			return err
		}
		return tx.Model(&user).Association("ScrapeSites").Replace(&sites)
	})
}
