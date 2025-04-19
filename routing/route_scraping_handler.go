package routing

import (
	"encoding/json"
	"github.com/charmbracelet/log"
	"magpie/authorization"
	"magpie/database"
	"net/http"
	"strconv"
)

func getScrapeSourcesCount(w http.ResponseWriter, r *http.Request) {
	userID, userErr := authorization.GetUserIDFromRequest(r)
	if userErr != nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	json.NewEncoder(w).Encode(database.GetAllScrapeSiteCountOfUser(userID))
}

func getScrapeSourcePage(w http.ResponseWriter, r *http.Request) {
	userID, userErr := authorization.GetUserIDFromRequest(r)
	if userErr != nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	page, err := strconv.Atoi(r.PathValue("page"))
	if err != nil {
		log.Error("error converting page to int", "error", err.Error())
		http.Error(w, "Invalid page", http.StatusBadRequest)
		return
	}

	scrapeSiteInfoPages := database.GetScrapeSiteInfoPage(userID, page)

	json.NewEncoder(w).Encode(scrapeSiteInfoPages)
}

func deleteScrapingSources(w http.ResponseWriter, r *http.Request) {
	userID, userErr := authorization.GetUserIDFromRequest(r)
	if userErr != nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	var scrapingSource []int

	if err := json.NewDecoder(r.Body).Decode(&scrapingSource); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	database.DeleteScrapeSiteRelation(userID, scrapingSource)

	json.NewEncoder(w).Encode("Scraping Sources deleted successfully")
}
