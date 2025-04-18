package routing

import (
	"encoding/json"
	"fmt"
	"magpie/authorization"
	"magpie/database"
	"magpie/helper"
	"magpie/models/routeModels"
	"magpie/scraper/redis_queue"
	"net/http"
)

func getUserSettings(w http.ResponseWriter, r *http.Request) {
	userID, userErr := authorization.GetUserIDFromRequest(r)
	if userErr != nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	user := database.GetUserFromId(userID)
	judges := database.GetUserJudges(userID)
	scrapingSources := database.GetScrapingSourcesOfUsers(userID)

	json.NewEncoder(w).Encode(user.ToUserSettings(judges, scrapingSources))
}

func saveUserSettings(w http.ResponseWriter, r *http.Request) {
	userID, userErr := authorization.GetUserIDFromRequest(r)
	if userErr != nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	var settings routeModels.UserSettings
	if err := json.NewDecoder(r.Body).Decode(&settings); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	if err := database.UpdateUserSettings(userID, settings); err != nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
	}

	json.NewEncoder(w).Encode(map[string]string{"message": "Settings saved successfully"})
}

func getUserRole(w http.ResponseWriter, r *http.Request) {
	userID, userErr := authorization.GetUserIDFromRequest(r)
	if userErr != nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	user := database.GetUserFromId(userID)

	json.NewEncoder(w).Encode(user.Role)
}

func exportProxies(w http.ResponseWriter, r *http.Request) {
	userID, userErr := authorization.GetUserIDFromRequest(r)
	if userErr != nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	var settings routeModels.ExportSettings
	if err := json.NewDecoder(r.Body).Decode(&settings); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	proxies, err := database.GetProxiesForExport(userID, settings)

	if err != nil {
		http.Error(w, fmt.Sprintf("Database error: %v", err), http.StatusInternalServerError)
		return
	}

	formattedProxies := helper.FormatProxies(proxies, settings.OutputFormat)

	w.Header().Set("Content-Type", "text/plain")
	w.Header().Set("Content-Disposition", "attachment; filename=proxies.txt")
	json.NewEncoder(w).Encode(formattedProxies)
}

func saveScrapingSources(w http.ResponseWriter, r *http.Request) {
	userID, err := authorization.GetUserIDFromRequest(r)
	if err != nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	var sources []string
	if err = json.NewDecoder(r.Body).Decode(&sources); err != nil {
		http.Error(w, "request body must be a JSON array of strings", http.StatusBadRequest)
		return
	}

	sites, err := database.SaveScrapingSourcesOfUsers(int(userID), sources)
	if err != nil {
		http.Error(w, "could not save sources", http.StatusInternalServerError)
		return
	}

	redis_queue.PublicScrapeSiteQueue.AddToQueue(sites)

	w.WriteHeader(http.StatusNoContent) // 204
}
