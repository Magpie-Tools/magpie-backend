package routing

import (
	"encoding/json"
	"fmt"
	"github.com/charmbracelet/log"
	"io"
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

	textareaContent := r.FormValue("sourcesTextarea") // Match the key sent by frontend
	clipboardContent := r.FormValue("clipboardSources")
	file, fileHeader, err := r.FormFile("file") // "file" is the key of the form field

	var fileContent []byte

	if err == nil {
		defer file.Close()

		log.Debugf("Uploaded file: %s (%d bytes)", fileHeader.Filename, fileHeader.Size)

		fileContent, err = io.ReadAll(file)
		if err != nil {
			http.Error(w, "Failed to read file", http.StatusInternalServerError)
			return
		}

	} else if len(textareaContent) == 0 && len(clipboardContent) == 0 {
		http.Error(w, "Failed to retrieve sources from any input method", http.StatusBadRequest)
		return
	}

	// Merge the file content and the textarea content
	mergedContent := string(fileContent) + "\n" + textareaContent + "\n" + clipboardContent

	log.Infof("Sources content received: %d bytes", len(mergedContent))

	// Parse the merged content into a slice of sources
	sources := helper.ParseTextToSources(mergedContent)

	sites, err := database.SaveScrapingSourcesOfUsers(userID, sources)
	if err != nil {
		log.Error("Could not save sources to database", "error", err.Error())
		http.Error(w, "Could not save sources to database", http.StatusInternalServerError)
		return
	}

	redis_queue.PublicScrapeSiteQueue.AddToQueue(sites)

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]int{"sourceCount": len(sites)})
}
