package server

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/charmbracelet/log"
	"magpie/internal/auth"
	"magpie/internal/database"
	sitequeue "magpie/internal/jobs/queue/sites"
	"magpie/internal/support"
)

func getScrapeSourcesCount(w http.ResponseWriter, r *http.Request) {
	userID, userErr := auth.GetUserIDFromRequest(r)
	if userErr != nil {
		writeError(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	json.NewEncoder(w).Encode(database.GetAllScrapeSiteCountOfUser(userID))
}

func getScrapeSourcePage(w http.ResponseWriter, r *http.Request) {
	userID, userErr := auth.GetUserIDFromRequest(r)
	if userErr != nil {
		writeError(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	page, err := strconv.Atoi(r.PathValue("page"))
	if err != nil {
		log.Error("error converting page to int", "error", err.Error())
		writeError(w, "Invalid page", http.StatusBadRequest)
		return
	}

	scrapeSiteInfoPages := database.GetScrapeSiteInfoPage(userID, page)

	json.NewEncoder(w).Encode(scrapeSiteInfoPages)
}

func deleteScrapingSources(w http.ResponseWriter, r *http.Request) {
	userID, userErr := auth.GetUserIDFromRequest(r)
	if userErr != nil {
		writeError(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	var scrapingSource []int

	if err := json.NewDecoder(r.Body).Decode(&scrapingSource); err != nil {
		writeError(w, "Invalid request", http.StatusBadRequest)
		return
	}

	deleted, orphaned, deleteErr := database.DeleteScrapeSiteRelation(userID, scrapingSource)
	if deleteErr != nil {
		log.Error("could not delete scrape sites", "error", deleteErr.Error())
		writeError(w, "Could not delete scraping sources", http.StatusInternalServerError)
		return
	}

	if len(orphaned) > 0 {
		if err := sitequeue.PublicScrapeSiteQueue.RemoveFromQueue(orphaned); err != nil {
			log.Error("failed to remove scrape sites from queue", "error", err, "count", len(orphaned))
		}
	}

	if deleted == 0 {
		json.NewEncoder(w).Encode("No scraping sources matched the delete criteria.")
		return
	}

	json.NewEncoder(w).Encode(fmt.Sprintf("Deleted %d scraping sources.", deleted))
}

func saveScrapingSources(w http.ResponseWriter, r *http.Request) {
	userID, err := auth.GetUserIDFromRequest(r)
	if err != nil {
		writeError(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	textareaContent := r.FormValue("scrapeSourceTextarea") // Match the key sent by frontend
	clipboardContent := r.FormValue("clipboardScrapeSources")
	file, fileHeader, err := r.FormFile("file") // "file" is the key of the form field

	var fileContent []byte

	if err == nil {
		defer file.Close()

		log.Debugf("Uploaded file: %s (%d bytes)", fileHeader.Filename, fileHeader.Size)

		fileContent, err = io.ReadAll(file)
		if err != nil {
			writeError(w, "Failed to read file", http.StatusInternalServerError)
			return
		}

	} else if len(textareaContent) == 0 && len(clipboardContent) == 0 {
		writeError(w, "Failed to retrieve sources from any input method", http.StatusBadRequest)
		return
	}

	// Merge the file content and the textarea content
	mergedContent := string(fileContent) + "\n" + textareaContent + "\n" + clipboardContent

	log.Infof("Sources content received: %d bytes", len(mergedContent))

	// Parse the merged content into a slice of sources
	sources := support.ParseTextToSources(mergedContent)

	sites, err := database.SaveScrapingSourcesOfUsers(userID, sources)
	if err != nil {
		log.Error("Could not save sources to database", "error", err.Error())
		writeError(w, "Could not save sources to database", http.StatusInternalServerError)
		return
	}

	sitequeue.PublicScrapeSiteQueue.AddToQueue(sites)

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]int{"sourceCount": len(sites)})
}
