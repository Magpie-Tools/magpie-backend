package server

import (
	"encoding/json"
	"fmt"
	"github.com/charmbracelet/log"
	"io"
	"magpie/internal/api/dto"
	"magpie/internal/auth"
	"magpie/internal/database"
	proxyqueue "magpie/internal/jobs/checker/queue/proxy"
	"magpie/internal/support"
	"net/http"
	"strconv"
)

func addProxies(w http.ResponseWriter, r *http.Request) {
	userID, userErr := auth.GetUserIDFromRequest(r)
	if userErr != nil {
		writeError(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	textareaContent := r.FormValue("proxyTextarea") // "proxyTextarea" matches the key sent by the frontend
	clipboardContent := r.FormValue("clipboardProxies")
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
		writeError(w, "Failed to retrieve file", http.StatusBadRequest)
		return
	}

	// Merge the file content and the textarea content
	mergedContent := string(fileContent) + "\n" + textareaContent + "\n" + clipboardContent

	log.Infof("File content received: %d bytes", len(mergedContent))

	proxyList := support.ParseTextToProxies(mergedContent)

	database.EnrichProxiesWithCountryAndType(&proxyList)

	proxyList, err = database.InsertAndGetProxiesWithUser(proxyList, userID)
	if err != nil {
		log.Error("Could not add proxies to database", "error", err.Error())
		writeError(w, "Could not add proxies to database", http.StatusInternalServerError)
		return
	}
	proxyqueue.PublicProxyQueue.AddToQueue(proxyList)

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]int{"proxyCount": len(proxyList)})
}

func getProxyPage(w http.ResponseWriter, r *http.Request) {
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

	proxyList := database.GetProxyInfoPage(userID, page)

	json.NewEncoder(w).Encode(proxyList)
}

func getProxyCount(w http.ResponseWriter, r *http.Request) {
	userID, userErr := auth.GetUserIDFromRequest(r)
	if userErr != nil {
		writeError(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	json.NewEncoder(w).Encode(database.GetAllProxyCountOfUser(userID))
}

func deleteProxies(w http.ResponseWriter, r *http.Request) {
	userID, userErr := auth.GetUserIDFromRequest(r)
	if userErr != nil {
		writeError(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	var proxies []int

	if err := json.NewDecoder(r.Body).Decode(&proxies); err != nil {
		writeError(w, "Invalid request", http.StatusBadRequest)
		return
	}

	database.DeleteProxyRelation(userID, proxies)

	json.NewEncoder(w).Encode("Proxies deleted successfully")
}

func exportProxies(w http.ResponseWriter, r *http.Request) {
	userID, userErr := auth.GetUserIDFromRequest(r)
	if userErr != nil {
		writeError(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	var settings dto.ExportSettings
	if err := json.NewDecoder(r.Body).Decode(&settings); err != nil {
		writeError(w, "Invalid request", http.StatusBadRequest)
		return
	}

	proxies, err := database.GetProxiesForExport(userID, settings)

	if err != nil {
		writeError(w, fmt.Sprintf("Database error: %v", err), http.StatusInternalServerError)
		return
	}

	formattedProxies := support.FormatProxies(proxies, settings.OutputFormat)

	w.Header().Set("Content-Type", "text/plain")
	w.Header().Set("Content-Disposition", "attachment; filename=proxies.txt")
	json.NewEncoder(w).Encode(formattedProxies)
}
