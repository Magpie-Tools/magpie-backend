package routing

import (
	"fmt"
	"github.com/charmbracelet/log"
	"io"
	"net/http"
	"strings"
)

func enableCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Set CORS headers
		w.Header().Set("Access-Control-Allow-Origin", "*") // Allow any origin
		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		// Handle preflight request
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		// Pass the request to the next handler
		next.ServeHTTP(w, r)
	})
}

func OpenRoutes(port int) {
	log.Info("Routes opened")

	router := http.NewServeMux()
	router.HandleFunc("POST /addProxies", func(writer http.ResponseWriter, request *http.Request) {
		file, fileHeader, err := request.FormFile("file") // "file" is the key of the form field
		if err != nil {
			http.Error(writer, "Failed to retrieve file", http.StatusBadRequest)
			return
		}
		defer file.Close()

		log.Debugf("Uploaded file: %s (%d bytes)", fileHeader.Filename, fileHeader.Size)

		fileContent, err := io.ReadAll(file)
		if err != nil {
			http.Error(writer, "Failed to read file", http.StatusInternalServerError)
			return
		}

		content := strings.Split(string(fileContent), "\n")

		log.Infof("File content received: %d bytes", len(content))

		writer.WriteHeader(http.StatusOK)
		writer.Write([]byte(`{"message": "Upload successful"}`))
	})

	server := http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: enableCORS(router),
	}

	log.Infof("Starting server on port :%d\n", port)
	err := server.ListenAndServe()
	if err != nil {
		return
	}
}
