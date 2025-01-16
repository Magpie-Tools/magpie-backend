package routing

import (
	"github.com/charmbracelet/log"
	"io"
	"magpie/checker"
	"magpie/helper"
	"net/http"
)

func addProxies(writer http.ResponseWriter, request *http.Request) {
	textareaContent := request.FormValue("proxyTextarea") // "proxyTextarea" matches the key sent by the frontend
	file, fileHeader, err := request.FormFile("file")     // "file" is the key of the form field

	var fileContent []byte

	if err == nil {
		defer file.Close()

		log.Debugf("Uploaded file: %s (%d bytes)", fileHeader.Filename, fileHeader.Size)

		fileContent, err = io.ReadAll(file)
		if err != nil {
			http.Error(writer, "Failed to read file", http.StatusInternalServerError)
			return
		}

	} else if len(textareaContent) == 0 {
		http.Error(writer, "Failed to retrieve file", http.StatusBadRequest)
		return
	}

	// Merge the file content and the textarea content
	mergedContent := string(fileContent) + "\n" + textareaContent

	log.Infof("File content received: %d bytes", len(mergedContent))

	checker.PublicProxyQueue.AddToQueue(helper.ParseTextToProxies(mergedContent))

	if err != nil {
		http.Error(writer, "The program is stopping at the moment", http.StatusInternalServerError)
		return
	}

	writer.WriteHeader(http.StatusOK)
	writer.Write([]byte(`{"message": "Added Proxies to Queue"}`))
}
