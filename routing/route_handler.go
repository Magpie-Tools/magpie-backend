package routing

import (
	"encoding/json"
	"errors"
	"github.com/charmbracelet/log"
	"gorm.io/gorm"
	"io"
	"magpie/authorization"
	"magpie/checker"
	"magpie/database"
	"magpie/helper"
	"magpie/models"
	"magpie/settings"
	"net/http"
	"strings"
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

	proxyList := helper.ParseTextToProxies(mergedContent)

	proxyList, err = database.InsertAndGetProxies(proxyList)
	if err != nil {
		log.Error("Could not add proxies to database", "error", err.Error())
		http.Error(writer, "Could not add proxies to database", http.StatusInternalServerError)
		return
	}
	checker.PublicProxyQueue.AddToQueue(proxyList)

	writer.WriteHeader(http.StatusOK)
	writer.Write([]byte(`{"message": "Added Proxies to Queue"}`))
}

func RegisterUser(w http.ResponseWriter, r *http.Request) {
	var user models.User
	if err := json.NewDecoder(r.Body).Decode(&user); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	// Hash the password
	hashedPassword, err := helper.HashPassword(user.Password)
	if err != nil {
		http.Error(w, "Failed to hash password", http.StatusInternalServerError)
		return
	}
	user.Password = hashedPassword

	// Check if there is any user in the database
	var existingUser models.User
	if err := database.DB.Select("id").Take(&existingUser).Error; err != nil {
		// If no user exists, assign admin role
		if errors.Is(err, gorm.ErrRecordNotFound) {
			user.Role = "admin"
		} else {
			http.Error(w, "Failed to query database", http.StatusInternalServerError)
			return
		}
	}

	// Save user to the database
	if err := database.DB.Create(&user).Error; err != nil {
		if strings.Contains(err.Error(), "unique constraint") {
			http.Error(w, "Email already in use", http.StatusConflict)
			return
		}
		http.Error(w, "Failed to create user", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{"message": "User created", "role": user.Role})
}

func LoginUser(w http.ResponseWriter, r *http.Request) {
	var credentials struct {
		Email    string `json:"email"`
		Password string `json:"password"`
	}

	if err := json.NewDecoder(r.Body).Decode(&credentials); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	var user models.User
	if err := database.DB.Where("email = ?", credentials.Email).First(&user).Error; err != nil {
		http.Error(w, "User not found", http.StatusUnauthorized)
		return
	}

	// Compare passwords
	if !helper.CheckPasswordHash(credentials.Password, user.Password) {
		http.Error(w, "Invalid password", http.StatusUnauthorized)
		return
	}

	// Generate token
	token, err := authorization.GenerateJWT(user.Email, user.Role)
	if err != nil {
		http.Error(w, "Failed to generate token", http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(map[string]string{"token": token})
}

func SaveSettings(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var newConfig settings.Config
	if err := json.NewDecoder(r.Body).Decode(&newConfig); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		log.Error("Error decoding request body:", err)
		return
	}

	settings.SetConfig(newConfig)
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Configuration updated successfully"))
}
