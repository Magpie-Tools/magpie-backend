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
)

func registerUser(w http.ResponseWriter, r *http.Request) {
	var credentials struct {
		Email    string `json:"email"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&credentials); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	user := models.User{
		Email:    credentials.Email,
		Password: credentials.Password,
	}

	// Validate email format
	if !authorization.IsValidEmail(user.Email) {
		http.Error(w, "Invalid email format", http.StatusBadRequest)
		return
	}

	// Check if password is provided
	if len(user.Password) < 8 {
		http.Error(w, "Password must be at least 8 characters long", http.StatusBadRequest)
		return
	}

	// Hash the password
	hashedPassword, err := helper.HashPassword(user.Password)
	if err != nil {
		http.Error(w, "Failed to hash password", http.StatusInternalServerError)
		return
	}
	user.Password = hashedPassword

	// Check if email already exists
	var existingUser models.User
	if err = database.DB.Where("email = ?", user.Email).First(&existingUser).Error; err == nil {
		http.Error(w, "Email already in use", http.StatusConflict)
		return
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		http.Error(w, "Failed to query database", http.StatusInternalServerError)
		return
	}

	// Check if there are no users in the database and assign admin role
	if err = database.DB.Select("id").Take(&existingUser).Error; errors.Is(err, gorm.ErrRecordNotFound) {
		user.Role = "admin"
	} else {
		user.Role = "user" // just to make sure
	}

	// Save user to the database
	if err = database.DB.Create(&user).Error; err != nil {
		http.Error(w, "Failed to create user", http.StatusInternalServerError)
		return
	}

	token, err := authorization.GenerateJWT(user.ID, user.Role)
	if err != nil {
		http.Error(w, "Failed to generate token", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{"token": token})
}

func loginUser(w http.ResponseWriter, r *http.Request) {
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
	token, err := authorization.GenerateJWT(user.ID, user.Role)
	if err != nil {
		http.Error(w, "Failed to generate token", http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(map[string]string{"token": token})
}

func addProxies(writer http.ResponseWriter, request *http.Request) {
	userID, userErr := authorization.GetUserIDFromRequest(request)
	if userErr != nil {
		http.Error(writer, "Unauthorized", http.StatusUnauthorized)
		return
	}

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
	proxyList = helper.AddUserIdToProxies(proxyList, userID)

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

func SaveSettings(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var newConfig settings.Config
	if err := json.NewDecoder(r.Body).Decode(&newConfig); err != nil {
		log.Error("Error decoding request body:", err)
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	settings.SetConfig(newConfig)
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Configuration updated successfully"))
}
