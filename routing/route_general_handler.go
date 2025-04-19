package routing

import (
	"encoding/json"
	"errors"
	"github.com/charmbracelet/log"
	"gorm.io/gorm"
	"magpie/authorization"
	"magpie/database"
	"magpie/helper"
	"magpie/models"
	"magpie/models/routeModels"
	redis_queue2 "magpie/scraper/redis_queue"
	"magpie/settings"
	"magpie/setup"
	"net/http"
)

func registerUser(w http.ResponseWriter, r *http.Request) {
	var credentials routeModels.Credentials
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

	//Set default values
	cfg := settings.GetConfig()
	user.HTTPProtocol = cfg.Protocols.HTTP
	user.HTTPSProtocol = cfg.Protocols.HTTPS
	user.SOCKS4Protocol = cfg.Protocols.Socks4
	user.SOCKS5Protocol = cfg.Protocols.Socks5
	user.UseHttpsForSocks = cfg.Checker.UseHttpsForSocks

	// Save user to the database
	if err = database.DB.Create(&user).Error; err != nil {
		http.Error(w, "Failed to create user", http.StatusInternalServerError)
		return
	}

	go setup.AddDefaultJudgesToUsers()
	sites, err := database.SaveScrapingSourcesOfUsers(user.ID, cfg.Scraper.ScrapeSites) // default scrape sites
	if err != nil {
		log.Warn("Could not add default Scraping Sources to user", "err", err)
	} else {
		redis_queue2.PublicScrapeSiteQueue.AddToQueue(sites)
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
	var credentials routeModels.Credentials
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

	json.NewEncoder(w).Encode(map[string]string{"token": token, "role": user.Role})
}

func saveSettings(w http.ResponseWriter, r *http.Request) {
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
	json.NewEncoder(w).Encode(map[string]string{"message": "Configuration updated successfully"})
}

func getGlobalSettings(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(settings.GetConfig())
}
