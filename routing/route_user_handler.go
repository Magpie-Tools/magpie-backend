package routing

import (
	"encoding/json"
	"errors"
	"github.com/charmbracelet/log"
	"gorm.io/gorm"
	"magpie/authorization"
	"magpie/checker/judges"
	"magpie/database"
	"magpie/helper"
	"magpie/models"
	"magpie/models/routeModels"
	redis_queue2 "magpie/scraper/redis_queue"
	"magpie/settings"
	"magpie/setup"
	"net/http"
)

func checkLogin(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func registerUser(w http.ResponseWriter, r *http.Request) {
	var credentials routeModels.Credentials
	if err := json.NewDecoder(r.Body).Decode(&credentials); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid request")
		return
	}

	user := models.User{
		Email:    credentials.Email,
		Password: credentials.Password,
	}

	// Validate email format
	if !authorization.IsValidEmail(user.Email) {
		writeError(w, http.StatusBadRequest, "Invalid email format")
		return
	}

	// Check if password is provided
	if len(user.Password) < 8 {
		writeError(w, http.StatusBadRequest, "Password must be at least 8 characters long")
		return
	}

	// Hash the password
	hashedPassword, err := helper.HashPassword(user.Password)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to hash password")
		return
	}
	user.Password = hashedPassword

	// Check if email already exists
	var existingUser models.User
	if err = database.DB.Where("email = ?", user.Email).First(&existingUser).Error; err == nil {
		writeError(w, http.StatusConflict, "Email already in use")
		return
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		writeError(w, http.StatusInternalServerError, "Failed to query database")
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
		writeError(w, http.StatusInternalServerError, "Failed to create user")
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
		writeError(w, http.StatusInternalServerError, "Failed to generate token")
		return
	}

	w.WriteHeader(http.StatusCreated)
	writeJSON(w, http.StatusCreated, map[string]string{"token": token})
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

	var jwrList []models.JudgeWithRegex
	for _, uj := range settings.SimpleUserJudges {
		judgeModel := database.GetJudgeFromString(uj.Url)
		if judgeModel == nil {
			log.Warnf("cannot load judge %d for user %d", uj.Url, userID)
			continue
		}
		judgeModel.SetUp()
		judgeModel.UpdateIp()
		jwrList = append(jwrList, models.JudgeWithRegex{
			Judge: judgeModel,
			Regex: uj.Regex,
		})
	}

	// atomically replace this user's judges in the global map
	judges.SetUserJudges(userID, jwrList)

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

func changePassword(w http.ResponseWriter, r *http.Request) {
	userID, userErr := authorization.GetUserIDFromRequest(r)
	if userErr != nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	user := database.GetUserFromId(userID)

	var changeUserPassword routeModels.ChangePassword
	if err := json.NewDecoder(r.Body).Decode(&changeUserPassword); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	if !helper.CheckPasswordHash(changeUserPassword.OldPassword, user.Password) {
		http.Error(w, "Invalid old password", http.StatusUnauthorized)
		return
	}

	hashed, err := helper.HashPassword(changeUserPassword.NewPassword)
	if err != nil {
		http.Error(w, "Failed to hash password", http.StatusInternalServerError)
		return
	}

	err = database.ChangePassword(userID, hashed)
	if err != nil {
		http.Error(w, "Failed to change password", http.StatusInternalServerError)
		log.Error(err)
		return
	}

	json.NewEncoder(w).Encode("Password changed successfully")
}
