package database

import (
	"fmt"
	"github.com/charmbracelet/log"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"magpie/helper"
	"magpie/models"
)

var (
	DB *gorm.DB
)

func SetupDB() {
	dbHost := helper.GetEnv("DB_HOST", "localhost")
	dbPort := helper.GetEnv("DB_PORT", "5434")
	dbName := helper.GetEnv("DB_NAME", "magpie")
	dbUser := helper.GetEnv("DB_USERNAME", "admin")
	dbPassword := helper.GetEnv("DB_PASSWORD", "admin")

	dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		dbHost, dbPort, dbUser, dbPassword, dbName,
	)

	silent := logger.New(
		log.Default(),
		logger.Config{
			LogLevel: logger.Silent,
		},
	)

	database, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: silent,
	})
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	DB = database

	err = database.AutoMigrate(models.User{}, models.Proxy{}, models.UserProxy{},
		models.ProxyStatistic{}, models.AnonymityLevel{})

	if err != nil {
		log.Fatalf("Failed to migrate database: %v", err)
	}

	log.Info("Database migration completed.")

	var count int64
	if DB.Model(&models.AnonymityLevel{}).Count(&count); count == 0 {
		levels := []models.AnonymityLevel{
			{Name: "elite"},
			{Name: "anonymous"},
			{Name: "transparent"},
		}
		DB.Create(&levels)
	}
	if DB.Model(&models.Protocol{}).Count(&count); count == 0 {
		levels := []models.Protocol{
			{Name: "http"},
			{Name: "https"},
			{Name: "socks4"},
			{Name: "socks5"},
		}
		DB.Create(&levels)
	}

}
