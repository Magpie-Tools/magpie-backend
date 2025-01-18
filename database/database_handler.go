package database

import (
	"fmt"
	"github.com/charmbracelet/log"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"magpie/helper"
	"magpie/models"
)

var (
	DB *gorm.DB
)

func SetupDB() {
	dbHost := helper.GetEnv("DB_HOST", "localhost")
	dbPort := helper.GetEnv("DB_PORT", "5432")
	dbName := helper.GetEnv("DB_NAME", "magpie")
	dbUser := helper.GetEnv("DB_USERNAME", "admin")
	dbPassword := helper.GetEnv("DB_PASSWORD", "admin")

	dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		dbHost, dbPort, dbUser, dbPassword, dbName,
	)
	database, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	DB = database

	err = database.AutoMigrate(models.User{})
	if err != nil {
		log.Fatalf("Failed to migrate database: %v", err)
	}

	log.Info("Database migration completed.")
}
