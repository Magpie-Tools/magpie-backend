package database

import (
	"fmt"
	"github.com/charmbracelet/log"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"magpie/internal/domain"
	"magpie/internal/support"
)

var (
	DB *gorm.DB
)

func SetupDB() {
	dbHost := support.GetEnv("DB_HOST", "localhost")
	dbPort := support.GetEnv("DB_PORT", "5434")
	dbName := support.GetEnv("DB_NAME", "magpie")
	dbUser := support.GetEnv("DB_USERNAME", "admin")
	dbPassword := support.GetEnv("DB_PASSWORD", "admin")

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

	err = database.AutoMigrate(domain.User{}, domain.Proxy{}, domain.UserProxy{},
		domain.ProxyStatistic{}, domain.AnonymityLevel{}, domain.Judge{}, domain.UserJudge{},
		domain.ScrapeSite{}, domain.UserScrapeSite{}, domain.ProxyScrapeSite{})

	if err != nil {
		log.Fatalf("Failed to migrate database: %v", err)
	}

	log.Info("Database migration completed.")

	var count int64
	if DB.Model(&domain.AnonymityLevel{}).Count(&count); count == 0 {
		levels := []domain.AnonymityLevel{
			{Name: "elite"},
			{Name: "anonymous"},
			{Name: "transparent"},
		}
		DB.Create(&levels)
	}
	if DB.Model(&domain.Protocol{}).Count(&count); count == 0 {
		levels := []domain.Protocol{
			{Name: "http", ID: 1},
			{Name: "https", ID: 2},
			{Name: "socks4", ID: 3},
			{Name: "socks5", ID: 4},
		}
		DB.Create(&levels)
	}

}
