package database

import (
	"errors"
	"fmt"
	"github.com/charmbracelet/log"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
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

	err = database.AutoMigrate(models.User{}, models.Proxy{})
	if err != nil {
		log.Fatalf("Failed to migrate database: %v", err)
	}

	log.Info("Database migration completed.")
}

const (
	batchThreshold    = 30000 // Use batches when exceeding this number of records
	maxParamsPerBatch = 32767 // Conservative default (PostgreSQL's limit)
	minBatchSize      = 100   // Minimum batch size to maintain efficiency
)

func InsertProxies(proxies []models.Proxy) error {
	proxyLength := len(proxies)

	if proxyLength == 0 {
		return nil
	}

	// Determine batch size
	batchSize := len(proxies)
	if proxyLength > batchThreshold {
		numFields, err := getNumDatabaseFields(models.Proxy{}, DB)
		if err != nil {
			return fmt.Errorf("failed to parse model schema: %w", err)
		}
		if numFields == 0 {
			return errors.New("model has no database fields")
		}

		batchSize = maxParamsPerBatch / numFields
		if batchSize < minBatchSize {
			batchSize = minBatchSize
		}
		if batchSize > proxyLength {
			batchSize = proxyLength
		}
	}

	tx := DB.Begin()
	if tx.Error != nil {
		return tx.Error
	}

	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
			log.Errorf("Transaction rolled back due to panic: %v", r)
		}
	}()

	result := tx.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "hash"}},
		DoNothing: true,
	}).CreateInBatches(proxies, batchSize)

	if result.Error != nil {
		tx.Rollback()
		return result.Error
	}

	if err := tx.Commit().Error; err != nil {
		return err
	}

	return nil
}

func getNumDatabaseFields(model interface{}, db *gorm.DB) (int, error) {
	stmt := &gorm.Statement{DB: db}
	if err := stmt.Parse(model); err != nil {
		return 0, err
	}
	return len(stmt.Schema.DBNames), nil
}
