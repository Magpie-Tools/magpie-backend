package database

import (
	"fmt"

	"magpie/internal/domain"
	"magpie/internal/support"

	"github.com/charmbracelet/log"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var (
	DB *gorm.DB
)

type Config struct {
	ExistingDB   *gorm.DB
	Dialector    gorm.Dialector
	Logger       logger.Interface
	AutoMigrate  bool
	Migrations   []any
	SeedDefaults bool
}

type Option func(*Config)

func SetupDB(opts ...Option) (*gorm.DB, error) {
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	switch {
	case cfg.ExistingDB != nil:
		DB = cfg.ExistingDB
	case cfg.Dialector != nil:
		gormCfg := &gorm.Config{}
		if cfg.Logger != nil {
			gormCfg.Logger = cfg.Logger
		}
		db, err := gorm.Open(cfg.Dialector, gormCfg)
		if err != nil {
			return nil, fmt.Errorf("database: open connection: %w", err)
		}
		DB = db
	default:
		return nil, fmt.Errorf("database: no dialector or existing connection provided")
	}

	if DB == nil {
		return nil, fmt.Errorf("database: connection was not configured")
	}

	if cfg.AutoMigrate && len(cfg.Migrations) > 0 {
		if err := DB.AutoMigrate(cfg.Migrations...); err != nil {
			return nil, fmt.Errorf("database: auto migrate: %w", err)
		}
		log.Info("Database migration completed.")
	}

	if cfg.SeedDefaults {
		if err := seedDefaults(DB); err != nil {
			return nil, fmt.Errorf("database: seed defaults: %w", err)
		}
	}

	return DB, nil
}

func defaultConfig() Config {
	dbHost := support.GetEnv("DB_HOST", "localhost")
	dbPort := support.GetEnv("DB_PORT", "5434")
	dbName := support.GetEnv("DB_NAME", "magpie")
	dbUser := support.GetEnv("DB_USERNAME", "admin")
	dbPassword := support.GetEnv("DB_PASSWORD", "admin")

	dsn := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		dbHost,
		dbPort,
		dbUser,
		dbPassword,
		dbName,
	)

	return Config{
		Dialector:    postgres.Open(dsn),
		Logger:       silentLogger(),
		AutoMigrate:  true,
		Migrations:   defaultMigrations(),
		SeedDefaults: true,
	}
}

func silentLogger() logger.Interface {
	return logger.New(
		log.Default(),
		logger.Config{LogLevel: logger.Silent},
	)
}

func defaultMigrations() []any {
	return []any{
		domain.User{},
		domain.Proxy{},
		domain.UserProxy{},
		domain.RotatingProxy{},
		domain.ProxyHistory{},
		domain.ProxySnapshot{},
		domain.ProxyStatistic{},
		domain.AnonymityLevel{},
		domain.Judge{},
		domain.UserJudge{},
		domain.ScrapeSite{},
		domain.UserScrapeSite{},
		domain.ProxyScrapeSite{},
		domain.Protocol{},
	}
}

func WithExistingDB(db *gorm.DB) Option {
	return func(cfg *Config) {
		cfg.ExistingDB = db
	}
}

func WithDialector(d gorm.Dialector) Option {
	return func(cfg *Config) {
		cfg.Dialector = d
	}
}

func WithLogger(l logger.Interface) Option {
	return func(cfg *Config) {
		cfg.Logger = l
	}
}

func WithAutoMigrate(enabled bool) Option {
	return func(cfg *Config) {
		cfg.AutoMigrate = enabled
	}
}

func WithMigrations(models ...any) Option {
	return func(cfg *Config) {
		if len(models) == 0 {
			cfg.Migrations = nil
			return
		}
		cfg.Migrations = append([]any(nil), models...)
	}
}

func WithSeedDefaults(enabled bool) Option {
	return func(cfg *Config) {
		cfg.SeedDefaults = enabled
	}
}

func seedDefaults(db *gorm.DB) error {
	if err := ensureAnonymityLevels(db); err != nil {
		return err
	}
	if err := ensureProtocols(db); err != nil {
		return err
	}
	return nil
}

func ensureAnonymityLevels(db *gorm.DB) error {
	if !db.Migrator().HasTable(&domain.AnonymityLevel{}) {
		return nil
	}

	var count int64
	if err := db.Model(&domain.AnonymityLevel{}).Count(&count).Error; err != nil {
		return err
	}
	if count > 0 {
		return nil
	}

	levels := []domain.AnonymityLevel{
		{Name: "elite"},
		{Name: "anonymous"},
		{Name: "transparent"},
	}

	return db.Create(&levels).Error
}

func ensureProtocols(db *gorm.DB) error {
	if !db.Migrator().HasTable(&domain.Protocol{}) {
		return nil
	}

	var count int64
	if err := db.Model(&domain.Protocol{}).Count(&count).Error; err != nil {
		return err
	}
	if count > 0 {
		return nil
	}

	protocols := []domain.Protocol{
		{Name: "http", ID: 1},
		{Name: "https", ID: 2},
		{Name: "socks4", ID: 3},
		{Name: "socks5", ID: 4},
	}

	return db.Create(&protocols).Error
}
