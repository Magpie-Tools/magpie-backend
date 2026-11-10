package config

import (
	"os"
	"testing"
)

func TestDefaultSettingsShipMonthlyAllowances(t *testing.T) {
	withTempSettingsWorkingDir(t)
	resetConfigAfterTest(t)
	t.Cleanup(ClearLegacyCadence)

	if err := ReadSettings(); err != nil {
		t.Fatalf("ReadSettings failed: %v", err)
	}
	cfg := GetConfig()
	if cfg.Checker.MonthlyChecksPerWorkspace != DefaultMonthlyChecksPerWorkspace {
		t.Fatalf("monthly checks = %d, want %d", cfg.Checker.MonthlyChecksPerWorkspace, DefaultMonthlyChecksPerWorkspace)
	}
	if cfg.Scraper.MonthlyScrapesPerWorkspace != DefaultMonthlyScrapesPerWorkspace {
		t.Fatalf("monthly scrapes = %d, want %d", cfg.Scraper.MonthlyScrapesPerWorkspace, DefaultMonthlyScrapesPerWorkspace)
	}
	if PendingLegacyCadence() != nil {
		t.Fatal("default settings must not request a legacy timer conversion")
	}
}

func TestReadSettingsCapturesLegacyTimersForConversion(t *testing.T) {
	withTempSettingsWorkingDir(t)
	resetConfigAfterTest(t)
	t.Cleanup(ClearLegacyCadence)

	legacy := `{"checker":{"checker_timer":{"hours":5}},"scraper":{"scraper_timer":{"hours":10}}}`
	if err := ensureSettingsStoragePermissions(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(settingsFilePath, []byte(legacy), settingsFileMode); err != nil {
		t.Fatal(err)
	}
	if err := ReadSettings(); err != nil {
		t.Fatalf("ReadSettings failed: %v", err)
	}
	pending := PendingLegacyCadence()
	if pending == nil || pending.CheckerTimer == nil || pending.ScraperTimer == nil {
		t.Fatalf("legacy timers were not captured: %+v", pending)
	}
	if pending.CheckerTimer.Hours != 5 || pending.ScraperTimer.Hours != 10 {
		t.Fatalf("captured timers = %+v / %+v", *pending.CheckerTimer, *pending.ScraperTimer)
	}
	// Until startup converts them, workspaces use the shipped defaults.
	cfg := GetConfig()
	if cfg.Checker.MonthlyChecksPerWorkspace != DefaultMonthlyChecksPerWorkspace || cfg.Scraper.MonthlyScrapesPerWorkspace != DefaultMonthlyScrapesPerWorkspace {
		t.Fatalf("unconverted allowances = %d / %d", cfg.Checker.MonthlyChecksPerWorkspace, cfg.Scraper.MonthlyScrapesPerWorkspace)
	}
}

func TestReadSettingsIgnoresLegacyTimersOnceAllowancesExist(t *testing.T) {
	withTempSettingsWorkingDir(t)
	resetConfigAfterTest(t)
	t.Cleanup(ClearLegacyCadence)

	current := `{"checker":{"checker_timer":{"hours":5},"monthly_checks_per_workspace":42},"scraper":{"scraper_timer":{"hours":10},"monthly_scrapes_per_workspace":7}}`
	if err := ensureSettingsStoragePermissions(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(settingsFilePath, []byte(current), settingsFileMode); err != nil {
		t.Fatal(err)
	}
	if err := ReadSettings(); err != nil {
		t.Fatalf("ReadSettings failed: %v", err)
	}
	if pending := PendingLegacyCadence(); pending != nil {
		t.Fatalf("explicit allowances must win over legacy timers: %+v", pending)
	}
	cfg := GetConfig()
	if cfg.Checker.MonthlyChecksPerWorkspace != 42 || cfg.Scraper.MonthlyScrapesPerWorkspace != 7 {
		t.Fatalf("allowances = %d / %d, want 42 / 7", cfg.Checker.MonthlyChecksPerWorkspace, cfg.Scraper.MonthlyScrapesPerWorkspace)
	}
}

func TestZeroInstanceAllowanceFallsBackToDefault(t *testing.T) {
	cfg := Config{}
	normalizeAllowances(&cfg)
	if cfg.Checker.MonthlyChecksPerWorkspace != DefaultMonthlyChecksPerWorkspace || cfg.Scraper.MonthlyScrapesPerWorkspace != DefaultMonthlyScrapesPerWorkspace {
		t.Fatalf("normalized allowances = %d / %d", cfg.Checker.MonthlyChecksPerWorkspace, cfg.Scraper.MonthlyScrapesPerWorkspace)
	}
}
