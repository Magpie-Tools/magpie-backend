package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// DevelopmentFixturesEnabled disables autonomous work so synthetic measurements
// remain available when developing the UI. Normal startup is unchanged.
func DevelopmentFixturesEnabled() (bool, error) {
	raw := strings.TrimSpace(os.Getenv("DEVELOPMENT_FIXTURES"))
	if raw == "" {
		return false, nil
	}
	enabled, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("DEVELOPMENT_FIXTURES must be a boolean")
	}
	if enabled && (InProductionMode || RuntimeEnvironmentIndicatesProduction()) {
		return false, fmt.Errorf("DEVELOPMENT_FIXTURES cannot be enabled in production")
	}
	return enabled, nil
}
