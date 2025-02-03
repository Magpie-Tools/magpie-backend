package settings

import (
	_ "embed"
	"encoding/json"
	"github.com/charmbracelet/log"
	"os"
	"sync/atomic"
	"time"
)

type Config struct {
	Protocols struct {
		HTTP   bool `json:"http"`
		HTTPS  bool `json:"https"`
		Socks4 bool `json:"socks4"`
		Socks5 bool `json:"socks5"`
	} `json:"protocols"`

	Timer Timer `json:"timer"`

	Checker struct {
		Threads uint32 `json:"threads"`
		Retries uint32 `json:"retries"`
		Timeout uint32 `json:"timeout"`

		JudgesThreads uint32  `json:"judges_threads"`
		JudgesTimeout uint32  `json:"judges_timeout"`
		Judges        []judge `json:"judges"`
		JudgeTimer    Timer   `json:"judge_timer"` // Only for production

		IpLookup       string   `json:"ip_lookup"`
		StandardHeader []string `json:"standard_header"`
		ProxyHeader    []string `json:"proxy_header"`
	} `json:"checker"`

	BlacklistSources []string `json:"blacklist_sources"`
}

type judge struct {
	URL   string `json:"url"`
	Regex string `json:"regex"`
}

type Timer struct {
	Days    uint32 `json:"days"`
	Hours   uint32 `json:"hours"`
	Minutes uint32 `json:"minutes"`
	Seconds uint32 `json:"seconds"`
}

const settingsFilePath = "data/settings.json"

var (
	//go:embed default_settings.json
	defaultConfig []byte

	configValue       atomic.Value
	timeBetweenChecks atomic.Value
	protocolsToCheck  atomic.Value
	currentIp         atomic.Value

	InProductionMode bool
)

func init() {
	// Initialize configValue with a default Config instance
	configValue.Store(Config{})
	protocolsToCheck.Store(make(map[string]int, 4))
	currentIp.Store("")
}

func ReadSettings() {

	data, err := os.ReadFile(settingsFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			log.Warn("Settings file not found, creating with default configuration")

			err = os.MkdirAll("data", os.ModePerm)
			if err != nil {
				log.Error("Error creating directory for settings file:", err)
				return
			}

			err = os.WriteFile(settingsFilePath, defaultConfig, os.ModePerm)
			if err != nil {
				log.Error("Error writing default settings file:", err)
				return
			}

			data = defaultConfig
		} else {
			log.Error("Error reading settings file:", err)
			return
		}
	}

	var newConfig Config
	err = json.Unmarshal(data, &newConfig)
	if err != nil {
		log.Error("Error unmarshalling settings file:", err)
		return
	}

	// Store the new configuration atomically
	configValue.Store(newConfig)
	protocolsToCheck.Store(getProtocolsOfConfig(newConfig))

	log.Debug("Settings file loaded successfully")
}

func SetConfig(newConfig Config) {
	// Update the Config atomically
	configValue.Store(newConfig)

	// Write the new configuration to the file
	data, err := json.MarshalIndent(newConfig, "", "  ")
	if err != nil {
		log.Error("Error marshalling new configuration:", err)
		return
	}

	err = os.WriteFile(settingsFilePath, data, os.ModePerm)
	if err != nil {
		log.Error("Error writing new configuration to file:", err)
		return
	}
	SetBetweenTime()
	log.Debug("Configuration updated and written to file successfully")
}

func GetConfig() Config {
	// Get the current Config atomically
	return configValue.Load().(Config)
}

func SetProductionMode(productionMode bool) {
	InProductionMode = productionMode
}

func GetTimeBetweenChecks() time.Duration {
	return timeBetweenChecks.Load().(time.Duration)
}

func GetCurrentIp() string {
	return currentIp.Load().(string)
}

func SetCurrentIp(ip string) {
	currentIp.Store(ip)
}

func GetProtocolsToCheck() map[string]int {
	return protocolsToCheck.Load().(map[string]int)
}

func getProtocolsOfConfig(cfg Config) map[string]int {
	protocols := make(map[string]int)

	if cfg.Protocols.HTTP {
		protocols["http"] = 1
	}
	if cfg.Protocols.HTTPS {
		protocols["https"] = 2
	}
	if cfg.Protocols.Socks4 {
		protocols["socks4"] = 3
	}
	if cfg.Protocols.Socks5 {
		protocols["socks5"] = 4
	}

	return protocols
}

func SetBetweenTime() {
	timeBetweenChecks.Store(CalculateBetweenTime())
}

// CalculateBetweenTime Also works with e.g a judgeCount
func CalculateBetweenTime() time.Duration {
	cfg := GetConfig()
	totalMs := CalculateMillisecondsOfCheckingPeriod(cfg.Timer)

	// Return the full checking period (e.g., 1 hour)
	intervalMs := totalMs

	// Enforce minimum interval (e.g., 1 second)
	minInterval := uint64(1000)
	if intervalMs < minInterval {
		intervalMs = minInterval
	}

	return time.Duration(intervalMs) * time.Millisecond
}

func CalculateMillisecondsOfCheckingPeriod(timer Timer) uint64 {
	// Calculate total duration in milliseconds
	return uint64(timer.Days)*24*60*60*1000 +
		uint64(timer.Hours)*60*60*1000 +
		uint64(timer.Minutes)*60*1000 +
		uint64(timer.Seconds)*1000
}
