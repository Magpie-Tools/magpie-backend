package settings

import (
	_ "embed"
	"encoding/json"
	"github.com/charmbracelet/log"
	"os"
	"sync/atomic"
	"time"
)

type config struct {
	Protocols struct {
		HTTP   bool `json:"http"`
		HTTPS  bool `json:"https"`
		Socks4 bool `json:"socks4"`
		Socks5 bool `json:"socks5"`
	} `json:"protocols"`

	Timer struct {
		Days    uint32 `json:"days"`
		Hours   uint32 `json:"hours"`
		Minutes uint32 `json:"minutes"`
		Seconds uint32 `json:"seconds"`
	} `json:"timer"`

	Checker struct {
		Threads        uint32   `json:"threads"`
		Retries        uint32   `json:"retries"`
		Timeout        uint32   `json:"timeout"`
		JudgesThreads  uint32   `json:"judges_threads"`
		JudgesTimeout  uint32   `json:"judges_timeout"`
		Judges         []judge  `json:"judges"`
		IpLookup       string   `json:"ip_lookup"`
		CurrentIp      string   `json:"current_ip"`
		StandardHeader []string `json:"standard_header"`
		ProxyHeader    []string `json:"proxy_header"`
	} `json:"checker"`

	BlacklistSources []string `json:"blacklist_sources"`
}

type judge struct {
	URL   string `json:"url"`
	Regex string `json:"regex"`
}

const settingsFilePath = "data/settings.json"

var (
	//go:embed default_settings.json
	defaultConfig []byte

	// Config is now managed via atomic.Value for thread-safe access.
	configValue       atomic.Value
	timeBetweenChecks atomic.Value
	protocolsToCheck  atomic.Value
)

func init() {
	// Initialize configValue with a default config instance
	configValue.Store(config{})
	protocolsToCheck.Store(make([]string, 4))
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

	var newConfig config
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

func SetConfig(newConfig config) {
	// Update the config atomically
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

	log.Debug("Configuration updated and written to file successfully")
}

func GetConfig() config {
	// Get the current config atomically
	return configValue.Load().(config)
}

func GetTimeBetweenChecks() time.Duration {
	return timeBetweenChecks.Load().(time.Duration)
}

func GetProtocolsToCheck() []string {
	return protocolsToCheck.Load().([]string)
}

func getProtocolsOfConfig(cfg config) []string {
	var protocols []string

	if cfg.Protocols.HTTP {
		protocols = append(protocols, "http")
	}
	if cfg.Protocols.HTTPS {
		protocols = append(protocols, "https")
	}
	if cfg.Protocols.Socks4 {
		protocols = append(protocols, "socks4")
	}
	if cfg.Protocols.Socks5 {
		protocols = append(protocols, "socks5")
	}

	return protocols
}

func calculateBetweenTime(proxyCount uint64) {
	cfg := GetConfig()
	timeBetweenChecks.Store(time.Duration(calculateMilliseconds(cfg) /
		proxyCount * (uint64(cfg.Checker.Retries) + 1) / uint64(cfg.Checker.Threads) *
		uint64(cfg.Checker.Timeout)))
}

func calculateMilliseconds(cfg config) uint64 {
	// Calculate total duration in milliseconds
	return uint64(cfg.Timer.Days)*24*60*60*1000 +
		uint64(cfg.Timer.Hours)*60*60*1000 +
		uint64(cfg.Timer.Minutes)*60*1000 +
		uint64(cfg.Timer.Seconds)*1000
}
