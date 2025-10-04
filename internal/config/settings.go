package config

import (
	_ "embed"
	"encoding/json"
	"os"
	"sync/atomic"

	"github.com/charmbracelet/log"
)

type Config struct {
	Protocols struct {
		HTTP   bool `json:"http"`
		HTTPS  bool `json:"https"`
		Socks4 bool `json:"socks4"`
		Socks5 bool `json:"socks5"`
	} `json:"protocols"`

	Checker struct {
		DynamicThreads bool   `json:"dynamic_threads"`
		Threads        uint32 `json:"threads"`
		Retries        uint32 `json:"retries"`
		Timeout        uint32 `json:"timeout"`
		CheckerTimer   Timer  `json:"checker_timer"`

		JudgesThreads uint32  `json:"judges_threads"`
		JudgesTimeout uint32  `json:"judges_timeout"`
		Judges        []judge `json:"judges"`
		JudgeTimer    Timer   `json:"judge_timer"` // Only for production

		UseHttpsForSocks bool     `json:"use_https_for_socks"`
		IpLookup         string   `json:"ip_lookup"`
		StandardHeader   []string `json:"standard_header"`
		ProxyHeader      []string `json:"proxy_header"`
	} `json:"checker"`

	Scraper struct {
		DynamicThreads bool   `json:"dynamic_threads"`
		Threads        uint32 `json:"threads"`
		Retries        uint32 `json:"retries"`
		Timeout        uint32 `json:"timeout"`

		ScraperTimer Timer `json:"scraper_timer"`

		ScrapeSites []string `json:"scrape_sites"`
	} `json:"scraper"`

	Runtime struct {
		ProxyGeoRefreshTimer Timer `json:"proxy_geo_refresh_timer"`
	} `json:"runtime"`

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

	configValue atomic.Value
	currentIp   atomic.Value

	InProductionMode bool
)

func init() {
	// Initialize configValue with a default Config instance
	configValue.Store(Config{})
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
	log.Debug("Default Configuration updated and written to file successfully")
}

func GetConfig() Config {
	// Get the current Config atomically
	return configValue.Load().(Config)
}

func SetProductionMode(productionMode bool) {
	InProductionMode = productionMode
}

func GetCurrentIp() string {
	return currentIp.Load().(string)
}

func SetCurrentIp(ip string) {
	currentIp.Store(ip)
}
