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
		Threads          uint32   `json:"threads"`
		Retries          uint32   `json:"retries"`
		Timeout          uint32   `json:"timeout"`
		JudgesThreads    uint32   `json:"judges_threads"`
		JudgesTimeout    uint32   `json:"judges_timeout"`
		Judges           []judge  `json:"judges"`
		BlacklistSources []string `json:"blacklist_sources"`
	} `json:"checker"`
}

type judge struct {
	URL   string `json:"url"`
	Regex string `json:"regex"`
}

var (
	//go:embed default_settings.json
	defaultConfig []byte

	// Config Global settings variable
	Config            config
	timeBetweenChecks atomic.Value
)

func ReadSettings() {
	const settingsFilePath = "data/settings.json"

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

	// Unmarshal the JSON data into the config struct
	err = json.Unmarshal(data, &Config)
	if err != nil {
		log.Error("Error unmarshalling settings file:", err)
		return
	}

	log.Debug("Settings file loaded successfully")
}

func SetThreads(threads uint32) {
	Config.Checker.Threads = threads
}

// This calculates how many milliseconds a thread should wait before checking a new proxy
func calculateBetweenTime(proxyCount uint64) {
	timeBetweenChecks.Store(time.Duration(calculateMilliseconds() /
		proxyCount * (uint64(Config.Checker.Retries) + 1) / uint64(Config.Checker.Threads) *
		uint64(Config.Checker.Timeout)))
}

func calculateMilliseconds() uint64 {
	// Calculate total duration in milliseconds
	return uint64(Config.Timer.Days)*24*60*60*1000 +
		uint64(Config.Timer.Hours)*60*60*1000 +
		uint64(Config.Timer.Minutes)*60*1000 +
		uint64(Config.Timer.Seconds)*1000
}

func GetTimeBetweenChecks() time.Duration {
	return timeBetweenChecks.Load().(time.Duration)
}

/* TODO
Embed an default_settings.json file that when data/settings.json is not found it gets automatically created
also clear the embed after that
*/
