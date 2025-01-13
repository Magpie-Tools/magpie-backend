package settings

import (
	"encoding/json"
	"github.com/charmbracelet/log"
	"os"
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

// Config Global settings variable
var Config config

var TimeBetweenChecks time.Duration

func ReadSettings() {
	data, err := os.ReadFile("data/settings.json")
	if err != nil {
		log.Error("Error reading settings file:", err)
		return
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

func calculateTime(proxyCount uint64) {
	TimeBetweenChecks = time.Duration(calculateMilliseconds() /
		proxyCount * (uint64(Config.Checker.Retries) + 1) / uint64(Config.Checker.Threads) *
		uint64(Config.Checker.Timeout))
}

func calculateMilliseconds() uint64 {
	// Calculate total duration in milliseconds
	return uint64(Config.Timer.Days)*24*60*60*1000 +
		uint64(Config.Timer.Hours)*60*60*1000 +
		uint64(Config.Timer.Minutes)*60*1000 +
		uint64(Config.Timer.Seconds)*1000
}

/* TODO
Embed an default_settings.json file that when data/settings.json is not found it gets automatically created
also clear the embed after that
*/
