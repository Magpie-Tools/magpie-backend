package setup

import (
	"github.com/charmbracelet/log"
	"magpie/checker"
	"magpie/checker/statistics"
	"magpie/database"
	"magpie/helper"
	"magpie/settings"
	"time"
)

func Setup() {
	settings.ReadSettings()

	database.SetupDB()
	statisticsSetup()
	proxyCount := statistics.GetProxyCount()
	if proxyCount > 0 {
		settings.SetBetweenTime(uint64(proxyCount))
	}

	judgeSetup()

	go func() {
		cfg := settings.GetConfig()

		if settings.GetCurrentIp() == "" && cfg.Checker.IpLookup == "" {
			return
		}

		for settings.GetCurrentIp() == "" {
			html, err := checker.DefaultRequest(cfg.Checker.IpLookup)
			if err != nil {
				log.Error("Error checking IP address:", err)
				continue
			}

			currentIp := helper.FindIP(html)
			settings.SetCurrentIp(currentIp)
			log.Infof("Found IP! Current IP: %s", currentIp)

			time.Sleep(3 * time.Second)
		}

	}()

	// Routines

	go checker.StartJudgeRoutine()
	go database.StartProxyStatisticsRoutine()
	go checker.Dispatcher()
}

func statisticsSetup() {
	statistics.SetProxyCount(database.GetAllProxyCount())
}

func judgeSetup() {
	cfg := settings.GetConfig()

	for _, judge := range cfg.Checker.Judges {
		err := checker.CreateAndAddJudgeToHandler(judge.URL, judge.Regex)
		if err != nil {
			log.Error("Error creating and adding judge to handler:", err)
		}
	}
}
