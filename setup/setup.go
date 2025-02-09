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
	settings.SetBetweenTime()

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

	proxies := database.GetAllProxies()
	checker.PublicProxyQueue.AddToQueue(proxies)
	proxyLen := len(proxies)
	statistics.IncreaseProxyCount(int64(proxyLen))
	log.Infof("Added %d proxies to queue", proxyLen)

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
		//TODO GET ALL DEFAULT JUDGES AND SETUP THEM AND CHECK IF THE USERS ALREADY HAVE JUDGES IF NOT ADD THE JUDGES TO THEIR JUDGES
		// AND ADD IT TO THE JUDGE HANDLER (BULK INSERT)
	}
}
