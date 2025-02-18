package setup

import (
	"github.com/charmbracelet/log"
	"magpie/checker"
	"magpie/database"
	"magpie/helper"
	"magpie/settings"
	"time"
)

func Setup() {
	settings.ReadSettings()

	database.SetupDB()
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

	proxies, err := database.GetAllProxies()
	if err != nil {
		log.Error("Error getting all proxies:", "error", err)
	} else {
		checker.PublicProxyQueue.AddToQueue(proxies)
		log.Infof("Added %d proxies to queue", len(proxies))
	}

	// Routines

	go checker.StartJudgeRoutine()
	go database.StartProxyStatisticsRoutine()
	go checker.Dispatcher()
}

func judgeSetup() {
	addJudgeRelationsToCache()
	AddDefaultJudgesToUsers()
}
