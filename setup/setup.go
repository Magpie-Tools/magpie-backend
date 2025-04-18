package setup

import (
	"github.com/charmbracelet/log"
	"magpie/checker"
	"magpie/checker/judges"
	"magpie/checker/redis_queue"
	"magpie/database"
	"magpie/helper"
	"magpie/scraper"
	redis_queue2 "magpie/scraper/redis_queue"
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
		redis_queue.PublicProxyQueue.AddToQueue(proxies)
		log.Infof("Added %d proxies to queue", len(proxies))
	}

	scrapeSites, err := database.GetAllScrapeSites()
	if err != nil {
		log.Error("Error getting all scrape sites:", "error", err)
	} else {
		redis_queue2.PublicScrapeSiteQueue.AddToQueue(scrapeSites)
		log.Infof("Added %d scrape sites to queue", len(scrapeSites))
	}

	// Routines

	go judges.StartJudgeRoutine()
	go database.StartProxyStatisticsRoutine()
	go checker.ThreadDispatcher()
	go scraper.ThreadDispatcher()
	go scraper.ManagePagePool()
}

func judgeSetup() {
	addJudgeRelationsToCache()
	AddDefaultJudgesToUsers()
}
