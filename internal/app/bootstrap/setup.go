package bootstrap

import (
	"context"
	"github.com/charmbracelet/log"
	"magpie/internal/config"
	"magpie/internal/database"
	"magpie/internal/jobs/checker"
	"magpie/internal/jobs/checker/judges"
	proxyqueue "magpie/internal/jobs/queue/proxy"
	sitequeue "magpie/internal/jobs/queue/sites"
	jobruntime "magpie/internal/jobs/runtime"
	"magpie/internal/jobs/scraper"
	"magpie/internal/support"
	"time"
)

func Setup() {
	config.ReadSettings()

	if _, err := database.SetupDB(); err != nil {
		log.Fatalf("failed to set up database: %v", err)
	}
	config.SetBetweenTime()

	judgeSetup()

	go func() {
		cfg := config.GetConfig()

		if config.GetCurrentIp() == "" && cfg.Checker.IpLookup == "" {
			return
		}

		for config.GetCurrentIp() == "" {
			html, err := checker.DefaultRequest(cfg.Checker.IpLookup)
			if err != nil {
				log.Error("Error checking IP address:", err)
				continue
			}

			currentIp := support.FindIP(html)
			config.SetCurrentIp(currentIp)
			log.Infof("Found IP! Current IP: %s", currentIp)

			time.Sleep(3 * time.Second)
		}

	}()

	proxies, err := database.GetAllProxies()
	if err != nil {
		log.Error("Error getting all proxies:", "error", err)
	} else {
		proxyqueue.PublicProxyQueue.AddToQueue(proxies)
		log.Infof("Added %d proxies to queue", len(proxies))
	}

	scrapeSites, err := database.GetAllScrapeSites()
	if err != nil {
		log.Error("Error getting all scrape sites:", "error", err)
	} else {
		sitequeue.PublicScrapeSiteQueue.AddToQueue(scrapeSites)
		log.Infof("Added %d scrape sites to queue", len(scrapeSites))
	}

	// Routines

	go judges.StartJudgeRoutine()
	go jobruntime.StartProxyStatisticsRoutine()
	go jobruntime.StartProxyGeoRefreshRoutine(context.Background())
	go checker.ThreadDispatcher()
	go scraper.ManagePagePool()
	go scraper.ThreadDispatcher()
}

func judgeSetup() {
	addJudgeRelationsToCache()
	AddDefaultJudgesToUsers()
}
