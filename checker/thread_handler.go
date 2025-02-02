package checker

import (
	"magpie/database"
	"magpie/helper"
	"magpie/models"
	"magpie/settings"
	"sync/atomic"
	"time"
)

var (
	currentThreads atomic.Uint32
	stopChannel    = make(chan struct{}) // Signal to stop threads
)

func Dispatcher() {
	for {
		targetThreads := settings.GetConfig().Checker.Threads

		// Start threads if currentThreads is less than targetThreads
		for currentThreads.Load() < targetThreads {
			go work()
			currentThreads.Add(1)
		}

		// Stop threads if currentThreads is greater than targetThreads
		for currentThreads.Load() > targetThreads {
			stopChannel <- struct{}{}
			currentThreads.Add(^uint32(0))
		}

		time.Sleep(2 * time.Second)
	}
}

func work() {
	for {
		select {
		case <-stopChannel:
			// Exit the work loop if a stop signal is received
			return
		default:
			proxy := PublicProxyQueue.GetNextProxy()
			protocolsToCheck := settings.GetProtocolsToCheck()

			for _, protocol := range protocolsToCheck {
				timeStart := time.Now()
				html, err := ProxyCheckRequest(proxy, getNextJudge(protocol), protocol)
				responseTime := time.Since(timeStart).Milliseconds()
				statistic := models.ProxyStatistic{
					Alive:         false,
					ResponseTime:  int16(responseTime),
					Country:       database.GetCountryCode(proxy.IP),
					EstimatedType: database.DetermineProxyType(proxy.IP),
					ProxyID:       proxy.ID,
				}

				if err == nil {
					lvl := helper.GetProxyLevel(html)
					statistic.LevelID = &lvl
					statistic.Alive = true
				}

				database.AddProxyStatistic(statistic)
			}

			// Perform proxy checking or other tasks
			time.Sleep(settings.GetTimeBetweenChecks())
		}
	}
}
