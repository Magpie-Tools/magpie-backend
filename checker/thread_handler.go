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

		time.Sleep(10 * time.Second)
	}
}

func work() {
	for {
		select {
		case <-stopChannel:
			// Exit the work loop if a stop signal is received
			return
		default:
			proxy, scheduledTime := PublicProxyQueue.GetNextProxy()
			ip := proxy.GetIp()
			protocolsToCheck := settings.GetProtocolsToCheck()

			for protocol, protocolId := range protocolsToCheck {
				timeStart := time.Now()
				html, err := CheckProxyWithRetries(proxy, getNextJudge(protocol), protocol)
				responseTime := time.Since(timeStart).Milliseconds()
				statistic := models.ProxyStatistic{
					Alive:         false,
					ResponseTime:  int16(responseTime),
					Country:       database.GetCountryCode(ip),
					EstimatedType: database.DetermineProxyType(ip),
					ProxyID:       proxy.ID,
				}

				if err == nil {
					lvl := helper.GetProxyLevel(html)
					statistic.LevelID = &lvl
					statistic.Alive = true
					statistic.ProtocolID = &protocolId
				}

				database.AddProxyStatistic(statistic)
			}

			// Requeue the proxy for the next check
			PublicProxyQueue.RequeueProxy(proxy, scheduledTime)
		}
	}
}

func CheckProxyWithRetries(proxy models.Proxy, judge *models.Judge, protocol string) (string, error) {
	retries := settings.GetConfig().Checker.Retries

	var (
		html string
		err  error
	)

	for i := uint32(0); i < retries; i++ {
		html, err = ProxyCheckRequest(proxy, judge, protocol)
		if err != nil {
			return html, err
		}
		continue
	}

	return html, err
}
