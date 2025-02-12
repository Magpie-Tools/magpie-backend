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

	useHttpsForSocks = atomic.Bool{}
)

func Dispatcher() {
	for {
		cfg := settings.GetConfig()
		useHttpsForSocks.Store(cfg.Checker.UseHttpsForSocks)
		targetThreads := cfg.Checker.Threads

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

			for _, user := range proxy.Users {
				for protocol, protocolId := range protocolsToCheck {
					var (
						nextJudge *models.Judge
						regex     string
					)
					if protocolId > 2 { // Socks protocol
						if useHttpsForSocks.Load() {
							nextJudge, regex = getNextJudge(user.ID, "https")
						} else {
							nextJudge, regex = getNextJudge(user.ID, "http")
						}
					} else {
						nextJudge, regex = getNextJudge(user.ID, protocol)
					}

					html, err, responseTime := CheckProxyWithRetries(proxy, nextJudge, protocol, regex)

					statistic := models.ProxyStatistic{
						Alive:         false,
						ResponseTime:  int16(responseTime),
						Country:       database.GetCountryCode(ip),
						EstimatedType: database.DetermineProxyType(ip),
						ProxyID:       proxy.ID,
						ProtocolID:    &protocolId,
						JudgeID:       nextJudge.ID,
					}

					if err == nil {
						lvl := helper.GetProxyLevel(html)
						statistic.LevelID = &lvl
						statistic.Alive = true
					}

					database.AddProxyStatistic(statistic)
				}
			}

			// Requeue the proxy for the next check
			PublicProxyQueue.RequeueProxy(proxy, scheduledTime)
		}
	}
}

func CheckProxyWithRetries(proxy models.Proxy, judge *models.Judge, protocol, regex string) (string, error, int64) {
	retries := settings.GetConfig().Checker.Retries

	var (
		html         string
		err          error
		responseTime int64
	)

	for i := uint32(0); i < retries; i++ {
		timeStart := time.Now()
		html, err = ProxyCheckRequest(proxy, judge, protocol)
		responseTime = time.Since(timeStart).Milliseconds()

		if err == nil && CheckForValidResponse(html, regex) {
			return html, err, responseTime
		}
	}

	return html, err, responseTime
}
