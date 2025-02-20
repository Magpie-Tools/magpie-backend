package checker

import (
	"magpie/checker/statistics"
	"magpie/database"
	"magpie/helper"
	"magpie/models"
	"magpie/settings"
	"math"
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

		var targetThreads uint32
		if cfg.Checker.DynamicThreads {
			targetThreads = getAutoThreads(cfg)
		} else {
			targetThreads = cfg.Checker.Threads
		}

		useHttpsForSocks.Store(cfg.Checker.UseHttpsForSocks)

		// Start threads if currentThreads is less than targetThreads
		for currentThreads.Load() < targetThreads {
			go work()
			currentThreads.Add(1)
		}

		// Stop threads if currentThreads is greater than targetThreads
		for currentThreads.Load() > targetThreads {
			stopChannel <- struct{}{}
			currentThreads.Add(^uint32(0)) // Decrement by 1
		}

		time.Sleep(15 * time.Second)
	}
}

func getAutoThreads(cfg settings.Config) uint32 {
	proxyCount := statistics.GetProxyCount()
	checkingPeriodMs := settings.CalculateMillisecondsOfCheckingPeriod(cfg.Timer)
	protocolsToCheck := settings.GetProtocolsToCheck()
	protocolsCount := len(protocolsToCheck)
	retries := cfg.Checker.Retries
	timeoutMs := cfg.Checker.Timeout

	numerator := uint64(proxyCount) * uint64(protocolsCount) * uint64(retries+1) * uint64(timeoutMs)
	if checkingPeriodMs == 0 {
		checkingPeriodMs = 1 // Avoid division by zero
	}
	requiredThreads := (numerator + checkingPeriodMs - 1) / checkingPeriodMs

	// Ensure at least 1 thread if there are proxies
	if requiredThreads == 0 && proxyCount > 0 {
		requiredThreads = 1
	}

	// Cap at MaxUint32 to prevent overflow even if its unrealistic that it'll be more than this
	if requiredThreads > math.MaxUint32 {
		requiredThreads = math.MaxUint32
	}

	return uint32(requiredThreads)
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
