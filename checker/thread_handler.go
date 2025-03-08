package checker

import (
	"github.com/charmbracelet/log"
	"magpie/checker/judges"
	"magpie/checker/redis_queue"
	"magpie/database"
	"magpie/helper"
	"magpie/models"
	"magpie/settings"
	"math"
	"strconv"
	"sync/atomic"
	"time"
)

var (
	currentThreads atomic.Uint32
	stopChannel    = make(chan struct{}) // Signal to stop threads
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
	totalProxies, err := redis_queue.PublicProxyQueue.GetProxyCount()
	if err != nil {
		log.Error("Failed to get proxy count", "error", err)
		return 1 // Fallback to minimal threads
	}

	activeInstances, err := redis_queue.PublicProxyQueue.GetActiveInstances()
	if err != nil {
		log.Error("Failed to get active instances", "error", err)
		activeInstances = 1
	}
	if activeInstances == 0 {
		activeInstances = 1 // Prevent division by zero
	}

	perInstanceProxies := (totalProxies + int64(activeInstances) - 1) / int64(activeInstances)

	checkingPeriodMs := settings.CalculateMillisecondsOfCheckingPeriod(cfg.Checker.CheckerTimer)
	protocolsToCheck := settings.GetProtocolsToCheck()
	protocolsCount := len(protocolsToCheck)
	retries := cfg.Checker.Retries
	timeoutMs := cfg.Checker.Timeout

	numerator := uint64(perInstanceProxies) * uint64(protocolsCount) * uint64(retries+1) * uint64(timeoutMs)
	if checkingPeriodMs == 0 {
		checkingPeriodMs = 1
	}
	requiredThreads := (numerator + checkingPeriodMs - 1) / checkingPeriodMs

	if requiredThreads == 0 && perInstanceProxies > 0 {
		requiredThreads = 1
	}

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
			proxy, scheduledTime, err := redis_queue.PublicProxyQueue.GetNextProxy()
			if err != nil {
				time.Sleep(3 * time.Second)
				continue
			}
			ip := proxy.GetIp()

			allJudges := make(map[string]struct {
				judge      *models.Judge
				regex      string
				protocol   string
				protocolId int
			})

			var maxTimeout uint16 = 0
			var maxRetries uint8 = 0

			for _, user := range proxy.Users {

				if user.Timeout > maxTimeout {
					maxTimeout = user.Timeout
				}
				if user.Retries > maxRetries {
					maxRetries = user.Retries
				}

				for protocol, protocolId := range user.GetProtocolMap() {
					var (
						nextJudge *models.Judge
						regex     string
					)
					if protocolId > 2 { // Socks protocol
						if user.UseHttpsForSocks {
							nextJudge, regex = judges.GetNextJudge(user.ID, "https")
						} else {
							nextJudge, regex = judges.GetNextJudge(user.ID, "http")
						}
					} else {
						nextJudge, regex = judges.GetNextJudge(user.ID, protocol)
					}

					allJudges[strconv.Itoa(int(nextJudge.ID))+regex] = struct {
						judge      *models.Judge
						regex      string
						protocol   string
						protocolId int
					}{judge: nextJudge, regex: regex, protocol: protocol, protocolId: protocolId}
				}
			}

			for _, item := range allJudges {
				html, err, responseTime, attempt := CheckProxyWithRetries(proxy, item.judge, item.protocol, item.regex, maxTimeout, maxRetries)

				statistic := models.ProxyStatistic{
					Alive:         false,
					ResponseTime:  uint16(responseTime),
					Attempt:       attempt,
					Country:       database.GetCountryCode(ip),
					EstimatedType: database.DetermineProxyType(ip),
					ProxyID:       proxy.ID,
					ProtocolID:    item.protocolId,
					JudgeID:       item.judge.ID,
				}

				if err == nil {
					lvl := helper.GetProxyLevel(html)
					statistic.LevelID = &lvl
					statistic.Alive = true
				}

				database.AddProxyStatistic(statistic)
			}

			// Requeue the proxy for the next check
			redis_queue.PublicProxyQueue.RequeueProxy(proxy, scheduledTime)
		}
	}
}

func CheckProxyWithRetries(proxy models.Proxy, judge *models.Judge, protocol, regex string, timeout uint16, retries uint8) (string, error, int64, uint8) {
	var (
		html         string
		err          error
		responseTime int64
	)

	for i := uint8(0); i < retries; i++ {
		timeStart := time.Now()
		html, err = ProxyCheckRequest(proxy, judge, protocol, timeout)
		responseTime = time.Since(timeStart).Milliseconds()

		if err == nil && CheckForValidResponse(html, regex) {
			return html, err, responseTime, i
		}
	}

	return html, err, responseTime, retries
}
