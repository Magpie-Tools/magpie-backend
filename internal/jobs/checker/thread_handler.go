package checker

import (
	"context"
	"errors"
	"github.com/charmbracelet/log"
	"magpie/internal/config"
	"magpie/internal/domain"
	"magpie/internal/jobs/checker/judges"
	proxyqueue "magpie/internal/jobs/queue/proxy"
	jobruntime "magpie/internal/jobs/runtime"
	"magpie/internal/support"
	"math"
	"strconv"
	"sync/atomic"
	"time"
)

var (
	currentThreads atomic.Uint32
	stopChannel    = make(chan struct{}) // Signal to stop threads
)

const maxResponseBodyLength = 4096

func ThreadDispatcher() {
	for {
		cfg := config.GetConfig()

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

		log.Debug("Checker threads", "active", currentThreads.Load())
		time.Sleep(15 * time.Second)
	}
}

func getAutoThreads(cfg config.Config) uint32 {
	totalProxies, err := proxyqueue.PublicProxyQueue.GetProxyCount()
	if err != nil {
		log.Error("Failed to get proxy count", "error", err)
		return 1 // Fallback to minimal threads
	}

	activeInstances, err := proxyqueue.PublicProxyQueue.GetActiveInstances()
	if err != nil {
		log.Error("Failed to get active instances", "error", err)
		activeInstances = 1
	}
	if activeInstances == 0 {
		activeInstances = 1 // Prevent division by zero
	}

	perInstanceProxies := (totalProxies + int64(activeInstances) - 1) / int64(activeInstances)

	checkingPeriodMs := config.CalculateMillisecondsOfCheckingPeriod(cfg.Checker.CheckerTimer)
	protocolsCount := 4
	retries := cfg.Checker.Retries
	timeoutMs := cfg.Checker.Timeout

	numerator := uint64(perInstanceProxies) * uint64(protocolsCount) * uint64(retries+1) * uint64(timeoutMs)
	if checkingPeriodMs == 0 {
		log.Warn("Checking Period is set to 0 Milliseconds. Setting it to 1 Day automatically")
		checkingPeriodMs = 86400000
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
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		defer close(done)
		select {
		case <-stopChannel:
			cancel()
		case <-ctx.Done():
		}
	}()

	defer func() {
		cancel()
		<-done
	}()

	for {
		proxy, scheduledTime, err := proxyqueue.PublicProxyQueue.GetNextProxyContext(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			time.Sleep(3 * time.Second)
			continue
		}

		judgeRequests := make(map[string]struct {
			judge        *domain.Judge
			protocol     string
			regexToProto map[string]int // Maps regex to protocolId
		})

		var maxTimeout uint16
		var maxRetries uint8

		// Prefetch data
		for _, user := range proxy.Users {
			if user.Timeout > maxTimeout {
				maxTimeout = user.Timeout
			}
			if user.Retries > maxRetries {
				maxRetries = user.Retries
			}

			for protocol, protocolId := range user.GetProtocolMap() {
				var (
					nextJudge       *domain.Judge
					regex           string
					requestProtocol string
				)

				// Determine which protocol to use for request
				if protocolId > 2 { // Socks protocol
					if user.UseHttpsForSocks {
						requestProtocol = "https"
					} else {
						requestProtocol = "http"
					}
				} else {
					requestProtocol = protocol
				}

				nextJudge, regex = judges.GetNextJudge(user.ID, requestProtocol)

				// Create a unique key for each judge+request_protocol combination
				judgeKey := strconv.Itoa(int(nextJudge.ID)) + "_" + requestProtocol

				if existingRequest, found := judgeRequests[judgeKey]; found {
					// If we already plan to check this judge with this protocol, just add the regex
					existingRequest.regexToProto[regex] = protocolId
					judgeRequests[judgeKey] = existingRequest
				} else {
					// First time seeing this judge+protocol, create new entry
					judgeRequests[judgeKey] = struct {
						judge        *domain.Judge
						protocol     string
						regexToProto map[string]int
					}{
						judge:        nextJudge,
						protocol:     requestProtocol,
						regexToProto: map[string]int{regex: protocolId},
					}
				}
			}
		}

		// Now make one request per judge/protocol and check all relevant regexes
		for _, item := range judgeRequests {
			html, err, responseTime, attempt := CheckProxyWithRetries(proxy, item.judge, item.protocol, maxTimeout, maxRetries)

			// Process each regex against the response
			for regex, protocolId := range item.regexToProto {
				statistic := domain.ProxyStatistic{
					Alive:        false,
					ResponseTime: uint16(responseTime),
					Attempt:      attempt,
					ProxyID:      proxy.ID,
					ProtocolID:   protocolId,
					JudgeID:      item.judge.ID,
					ResponseBody: truncateResponseBody(html),
				}

				if err == nil && CheckForValidResponse(html, regex) {
					lvl := support.GetProxyLevel(html)
					statistic.LevelID = &lvl
					statistic.Alive = true
				}

				jobruntime.AddProxyStatistic(statistic)
			}
		}

		// Requeue the proxy for the next check
		proxyqueue.PublicProxyQueue.RequeueProxy(proxy, scheduledTime)
	}
}

func CheckProxyWithRetries(proxy domain.Proxy, judge *domain.Judge, protocol string, timeout uint16, retries uint8) (string, error, int64, uint8) {
	var (
		html         string
		err          error
		responseTime int64
	)

	for i := uint8(0); i < retries; i++ {
		timeStart := time.Now()
		html, err = ProxyCheckRequest(proxy, judge, protocol, timeout)
		responseTime = time.Since(timeStart).Milliseconds()

		if err == nil {
			return html, err, responseTime, i
		}
	}

	return html, err, responseTime, retries
}

func truncateResponseBody(body string) string {
	if body == "" {
		return ""
	}

	runes := []rune(body)
	if len(runes) > maxResponseBodyLength {
		runes = runes[:maxResponseBodyLength]
	}

	return string(runes)
}
