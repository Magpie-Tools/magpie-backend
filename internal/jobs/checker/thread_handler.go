package checker

import (
	"context"
	"errors"
	"math"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"magpie/internal/config"
	"magpie/internal/database"
	"magpie/internal/domain"
	"magpie/internal/jobs/checker/judges"
	proxyqueue "magpie/internal/jobs/queue/proxy"
	jobruntime "magpie/internal/jobs/runtime"
	"magpie/internal/support"

	"github.com/charmbracelet/log"
)

var (
	currentThreads atomic.Uint32
	stopChannel    = make(chan struct{}) // Signal to stop threads
)

const maxResponseBodyLength = 4096

type userCheck struct {
	userID     uint
	regex      string
	protocolID int
}

type requestAssignment struct {
	judge    *domain.Judge
	protocol string
	checks   []userCheck
}

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
	ctx, cleanup := createWorkerContext()
	defer cleanup()

	for {
		proxy, scheduledTime, err := proxyqueue.PublicProxyQueue.GetNextProxyContext(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			time.Sleep(3 * time.Second)
			continue
		}

		proxy = refreshProxyUsers(proxy)

		judgeRequests, userSuccess, userHasChecks, maxTimeout, maxRetries := buildRequestAssignments(proxy)
		processJudgeAssignments(proxy, judgeRequests, userSuccess, maxTimeout, maxRetries)

		removedUsers, orphaned := handleFailureTracking(proxy, userSuccess, userHasChecks)
		if len(removedUsers) > 0 {
			proxy = filterRemovedUsers(proxy, removedUsers)
		}

		if len(orphaned) > 0 {
			if err := proxyqueue.PublicProxyQueue.RemoveFromQueue(orphaned); err != nil {
				log.Error("failed to remove orphaned proxies from queue", "error", err)
			}
		}

		hasUsers, err := database.ProxyHasUsers(proxy.ID)
		if err != nil {
			log.Error("failed to verify proxy ownership before requeue", "proxy_id", proxy.ID, "error", err)
			// Requeue to avoid dropping proxies on transient errors
			proxyqueue.PublicProxyQueue.RequeueProxy(proxy, scheduledTime)
			continue
		}
		if !hasUsers {
			//log.Debug("proxy no longer has associated users; skipping requeue", "proxy_id", proxy.ID)
			continue
		}

		// Requeue the proxy for the next check
		proxyqueue.PublicProxyQueue.RequeueProxy(proxy, scheduledTime)
	}
}

func createWorkerContext() (context.Context, func()) {
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

	cleanup := func() {
		cancel()
		<-done
	}

	return ctx, cleanup
}

func refreshProxyUsers(proxy domain.Proxy) domain.Proxy {
	refreshedUsers := make(map[uint]domain.User, len(proxy.Users))
	for i := range proxy.Users {
		if cached, ok := refreshedUsers[proxy.Users[i].ID]; ok {
			proxy.Users[i] = cached
			continue
		}

		fresh := database.GetUserFromId(proxy.Users[i].ID)
		if fresh.ID == 0 {
			continue
		}

		refreshedUsers[proxy.Users[i].ID] = fresh
		proxy.Users[i] = fresh
	}

	return proxy
}

func buildRequestAssignments(proxy domain.Proxy) (map[string]*requestAssignment, map[uint]bool, map[uint]bool, uint16, uint8) {
	judgeRequests := make(map[string]*requestAssignment)
	userSuccess := make(map[uint]bool, len(proxy.Users))
	userHasChecks := make(map[uint]bool, len(proxy.Users))

	var maxTimeout uint16
	var maxRetries uint8

	for _, user := range proxy.Users {
		userSuccess[user.ID] = false

		if user.Timeout > maxTimeout {
			maxTimeout = user.Timeout
		}
		if user.Retries > maxRetries {
			maxRetries = user.Retries
		}

		for protocol, protocolID := range user.GetProtocolMap() {
			requestProtocol := determineRequestProtocol(protocol, protocolID, user.UseHttpsForSocks)

			nextJudge, regex := judges.GetNextJudge(user.ID, requestProtocol)
			judgeKey := strconv.Itoa(int(nextJudge.ID)) + "_" + requestProtocol

			assignment, found := judgeRequests[judgeKey]
			if !found {
				assignment = &requestAssignment{
					judge:    nextJudge,
					protocol: requestProtocol,
				}
				judgeRequests[judgeKey] = assignment
			}

			assignment.checks = append(assignment.checks, userCheck{
				userID:     user.ID,
				regex:      regex,
				protocolID: protocolID,
			})
			userHasChecks[user.ID] = true
		}
	}

	return judgeRequests, userSuccess, userHasChecks, maxTimeout, maxRetries
}

func determineRequestProtocol(protocol string, protocolID int, useHTTPSForSocks bool) string {
	if protocolID > 2 {
		if useHTTPSForSocks {
			return "https"
		}
		return "http"
	}

	return protocol
}

func processJudgeAssignments(proxy domain.Proxy, assignments map[string]*requestAssignment, userSuccess map[uint]bool, maxTimeout uint16, maxRetries uint8) {
	for _, item := range assignments {
		html, err, responseTime, attempt := CheckProxyWithRetries(proxy, item.judge, item.protocol, maxTimeout, maxRetries)

		for _, check := range item.checks {
			statistic := domain.ProxyStatistic{
				Alive:        false,
				ResponseTime: uint16(responseTime),
				Attempt:      attempt,
				ProxyID:      proxy.ID,
				ProtocolID:   check.protocolID,
				JudgeID:      item.judge.ID,
				ResponseBody: truncateResponseBody(html),
			}

			if err == nil && CheckForValidResponse(html, check.regex) {
				lvl := support.GetProxyLevel(html)
				statistic.LevelID = &lvl
				statistic.Alive = true
				userSuccess[check.userID] = true
			}

			jobruntime.AddProxyStatistic(statistic)
		}
	}
}

func handleFailureTracking(proxy domain.Proxy, userSuccess, userHasChecks map[uint]bool) (map[uint]struct{}, []domain.Proxy) {
	removedUsers := make(map[uint]struct{})
	var orphaned []domain.Proxy

	for _, user := range proxy.Users {
		if !userHasChecks[user.ID] {
			continue
		}

		if userSuccess[user.ID] {
			if err := database.ResetUserProxyFailures(user.ID, proxy.ID); err != nil {
				log.Error("failed to reset proxy failure streak", "proxy_id", proxy.ID, "user_id", user.ID, "error", err)
			}
			continue
		}

		newCount, err := database.IncrementUserProxyFailures(user.ID, proxy.ID)
		if err != nil {
			log.Error("failed to track proxy failure streak", "proxy_id", proxy.ID, "user_id", user.ID, "error", err)
			continue
		}

		if newCount == 0 || !user.AutoRemoveFailingProxies || user.AutoRemoveFailureThreshold == 0 {
			continue
		}

		if newCount < uint16(user.AutoRemoveFailureThreshold) {
			continue
		}

		log.Info("auto-removing proxy after repeated failures", "proxy_id", proxy.ID, "user_id", user.ID, "failures", newCount)

		_, orphanedProxies, err := database.DeleteProxyRelation(user.ID, []int{int(proxy.ID)})
		if err != nil {
			log.Error("failed to auto remove proxy for user", "proxy_id", proxy.ID, "user_id", user.ID, "error", err)
			continue
		}

		removedUsers[user.ID] = struct{}{}
		if len(orphanedProxies) > 0 {
			orphaned = append(orphaned, orphanedProxies...)
		}
	}

	return removedUsers, orphaned
}

func filterRemovedUsers(proxy domain.Proxy, removed map[uint]struct{}) domain.Proxy {
	filtered := make([]domain.User, 0, len(proxy.Users))
	for _, user := range proxy.Users {
		if _, ok := removed[user.ID]; ok {
			continue
		}
		filtered = append(filtered, user)
	}
	proxy.Users = filtered

	return proxy
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

	body = strings.ReplaceAll(body, "\x00", "")
	if body == "" {
		return ""
	}

	runes := []rune(body)
	if len(runes) > maxResponseBodyLength {
		runes = runes[:maxResponseBodyLength]
	}

	return string(runes)
}
