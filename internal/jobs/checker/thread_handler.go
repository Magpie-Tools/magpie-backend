package checker

import (
	"context"
	"errors"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"magpie/internal/api/dto"
	"magpie/internal/checkerconfig"
	"magpie/internal/checkpace"
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
	currentThreads          atomic.Uint32
	stopChannel             = make(chan struct{}) // Signal to stop threads
	workspaceCache          sync.Map
	checkProxyWithRetries   = CheckProxyWithRetries
	enqueueProxyStatistic   = jobruntime.AddProxyStatisticForWorkspaces
	getWorkspacesForChecker = database.GetWorkspacesByIDsForChecker
)

const (
	maxResponseBodyLength = 512
	workspaceCacheTTL     = 5 * time.Second
)

type cachedWorkspace struct {
	workspace domain.Workspace
	expiresAt time.Time
}

type workspaceCheck struct {
	workspaceID uint
	regex       string
	configKey   string
	headers     []string
}

type requestAssignment struct {
	judge             *domain.Judge
	proxyProtocol     string
	transportProtocol string
	protocolID        int
	timeout           uint16
	retries           uint8
	budgetSet         bool
	checks            []workspaceCheck
}

func ThreadDispatcher(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}

	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	var workers sync.WaitGroup
	defer workers.Wait()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		cfg := config.GetConfig()

		var targetThreads uint32
		if cfg.Checker.DynamicThreads {
			targetThreads = getAutoThreads(cfg)
		} else {
			targetThreads = cfg.Checker.Threads
		}

		activeThreads := currentThreads.Load()
		for activeThreads < targetThreads {
			launchCheckerWorker(ctx, &workers)
			activeThreads = currentThreads.Load()
		}

		// Non-blocking stop requests avoid dispatcher stalls when counts drift.
		for activeThreads > targetThreads {
			if !requestCheckerWorkerStop() {
				break
			}
			activeThreads--
		}

		log.Debug("Checker threads", "active", currentThreads.Load())
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func launchCheckerWorker(ctx context.Context, workers *sync.WaitGroup) {
	currentThreads.Add(1)
	workers.Add(1)
	go func() {
		defer workers.Done()
		defer func() {
			if recovered := recover(); recovered != nil {
				log.Error("checker worker panicked", "panic", recovered)
			}
			currentThreads.Add(^uint32(0))
		}()

		work(ctx)
	}()
}

func requestCheckerWorkerStop() bool {
	select {
	case stopChannel <- struct{}{}:
		return true
	default:
		return false
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

	// Workspace allowances set the planned check rate. Before the first
	// allowance snapshot, assume one protocol per route at the fallback cadence.
	checksPerSecond := float64(totalProxies) / checkpace.FallbackInterval.Seconds()
	if pace := currentPace(); pace != nil {
		checksPerSecond = pace.ChecksPerSecond
	}
	perInstanceRate := checksPerSecond / float64(activeInstances)
	worstCaseSeconds := float64(cfg.Checker.Retries+1) * float64(cfg.Checker.Timeout) / 1000
	required := math.Ceil(perInstanceRate * worstCaseSeconds)
	if required > math.MaxUint32 {
		required = math.MaxUint32
	}
	requiredThreads := uint64(required)

	if requiredThreads == 0 && perInstanceProxies > 0 {
		requiredThreads = 1
	}

	maxThreads := cfg.Checker.MaxThreads
	if maxThreads == 0 {
		if cfg.Checker.Threads > 0 {
			maxThreads = cfg.Checker.Threads
		} else {
			maxThreads = 250
		}
	}
	if requiredThreads > uint64(maxThreads) {
		requiredThreads = uint64(maxThreads)
	}

	if requiredThreads > math.MaxUint32 {
		requiredThreads = math.MaxUint32
	}

	return uint32(requiredThreads)
}

// work owns one route lease at a time. Statistics are handed off asynchronously,
// while failure tracking and ownership verification wait for shared batches.
// If a cycle stops before completing its lease, a later dequeue can reclaim the
// route after expiry. Each worker owns its assignment and success maps.
func work(parent context.Context) {
	ctx, cleanup := createWorkerContext(parent)
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

		var payloadChanged bool
		proxy, payloadChanged = refreshProxyWorkspaces(proxy)

		dueProxy, anyDue := selectDueWorkspaces(proxy, scheduledTime)
		if !anyDue {
			// Every owner is between cadence boundaries or out of allowance.
			// Revisit later without outbound requests or database work.
			if err := requeueCheckedProxy(proxy, scheduledTime, payloadChanged); err != nil {
				log.Error("failed to requeue proxy without due workspaces", "proxy_id", proxy.ID, "error", err)
			}
			continue
		}

		judgeRequests, workspaceSuccess, workspaceHasChecks, maxTimeout, maxRetries := buildRequestAssignments(dueProxy)
		saveResponses := config.GetConfig().Checker.SaveResponses
		if err := processJudgeAssignments(proxy, judgeRequests, workspaceSuccess, maxTimeout, maxRetries, saveResponses); err != nil {
			continue
		}
		if err := proxyqueue.PublicProxyQueue.RenewLeaseIfNeeded(ctx, proxy); err != nil {
			continue
		}

		removedWorkspaces, orphaned := handleFailureTracking(proxy, workspaceSuccess, workspaceHasChecks)
		payloadChanged = payloadChanged || len(removedWorkspaces) > 0
		if len(removedWorkspaces) > 0 {
			proxy = filterRemovedWorkspaces(proxy, removedWorkspaces)
		}

		if len(orphaned) > 0 {
			for i := range orphaned {
				if orphaned[i].ID == proxy.ID {
					orphaned[i].QueueLease = proxy.QueueLease
				}
			}
			if err := proxyqueue.PublicProxyQueue.RemoveFromQueue(orphaned); err != nil {
				log.Error("failed to remove orphaned proxies from queue", "error", err)
			}
		}

		hasUsers, err := verifyProxyOwnership(ctx, proxy.ID)
		if err != nil {
			log.Error("failed to verify proxy ownership before requeue", "proxy_id", proxy.ID, "error", err)
			// Requeue to avoid dropping proxies on transient errors
			if err := requeueCheckedProxy(proxy, scheduledTime, payloadChanged); err != nil {
				log.Error("failed to requeue proxy after ownership check error", "proxy_id", proxy.ID, "error", err)
			}
			continue
		}
		if !hasUsers {
			if err := proxyqueue.PublicProxyQueue.RemoveFromQueue([]domain.Proxy{proxy}); err != nil {
				log.Error("failed to remove proxy without owners from queue", "proxy_id", proxy.ID, "error", err)
			}
			continue
		}

		// Requeue the proxy for the next check
		if err := requeueCheckedProxy(proxy, scheduledTime, payloadChanged); err != nil {
			log.Error("failed to requeue proxy", "proxy_id", proxy.ID, "error", err)
		}
	}
}

// selectDueWorkspaces keeps the owners whose monthly allowance cadence is due
// this cycle. The route's fastest owner is always due, so single-owner routes
// return the proxy unchanged without allocating. The decision uses only the
// in-memory allowance snapshot: no Redis, database, JSON or cryptographic work.
func selectDueWorkspaces(proxy domain.Proxy, scheduled time.Time) (domain.Proxy, bool) {
	pace := currentPace()
	if pace == nil || len(proxy.Workspaces) == 0 {
		return proxy, len(proxy.Workspaces) > 0
	}
	routeInterval := pace.RouteCheckInterval(proxy.Workspaces)
	var due []domain.Workspace
	for index, workspace := range proxy.Workspaces {
		if pace.CheckDue(workspace.ID, proxy.ID, scheduled, routeInterval) {
			if due != nil {
				due = append(due, workspace)
			}
			continue
		}
		if due == nil {
			due = make([]domain.Workspace, index, len(proxy.Workspaces))
			copy(due, proxy.Workspaces[:index])
		}
	}
	if due == nil {
		return proxy, true
	}
	filtered := proxy
	filtered.Workspaces = due
	return filtered, len(due) > 0
}

var currentPace = checkpace.Current

// requeueCheckedProxy rewrites the payload only when queued ownership changed.
// Refreshed checker settings live in runtime snapshots and do not need a payload
// write. Normal cycles must keep serialization and encryption out of requeueing.
func requeueCheckedProxy(proxy domain.Proxy, scheduledTime time.Time, persistPayload bool) error {
	if persistPayload {
		return proxyqueue.PublicProxyQueue.RequeueProxyWithPayload(proxy, scheduledTime)
	}
	return proxyqueue.PublicProxyQueue.RequeueProxy(proxy, scheduledTime)
}

func createWorkerContext(parent context.Context) (context.Context, func()) {
	if parent == nil {
		parent = context.Background()
	}

	ctx, cancel := context.WithCancel(parent)
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

// refreshProxyWorkspaces replaces queued workspace stubs with current settings.
// The bool reports an ownership-list change requiring a payload rewrite, not a
// settings change. Before snapshots are ready, the database/cache fallback keeps
// unknown workspaces on lookup failure so a transient error cannot remove them.
func refreshProxyWorkspaces(proxy domain.Proxy) (domain.Proxy, bool) {
	if len(proxy.Workspaces) == 0 {
		return proxy, false
	}

	ids := make([]uint, 0, len(proxy.Workspaces))
	seen := make(map[uint]struct{}, len(proxy.Workspaces))
	for _, workspace := range proxy.Workspaces {
		if workspace.ID == 0 {
			continue
		}
		if _, ok := seen[workspace.ID]; ok {
			continue
		}
		seen[workspace.ID] = struct{}{}
		ids = append(ids, workspace.ID)
	}

	if _, ready := lookupCheckerWorkspace(0); ready {
		refreshed := make([]domain.Workspace, 0, len(ids))
		for _, id := range ids {
			if snapshot, _ := lookupCheckerWorkspace(id); snapshot != nil {
				refreshed = append(refreshed, snapshot.Value)
			}
		}
		changed := len(refreshed) != len(proxy.Workspaces)
		proxy.Workspaces = refreshed
		return proxy, changed
	}
	now := time.Now()
	refreshedWorkspaces := make(map[uint]domain.Workspace, len(ids))
	missing := make([]uint, 0, len(ids))

	for _, id := range ids {
		if cached, ok := workspaceCache.Load(id); ok {
			entry, ok := cached.(cachedWorkspace)
			if ok && entry.expiresAt.After(now) {
				refreshedWorkspaces[id] = entry.workspace
				continue
			}
			workspaceCache.Delete(id)
		}
		missing = append(missing, id)
	}

	if len(missing) > 0 {
		dbWorkspaces, err := getWorkspacesForChecker(missing)
		if err != nil {
			// Preserve the historical log message for existing log consumers.
			log.Error("refresh proxy users", "error", err)
			for i := range proxy.Workspaces {
				if fresh, ok := refreshedWorkspaces[proxy.Workspaces[i].ID]; ok {
					proxy.Workspaces[i] = fresh
				}
			}
			return proxy, false
		} else {
			expiry := now.Add(workspaceCacheTTL)
			for id, workspace := range dbWorkspaces {
				refreshedWorkspaces[id] = workspace
				workspaceCache.Store(id, cachedWorkspace{
					workspace: workspace,
					expiresAt: expiry,
				})
			}
		}
	}

	refreshed := make([]domain.Workspace, 0, len(proxy.Workspaces))
	payloadChanged := false
	seenRefreshed := make(map[uint]struct{}, len(proxy.Workspaces))
	for _, queuedWorkspace := range proxy.Workspaces {
		fresh, ok := refreshedWorkspaces[queuedWorkspace.ID]
		if !ok {
			payloadChanged = true
			continue
		}
		if _, duplicate := seenRefreshed[fresh.ID]; duplicate {
			payloadChanged = true
			continue
		}
		seenRefreshed[fresh.ID] = struct{}{}
		refreshed = append(refreshed, fresh)
	}
	proxy.Workspaces = refreshed

	return proxy, payloadChanged
}

var lookupCheckerWorkspace = checkerconfig.Lookup

// judgePick remembers a cycle's judge position for one protocol and judge set.
type judgePick struct {
	protocolIndex int
	judgeSet      uint32
}

// buildRequestAssignments shares a physical request only when judge, protocol,
// transport, timeout and retry budget match. Validation remains per workspace.
// Owners whose plans list the same judges for a protocol use the same judge in
// a cycle; only the first such plan advances its round-robin position, so
// independent counters cannot split one route into one request per judge.
// The returned success and eligibility maps use workspace IDs; no eligible judge
// means no failure-streak update. Maximum budgets support fallback assignments.
func buildRequestAssignments(proxy domain.Proxy) (map[string]*requestAssignment, map[uint]bool, map[uint]bool, uint16, uint8) {
	requests := make(map[string]*requestAssignment)
	var picks map[judgePick]int
	if len(proxy.Workspaces) > 1 {
		picks = make(map[judgePick]int, len(domain.CheckerProtocols))
	}
	success, hasChecks := make(map[uint]bool, len(proxy.Workspaces)), make(map[uint]bool, len(proxy.Workspaces))
	var maxTimeout uint16
	var maxRetries uint8
	for workspaceIndex, workspace := range proxy.Workspaces {
		success[workspace.ID] = false
		var plan *checkerconfig.Plan
		if snapshot, ready := lookupCheckerWorkspace(workspace.ID); ready {
			if snapshot == nil {
				continue
			}
			workspace = snapshot.Value
			proxy.Workspaces[workspaceIndex] = workspace
			plan = snapshot.Plan(proxy.ID)
		}
		var resolved [4]dto.ProtocolCheckSettings
		if plan != nil {
			resolved = plan.Settings
		} else {
			resolved = domain.ResolveCheckerSettings(workspace.DefaultCheckerSettings(), nil)
		}
		for index, setting := range resolved {
			if !setting.Enabled {
				continue
			}
			protocol, protocolID := domain.CheckerProtocols[index], index+1
			transport := support.ResolveCheckerTransportProtocol(setting.Transport)
			if setting.Timeout > maxTimeout {
				maxTimeout = setting.Timeout
			}
			if setting.Retries > maxRetries {
				maxRetries = setting.Retries
			}
			var judge *domain.Judge
			var regex, configKey string
			var headers []string
			if plan != nil {
				judge, regex = nextPlanJudge(plan, index, picks)
				configKey = plan.Keys[index]
				headers = plan.Headers
			} else {
				scheme := determineJudgeScheme(protocol, protocolID, workspace.UseHttpsForSocks)
				if support.IsHTTP3Transport(transport) {
					scheme = "https"
				}
				judge, regex = judges.GetNextJudge(workspace.ID, scheme)
			}
			if judge == nil || config.IsWebsiteBlocked(judge.FullString) {
				continue
			}
			key := strconv.Itoa(int(judge.ID)) + "_" + protocol + "_" + transport + "_" + strconv.Itoa(int(setting.Timeout)) + "_" + strconv.Itoa(int(setting.Retries))
			assignment := requests[key]
			if assignment == nil {
				assignment = &requestAssignment{judge: judge, proxyProtocol: protocol, transportProtocol: transport, protocolID: protocolID, timeout: setting.Timeout, retries: setting.Retries, budgetSet: true}
				requests[key] = assignment
			}
			assignment.checks = append(assignment.checks, workspaceCheck{workspaceID: workspace.ID, regex: regex, configKey: configKey, headers: headers})
			hasChecks[workspace.ID] = true
		}
	}
	return requests, success, hasChecks, maxTimeout, maxRetries
}

// nextPlanJudge reuses the cycle's position for an identical judge set. A nil
// picks map means the route has one owner and needs no coordination.
func nextPlanJudge(plan *checkerconfig.Plan, index int, picks map[judgePick]int) (*domain.Judge, string) {
	list := plan.Judges[index]
	// Hand-built plans without a set identity keep independent rotation.
	if picks == nil || len(list) < 2 || plan.JudgeSets[index] == 0 {
		return plan.NextJudge(index)
	}
	key := judgePick{protocolIndex: index, judgeSet: plan.JudgeSets[index]}
	position, ok := picks[key]
	if !ok {
		position = plan.NextJudgeIndex(index)
		picks[key] = position
	}
	item := list[position]
	return item.Judge, item.Regex
}

func determineJudgeScheme(protocol string, protocolID int, useHTTPSForSocks bool) string {
	if protocolID <= 2 {
		return protocol
	}
	if useHTTPSForSocks {
		return "https"
	}
	return "http"
}

// processJudgeAssignments runs assignments serially and updates the worker's
// success map. Each physical check hands off one statistic with independent
// workspace verdicts, even on a transport failure. Lease loss or cancellation
// aborts the cycle; statistics already handed off are not undone. A nil error
// does not mean those statistics have committed to PostgreSQL.
func processJudgeAssignments(proxy domain.Proxy, assignments map[string]*requestAssignment, workspaceSuccess map[uint]bool, maxTimeout uint16, maxRetries uint8, saveResponses bool) error {
	for _, item := range assignments {
		timeout, retries := item.timeout, item.retries
		if !item.budgetSet {
			timeout, retries = maxTimeout, maxRetries
		}
		html, err, responseTime, attempt := checkProxyWithRetries(proxy, item.judge, item.proxyProtocol, item.transportProtocol, timeout, retries)
		leaseLost := errors.Is(err, proxyqueue.ErrProxyLeaseLost)
		// Charge every outbound request, retries included, before anything
		// can abandon the cycle. Metering does not wait for the statistics
		// pipeline, which may drop results under load.
		chargeCheckAttempts(item.checks, requestsMade(attempt, leaseLost))
		if leaseLost {
			return err
		}
		if ctxErr := proxyCheckContext(proxy).Err(); ctxErr != nil {
			return ctxErr
		}
		truncatedBody := ""
		if saveResponses {
			truncatedBody = truncateResponseBody(html)
		}
		responseValidByRegex := make(map[string]bool, len(item.checks))
		statAlive := false
		createdAt := time.Now().UTC()
		evidence := make([]domain.WorkspaceCheckEvidence, 0, len(item.checks))

		for _, check := range item.checks {
			validationKey := check.regex
			if strings.EqualFold(validationKey, "default") {
				validationKey += check.configKey
			}
			validResponse, ok := responseValidByRegex[validationKey]
			if !ok {
				validResponse = err == nil && checkForValidResponseWithHeaders(html, check.regex, check.headers)
				responseValidByRegex[validationKey] = validResponse
			}
			if check.configKey != "" {
				evidence = append(evidence, domain.WorkspaceCheckEvidence{WorkspaceID: check.workspaceID, ConfigKey: check.configKey, Alive: validResponse})
			}
			if validResponse {
				statAlive = true
				workspaceSuccess[check.workspaceID] = true
			}
		}

		statistic := domain.ProxyStatistic{
			TransportProtocol: item.transportProtocol, CheckTimeout: timeout, CheckRetries: retries, CheckEvidence: evidence,
			Alive:        statAlive,
			ResponseTime: uint16(responseTime),
			Attempt:      attempt,
			ProxyID:      proxy.ID,
			ProtocolID:   item.protocolID,
			JudgeID:      item.judge.ID,
			ResponseBody: truncatedBody,
			CreatedAt:    createdAt,
		}
		if statAlive {
			statistic.LevelID = new(support.GetProxyLevel(html))
		}

		workspaceIDs := collectCheckWorkspaceIDs(item.checks)
		enqueueProxyStatistic(statistic, workspaceIDs)
	}
	return nil
}

// requestsMade converts checkProxyWithRetries' zero-based attempt index into
// a request count. When the lease is lost, the index is the request that was
// never sent.
func requestsMade(attempt uint8, leaseLost bool) uint64 {
	if leaseLost {
		return uint64(attempt)
	}
	return uint64(attempt) + 1
}

// chargeCheckAttempts charges each workspace sharing a request once.
// Assignments rarely serve more than a few workspaces, so a linear duplicate
// scan avoids allocating.
func chargeCheckAttempts(checks []workspaceCheck, requests uint64) {
	if requests == 0 {
		return
	}
	for index, check := range checks {
		if check.workspaceID == 0 {
			continue
		}
		duplicate := false
		for _, earlier := range checks[:index] {
			if earlier.workspaceID == check.workspaceID {
				duplicate = true
				break
			}
		}
		if !duplicate {
			chargeChecks(check.workspaceID, requests)
		}
	}
}

var chargeChecks = checkpace.ChargeChecks

func collectCheckWorkspaceIDs(checks []workspaceCheck) []uint {
	if len(checks) == 0 {
		return nil
	}

	seen := make(map[uint]struct{}, len(checks))
	workspaceIDs := make([]uint, 0, len(checks))
	for _, check := range checks {
		if check.workspaceID == 0 {
			continue
		}
		if _, ok := seen[check.workspaceID]; ok {
			continue
		}
		seen[check.workspaceID] = struct{}{}
		workspaceIDs = append(workspaceIDs, check.workspaceID)
	}
	return workspaceIDs
}

func handleFailureTracking(proxy domain.Proxy, workspaceSuccess, workspaceHasChecks map[uint]bool) (map[uint]struct{}, []domain.Proxy) {
	if len(proxy.Workspaces) == 0 {
		return nil, nil
	}

	events := make([]failureEvent, 0, len(proxy.Workspaces))
	for _, workspace := range proxy.Workspaces {
		if !workspaceHasChecks[workspace.ID] {
			continue
		}
		events = append(events, failureEvent{
			WorkspaceID:       workspace.ID,
			Success:           workspaceSuccess[workspace.ID],
			AutoRemove:        workspace.AutoRemoveFailingProxies,
			FailureThreshold:  workspace.AutoRemoveFailureThreshold,
			FailureAction:     workspace.EffectiveFailureAction(),
			HasEligibleChecks: true,
		})
	}

	if len(events) == 0 {
		return nil, nil
	}

	reqCtx, cancel := context.WithTimeout(context.Background(), failureProcessingTimeout)
	defer cancel()
	removed, orphaned, err := processFailureEvents(reqCtx, proxy.ID, events)
	if err != nil {
		log.Error("failed to process proxy failure tracking", "proxy_id", proxy.ID, "error", err)
	}

	return removed, orphaned
}

func filterRemovedWorkspaces(proxy domain.Proxy, removed map[uint]struct{}) domain.Proxy {
	filtered := make([]domain.Workspace, 0, len(proxy.Workspaces))
	for _, workspace := range proxy.Workspaces {
		if _, ok := removed[workspace.ID]; ok {
			continue
		}
		filtered = append(filtered, workspace)
	}
	proxy.Workspaces = filtered

	return proxy
}

// CheckProxyWithRetries makes an initial request plus up to retries additional
// requests, stopping on transport success. Workspace response validation happens
// afterward and does not trigger retries. The body and timing describe the last
// request. Timing is in milliseconds; the fourth result is a zero-based attempt
// index, not a request count. A failed lease renewal returns before that attempt
// and joins ErrProxyLeaseLost to the error so the worker abandons the cycle.
func CheckProxyWithRetries(proxy domain.Proxy, judge *domain.Judge, protocol string, transportProtocol string, timeout uint16, retries uint8) (string, error, int64, uint8) {
	var (
		html         string
		err          error
		responseTime int64
	)

	for i := uint16(0); i <= uint16(retries); i++ {
		ctx := proxyCheckContext(proxy)
		if err := proxyqueue.PublicProxyQueue.RenewLeaseIfNeeded(ctx, proxy); err != nil {
			return html, errors.Join(proxyqueue.ErrProxyLeaseLost, err), responseTime, uint8(i)
		}
		timeStart := time.Now()
		html, err = proxyCheckRequestContext(ctx, proxy, judge, protocol, transportProtocol, timeout)
		responseTime = time.Since(timeStart).Milliseconds()

		if err == nil {
			return html, err, responseTime, uint8(i)
		}
	}

	return html, err, responseTime, retries
}

func proxyCheckContext(proxy domain.Proxy) context.Context {
	if proxy.QueueLease != nil && proxy.QueueLease.Context != nil {
		return proxy.QueueLease.Context
	}
	return context.Background()
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
