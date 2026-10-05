package proxyqueue

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"strconv"
	"strings"
	"time"

	"magpie/internal/config"
	"magpie/internal/domain"
	queueutil "magpie/internal/jobs/queue"
	"magpie/internal/jobs/runtime"
	"magpie/internal/security"
	"magpie/internal/support"

	"github.com/charmbracelet/log"
	"github.com/redis/go-redis/v9"
)

const (
	proxyKeyPrefix             = "proxy:"
	queuedProxyVersion         = 3
	envEncryptQueueCredentials = "PROXY_QUEUE_ENCRYPT_CREDENTIALS"

	legacyQueueKey      = "proxy_queue"
	proxyQueueHeadKey   = "proxy_queue_heads"
	queueShardKeyPrefix = "proxy_queue:"
	defaultQueueShards  = 16
	maxQueueShards      = 128
	minDequeueSleep     = 10 * time.Millisecond
	idleQueueSleep      = 250 * time.Millisecond
	maxDequeueSleep     = 2 * time.Second
	processingLease     = 5 * time.Minute
	// Due scores are whole Unix milliseconds. The exactly representable half
	// millisecond marks worker leases without another key or Redis operation.
	leaseScoreMarker        = 0.5
	queueRescheduleLockKey  = "magpie:leader:proxy_queue_reschedule"
	queueRescheduleStateKey = "magpie:queue:proxy:interval_ms"
)

//go:embed pop.lua
var luaPopScript string

//go:embed complete.lua
var luaCompleteScript string

//go:embed renew.lua
var luaRenewScript string

//go:embed remove.lua
var luaRemoveScript string

//go:embed add.lua
var luaAddScript string

//go:embed requeue_all.lua
var luaRequeueAllScript string

//go:embed migrate.lua
var luaMigrateScript string

var completeScript = redis.NewScript(luaCompleteScript)
var renewScript = redis.NewScript(luaRenewScript)
var removeScript = redis.NewScript(luaRemoveScript)
var requeueAllScript = redis.NewScript(luaRequeueAllScript)
var migrateScript = redis.NewScript(luaMigrateScript)
var ErrProxyLeaseLost = errors.New("proxy processing lease lost")

type RedisProxyQueue struct {
	client         *redis.Client
	ctx            context.Context
	popScript      *redis.Script
	queueShardKeys []string
	popQueueKeys   []string
}

type proxyPopResult struct {
	Found       bool
	Member      string
	ProxyJSON   string
	ScoreMs     int64
	NextReadyMs int64
	QueueKey    string
}

type queuedProxyUser struct {
	ID uint `json:"ID"`
}

type queuedProxy struct {
	Version           uint8             `json:"Version,omitempty"`
	ID                uint64            `json:"ID"`
	IP                string            `json:"IP"`
	Port              uint16            `json:"Port"`
	UsernameEncrypted string            `json:"UsernameEncrypted,omitempty"`
	PasswordEncrypted string            `json:"PasswordEncrypted,omitempty"`
	Username          string            `json:"Username,omitempty"`
	Password          string            `json:"Password,omitempty"`
	Hash              []byte            `json:"Hash,omitempty"`
	WorkspaceIDs      []uint            `json:"WorkspaceIDs,omitempty"`
	UserIDs           []uint            `json:"UserIDs,omitempty"` // Version 2 compatibility
	Users             []queuedProxyUser `json:"Users,omitempty"`   // Legacy payload compatibility
}

var PublicProxyQueue RedisProxyQueue
var runLeaderTaskOnce = support.RunLeaderTaskOnce

func init() {
	client, err := support.GetRedisClient()
	if err != nil {
		log.Warn("Redis unavailable during proxy queue init; continuing in degraded mode", "error", err)
	}
	PublicProxyQueue = *NewRedisProxyQueue(client)
}

// StartIntervalUpdates runs after settings load. Package initialization must
// not publish the one-second placeholder to a shared queue.
func (rpq *RedisProxyQueue) StartIntervalUpdates(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	updates := config.CheckIntervalUpdates()
	for {
		select {
		case <-ctx.Done():
			return
		case interval := <-updates:
			err := applyIntervalUpdateAsLeader(
				queueRescheduleLockKey,
				interval,
				rpq.Reschedule,
			)
			if err != nil {
				log.Error("Failed to reschedule proxy queue after interval update", "error", err)
			}
		}
	}
}

func applyIntervalUpdateAsLeader(lockKey string, interval time.Duration, reschedule func(time.Duration) error) error {
	return applyIntervalUpdateAsLeaderWithRunner(runLeaderTaskOnce, lockKey, interval, reschedule)
}

func applyIntervalUpdateAsLeaderWithRunner(
	runner func(context.Context, string, time.Duration, func(context.Context) error) error,
	lockKey string,
	interval time.Duration,
	reschedule func(time.Duration) error,
) error {
	if runner == nil {
		return errors.New("leader runner is nil")
	}
	if reschedule == nil {
		return errors.New("reschedule function is nil")
	}

	err := runner(context.Background(), lockKey, support.DefaultLeadershipTTL, func(context.Context) error {
		return reschedule(interval)
	})
	if errors.Is(err, support.ErrLeaderLockNotAcquired) {
		return nil
	}
	return err
}

func NewRedisProxyQueue(client *redis.Client) *RedisProxyQueue {
	shards := support.GetEnvInt("PROXY_QUEUE_SHARDS", defaultQueueShards)
	if shards <= 0 {
		shards = defaultQueueShards
	}
	if shards > maxQueueShards {
		shards = maxQueueShards
	}

	shardKeys := buildQueueShardKeys(shards)
	popKeys := buildPopQueueKeys(shardKeys)

	queue := &RedisProxyQueue{
		client:         client,
		ctx:            context.Background(),
		popScript:      redis.NewScript(luaPopScript),
		queueShardKeys: shardKeys,
		popQueueKeys:   popKeys,
	}
	if client != nil {
		if err := queue.refreshQueueHeads(); err != nil {
			log.Warn("proxy queue head refresh failed", "error", err)
		}
	}
	return queue
}

func (rpq *RedisProxyQueue) clientOrErr() (*redis.Client, error) {
	if rpq == nil {
		return nil, errors.New("redis proxy queue is nil")
	}
	if rpq.client != nil {
		return rpq.client, nil
	}

	client, err := support.GetRedisClient()
	if err != nil {
		return nil, fmt.Errorf("redis proxy queue unavailable: %w", err)
	}
	return client, nil
}

func (rpq *RedisProxyQueue) baseContext() context.Context {
	if rpq == nil || rpq.ctx == nil {
		return context.Background()
	}
	return rpq.ctx
}

func (rpq *RedisProxyQueue) popKeys() []string {
	if len(rpq.popQueueKeys) == 0 {
		return []string{legacyQueueKey}
	}
	return rpq.popQueueKeys
}

func (rpq *RedisProxyQueue) refreshQueueHeads() error {
	client, err := rpq.clientOrErr()
	if err != nil {
		return err
	}
	return queueutil.RefreshHeads(rpq.baseContext(), client, proxyQueueHeadKey, rpq.popKeys())
}

func buildQueueShardKeys(shards int) []string {
	keys := make([]string, shards)
	for i := 0; i < shards; i++ {
		keys[i] = fmt.Sprintf("%s%d", queueShardKeyPrefix, i)
	}
	return keys
}

func buildPopQueueKeys(shardKeys []string) []string {
	keys := make([]string, 0, len(shardKeys)+1)
	keys = append(keys, legacyQueueKey)
	keys = append(keys, shardKeys...)
	return keys
}

func (rpq *RedisProxyQueue) queueKeyForMember(member string) string {
	if rpq == nil || len(rpq.queueShardKeys) == 0 {
		return legacyQueueKey
	}
	idx := proxyQueueShardIndex(member, len(rpq.queueShardKeys))
	return rpq.queueShardKeys[idx]
}

func proxyQueueShardIndex(member string, shards int) int {
	if shards <= 1 {
		return 0
	}

	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(member))
	return int(hasher.Sum32() % uint32(shards))
}

func (rpq *RedisProxyQueue) AddToQueue(proxies []domain.Proxy) error {
	if len(proxies) == 0 {
		return nil
	}

	queueable := make([]domain.Proxy, 0, len(proxies))
	for _, proxy := range proxies {
		if hasQueuedWorkspace(proxy.Workspaces) {
			queueable = append(queueable, proxy)
		}
	}
	if len(queueable) == 0 {
		return nil
	}

	client, err := rpq.clientOrErr()
	if err != nil {
		return err
	}
	ctx := rpq.baseContext()

	pipe := client.Pipeline()
	interval := config.GetTimeBetweenChecks()
	now := time.Now()
	proxyLenDuration := time.Duration(len(queueable))
	batchSize := 500 // Adjust based on your Redis server capabilities

	for i, proxy := range queueable {
		offset := (interval * time.Duration(i)) / proxyLenDuration
		nextCheck := now.Add(offset)
		hashKey := string(proxy.Hash)
		proxyKey := proxyKeyPrefix + hashKey
		queueKey := rpq.queueKeyForMember(hashKey)

		proxyJSON, err := marshalQueuedProxy(proxy)
		if err != nil {
			return fmt.Errorf("failed to marshal proxy: %w", err)
		}

		// Imports may update ownership while a worker holds a lease in a legacy
		// or different configured shard. Keep that member in its owned shard.
		keys := append([]string{proxyQueueHeadKey, proxyKey, queueKey}, rpq.popKeys()...)
		pipe.Eval(ctx, luaAddScript, keys, hashKey, nextCheck.UnixMilli(), proxyJSON)

		// Execute in batches to prevent oversized pipelines
		if i%batchSize == 0 && i > 0 {
			if _, err := pipe.Exec(ctx); err != nil {
				return fmt.Errorf("batch pipeline failed: %w", err)
			}
			pipe = client.Pipeline()
		}
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("final pipeline exec failed: %w", err)
	}

	return nil
}

func hasQueuedWorkspace(workspaces []domain.Workspace) bool {
	for _, workspace := range workspaces {
		if workspace.ID != 0 {
			return true
		}
	}
	return false
}

func (rpq *RedisProxyQueue) RemoveFromQueue(proxies []domain.Proxy) error {
	if rpq == nil {
		return errors.New("redis proxy queue is nil")
	}
	if len(proxies) == 0 {
		return nil
	}

	client, err := rpq.clientOrErr()
	if err != nil {
		return err
	}
	ctx := rpq.baseContext()

	const batchSize = 500
	pipe := client.Pipeline()
	opCount := 0

	flush := func() error {
		if opCount == 0 {
			return nil
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return fmt.Errorf("remove pipeline exec failed: %w", err)
		}
		pipe = client.Pipeline()
		opCount = 0
		return nil
	}

	for _, proxy := range proxies {
		if len(proxy.Hash) == 0 {
			continue
		}

		hashKey := string(proxy.Hash)
		proxyKey := proxyKeyPrefix + hashKey
		queueKey := rpq.queueKeyForMember(hashKey)
		if proxy.QueueLease != nil {
			removed, err := removeScript.Run(ctx, client,
				[]string{proxyKey, proxy.QueueLease.QueueKey, legacyQueueKey}, hashKey, proxy.QueueLease.ScoreMS, proxy.QueueLease.Payload).Int64()
			if err != nil {
				return err
			}
			if removed == 0 {
				return ErrProxyLeaseLost
			}
			continue
		}

		pipe.Del(ctx, proxyKey)
		opCount++
		pipe.ZRem(ctx, queueKey, hashKey)
		opCount++
		if queueKey != legacyQueueKey {
			pipe.ZRem(ctx, legacyQueueKey, hashKey)
			opCount++
		}

		if opCount >= batchSize {
			if err := flush(); err != nil {
				return err
			}
		}
	}

	return flush()
}

func (rpq *RedisProxyQueue) GetNextProxy() (domain.Proxy, time.Time, error) {
	return rpq.GetNextProxyContext(rpq.baseContext())
}

func (rpq *RedisProxyQueue) GetNextProxyContext(ctx context.Context) (domain.Proxy, time.Time, error) {
	if ctx == nil {
		ctx = rpq.baseContext()
	}
	client, err := rpq.clientOrErr()
	if err != nil {
		return domain.Proxy{}, time.Time{}, err
	}
	popScript := rpq.popScript
	if popScript == nil {
		popScript = redis.NewScript(luaPopScript)
	}

	for {
		select {
		case <-ctx.Done():
			return domain.Proxy{}, time.Time{}, ctx.Err()
		default:
		}

		currentTimeMs := time.Now().UnixMilli()
		result, err := popScript.Run(
			ctx,
			client,
			[]string{proxyQueueHeadKey},
			currentTimeMs,
			int64(processingLease/time.Millisecond),
			proxyKeyPrefix,
		).Result()

		if err != nil {
			return domain.Proxy{}, time.Time{}, fmt.Errorf("lua script failed: %w", err)
		}

		popResult, err := parseProxyPopResult(result)
		if err != nil {
			return domain.Proxy{}, time.Time{}, fmt.Errorf("invalid lua response: %w", err)
		}
		if !popResult.Found {
			waitDuration := dequeueWaitDuration(popResult.NextReadyMs, currentTimeMs)
			if err := waitForNextProxyPoll(ctx, waitDuration); err != nil {
				return domain.Proxy{}, time.Time{}, err
			}
			continue
		}

		var payload queuedProxy
		if err := json.Unmarshal([]byte(popResult.ProxyJSON), &payload); err != nil {
			return domain.Proxy{}, time.Time{}, fmt.Errorf("failed to unmarshal proxy: %w", err)
		}
		rewritePayload := payload.needsRewrite()
		proxy, err := payload.toDomainProxy()
		if err != nil {
			return domain.Proxy{}, time.Time{}, fmt.Errorf("decode queued proxy: %w", err)
		}
		if payload.Version >= queuedProxyVersion && len(payload.Hash) > 0 && popResult.Member != string(payload.Hash) {
			return domain.Proxy{}, time.Time{}, errors.New("queued proxy hash does not match its sorted-set member")
		}
		leaseScoreMs := currentTimeMs + int64(processingLease/time.Millisecond)
		claimed, err := rpq.migrateDequeuedProxyMember(ctx, client, &popResult, &proxy, leaseScoreMs, rewritePayload)
		if err != nil {
			return domain.Proxy{}, time.Time{}, fmt.Errorf("migrate queued proxy member: %w", err)
		}
		if !claimed {
			// Another worker already owns the canonical route, or this candidate
			// changed during decoding. Migration must not return a second check.
			continue
		}

		proxy.QueueLease = &domain.ProxyQueueLease{
			ScoreMS: leaseScoreMs, QueueKey: popResult.QueueKey,
			Payload: popResult.ProxyJSON, Context: ctx,
		}
		return proxy, time.UnixMilli(popResult.ScoreMs), nil
	}
}

func (rpq *RedisProxyQueue) migrateDequeuedProxyMember(
	ctx context.Context,
	client *redis.Client,
	popResult *proxyPopResult,
	proxy *domain.Proxy,
	leaseScoreMs int64,
	rewritePayload bool,
) (bool, error) {
	if client == nil || popResult == nil || proxy == nil {
		return false, errors.New("queue member migration requires a client, pop result and proxy")
	}

	oldMember := popResult.Member
	newMember := string(proxy.Hash)
	if oldMember == "" || newMember == "" {
		return false, errors.New("queue member migration requires nonempty hashes")
	}
	if oldMember == newMember && !rewritePayload {
		return true, nil
	}

	payload, err := marshalQueuedProxy(*proxy)
	if err != nil {
		return false, err
	}

	newQueueKey := rpq.queueKeyForMember(newMember)
	keys := append([]string{proxyQueueHeadKey, proxyKeyPrefix + oldMember, proxyKeyPrefix + newMember, popResult.QueueKey, newQueueKey}, rpq.popKeys()...)
	result, err := migrateScript.Run(ctx, client, keys,
		oldMember, newMember, leaseScoreMs, time.Now().UnixMilli(), payload, popResult.ProxyJSON,
		strconv.FormatBool(encryptProxyQueueCredentials())).Slice()
	if err != nil {
		return false, err
	}
	if len(result) != 3 {
		return false, fmt.Errorf("unexpected migration response length %d", len(result))
	}
	claimed, err := coerceLuaInt64(result[0])
	if err != nil || claimed == 0 {
		return false, err
	}
	current, err := coerceLuaString(result[1])
	if err != nil {
		return false, err
	}
	queueKey, err := coerceLuaString(result[2])
	if err != nil {
		return false, err
	}
	if current != string(payload) {
		var merged queuedProxy
		if err := json.Unmarshal([]byte(current), &merged); err != nil {
			return false, err
		}
		decoded, err := merged.toDomainProxy()
		if err != nil {
			return false, err
		}
		if string(decoded.Hash) != newMember {
			return false, errors.New("migrated proxy hash does not match canonical member")
		}
		*proxy = decoded
	}
	// The script returns the exact payload written with the acquired lease.
	// A later import must not become this worker's pre-check snapshot.
	popResult.Member, popResult.ProxyJSON, popResult.QueueKey = newMember, current, queueKey
	return true, nil
}

func dequeueWaitDuration(nextReadyMs int64, currentMs int64) time.Duration {
	if nextReadyMs <= 0 {
		return idleQueueSleep
	}

	waitMs := nextReadyMs - currentMs
	if waitMs <= 0 {
		return minDequeueSleep
	}

	wait := time.Duration(waitMs) * time.Millisecond
	if wait < minDequeueSleep {
		return minDequeueSleep
	}
	if wait > maxDequeueSleep {
		return maxDequeueSleep
	}
	return wait
}

func waitForNextProxyPoll(ctx context.Context, duration time.Duration) error {
	if duration <= 0 {
		duration = minDequeueSleep
	}

	timer := time.NewTimer(duration)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func coerceLuaString(value interface{}) (string, error) {
	switch v := value.(type) {
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	default:
		return "", fmt.Errorf("expected string/[]byte, got %T", value)
	}
}

func coerceLuaInt64(value interface{}) (int64, error) {
	switch v := value.(type) {
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case uint64:
		if v > math.MaxInt64 {
			return 0, fmt.Errorf("uint64 value out of range: %d", v)
		}
		return int64(v), nil
	case float64:
		if math.Trunc(v) != v {
			return 0, fmt.Errorf("non-integer float64 value %f", v)
		}
		return int64(v), nil
	case string:
		parsed, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return 0, err
		}
		return parsed, nil
	case []byte:
		parsed, err := strconv.ParseInt(string(v), 10, 64)
		if err != nil {
			return 0, err
		}
		return parsed, nil
	default:
		return 0, fmt.Errorf("expected numeric lua response, got %T", value)
	}
}

func (rpq *RedisProxyQueue) RequeueProxy(proxy domain.Proxy, lastCheckTime time.Time) error {
	return rpq.requeueProxy(proxy, lastCheckTime, false)
}

// RequeueProxyWithPayload persists changed ownership or credentials before
// rescheduling. The normal checker path should use RequeueProxy so each check
// only updates sorted-set scheduling state.
func (rpq *RedisProxyQueue) RequeueProxyWithPayload(proxy domain.Proxy, lastCheckTime time.Time) error {
	return rpq.requeueProxy(proxy, lastCheckTime, true)
}

func (rpq *RedisProxyQueue) requeueProxy(proxy domain.Proxy, lastCheckTime time.Time, persistPayload bool) error {
	client, err := rpq.clientOrErr()
	if err != nil {
		return err
	}
	ctx := rpq.baseContext()

	interval := rpq.getEffectiveCheckInterval()
	base := lastCheckTime
	// Clamp to now so overdue proxies don't keep hogging the queue.
	if now := time.Now(); now.After(base) {
		base = now
	}
	nextCheck := base.Add(interval)
	hashKey := string(proxy.Hash)
	queueKey := rpq.queueKeyForMember(hashKey)

	payload, snapshot := "", ""
	expected := int64(0)
	oldQueue := queueKey
	if proxy.QueueLease != nil {
		expected = proxy.QueueLease.ScoreMS
		oldQueue = proxy.QueueLease.QueueKey
		if persistPayload {
			snapshot = proxy.QueueLease.Payload
		}
	}
	if persistPayload {
		raw, err := marshalQueuedProxy(proxy)
		if err != nil {
			return fmt.Errorf("failed to marshal proxy: %w", err)
		}
		payload = string(raw)
	}
	completed, err := completeScript.Run(ctx, client,
		[]string{proxyQueueHeadKey, queueKey, legacyQueueKey, proxyKeyPrefix + hashKey, oldQueue},
		hashKey, expected, nextCheck.UnixMilli(), payload, snapshot).Int64()
	if err != nil {
		return err
	}
	if completed == 0 {
		return ErrProxyLeaseLost
	}
	return nil
}

// RenewLeaseIfNeeded is called between outbound attempts. Supported requests
// last at most 65.535 seconds, so a two-minute margin covers the next request.
// Short checks issue no renewal commands.
func (rpq *RedisProxyQueue) RenewLeaseIfNeeded(ctx context.Context, proxy domain.Proxy) error {
	lease := proxy.QueueLease
	if lease == nil {
		return nil
	}
	if ctx == nil {
		ctx = rpq.baseContext()
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	now := time.Now().UnixMilli()
	if lease.ScoreMS-now > int64(2*time.Minute/time.Millisecond) {
		return nil
	}
	client, err := rpq.clientOrErr()
	if err != nil {
		return err
	}
	renewed, err := renewScript.Run(ctx, client, []string{lease.QueueKey},
		string(proxy.Hash), lease.ScoreMS, now, now+int64(processingLease/time.Millisecond)).Int64()
	if err != nil {
		return err
	}
	if renewed == 0 {
		return ErrProxyLeaseLost
	}
	lease.ScoreMS = renewed
	return nil
}

func (rpq *RedisProxyQueue) getEffectiveCheckInterval() time.Duration {
	fallback := config.GetTimeBetweenChecks()
	client, err := rpq.clientOrErr()
	if err != nil {
		return fallback
	}
	ctx := rpq.baseContext()

	raw, err := client.Get(ctx, queueRescheduleStateKey).Result()
	if err != nil {
		return fallback
	}
	return parseIntervalStateMillis(raw, fallback)
}

func parseIntervalStateMillis(raw string, fallback time.Duration) time.Duration {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return fallback
	}

	ms, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || ms <= 0 {
		return fallback
	}
	return time.Duration(ms) * time.Millisecond
}

func marshalQueuedProxy(proxy domain.Proxy) ([]byte, error) {
	queued, err := newQueuedProxy(proxy)
	if err != nil {
		return nil, err
	}
	return json.Marshal(queued)
}

func newQueuedProxy(proxy domain.Proxy) (queuedProxy, error) {
	queued := queuedProxy{
		Version:      queuedProxyVersion,
		ID:           proxy.ID,
		IP:           proxy.GetIp(),
		Port:         proxy.Port,
		Hash:         append([]byte(nil), proxy.Hash...),
		WorkspaceIDs: collectQueuedWorkspaceIDs(proxy.Workspaces),
	}

	if !encryptProxyQueueCredentials() {
		queued.Username = proxy.Username
		queued.Password = proxy.Password
		return queued, nil
	}

	username, err := security.EncryptProxySecret(proxy.Username)
	if err != nil {
		return queuedProxy{}, err
	}
	password, err := security.EncryptProxySecret(proxy.Password)
	if err != nil {
		return queuedProxy{}, err
	}
	queued.UsernameEncrypted = username
	queued.PasswordEncrypted = password
	return queued, nil
}

func encryptProxyQueueCredentials() bool {
	return support.GetEnvBool(envEncryptQueueCredentials, false)
}

func (qp queuedProxy) needsRewrite() bool {
	if qp.Version < queuedProxyVersion || len(qp.Hash) == 0 {
		return true
	}

	hasEncryptedCredentials := qp.UsernameEncrypted != "" || qp.PasswordEncrypted != ""
	hasPlainCredentials := qp.Username != "" || qp.Password != ""
	if encryptProxyQueueCredentials() {
		return hasPlainCredentials
	}
	return hasEncryptedCredentials
}

func (qp queuedProxy) toDomainProxy() (domain.Proxy, error) {
	workspaceIDs := qp.WorkspaceIDs
	if len(workspaceIDs) == 0 {
		workspaceIDs = qp.UserIDs
	}
	if len(workspaceIDs) == 0 && len(qp.Users) > 0 {
		workspaceIDs = make([]uint, 0, len(qp.Users))
		for _, user := range qp.Users {
			if user.ID == 0 {
				continue
			}
			workspaceIDs = append(workspaceIDs, user.ID)
		}
	}

	workspaces := make([]domain.Workspace, 0, len(workspaceIDs))
	seen := make(map[uint]struct{}, len(workspaceIDs))
	for _, workspaceID := range workspaceIDs {
		if workspaceID == 0 {
			continue
		}
		if _, ok := seen[workspaceID]; ok {
			continue
		}
		seen[workspaceID] = struct{}{}
		workspaces = append(workspaces, domain.Workspace{ID: workspaceID})
	}

	username := qp.Username
	if qp.UsernameEncrypted != "" {
		plain, _, err := security.DecryptProxySecret(qp.UsernameEncrypted)
		if err != nil {
			return domain.Proxy{}, err
		}
		username = plain
	}
	password := qp.Password
	if qp.PasswordEncrypted != "" {
		plain, _, err := security.DecryptProxySecret(qp.PasswordEncrypted)
		if err != nil {
			return domain.Proxy{}, err
		}
		password = plain
	}

	proxy := domain.Proxy{
		ID:         qp.ID,
		IP:         qp.IP,
		Port:       qp.Port,
		Username:   username,
		Password:   password,
		Workspaces: workspaces,
	}
	if qp.Version >= queuedProxyVersion && len(qp.Hash) > 0 {
		proxy.Hash = append([]byte(nil), qp.Hash...)
	} else {
		if err := proxy.GenerateHash(); err != nil {
			return domain.Proxy{}, err
		}
	}
	return proxy, nil
}

func collectQueuedWorkspaceIDs(users []domain.Workspace) []uint {
	if len(users) == 0 {
		return nil
	}

	out := make([]uint, 0, len(users))
	seen := make(map[uint]struct{}, len(users))
	for _, user := range users {
		if user.ID == 0 {
			continue
		}
		if _, ok := seen[user.ID]; ok {
			continue
		}
		seen[user.ID] = struct{}{}
		out = append(out, user.ID)
	}

	if len(out) == 0 {
		return nil
	}

	return out
}

func (rpq *RedisProxyQueue) GetProxyCount() (int64, error) {
	client, err := rpq.clientOrErr()
	if err != nil {
		return 0, err
	}
	ctx := rpq.baseContext()

	var total int64
	for _, key := range rpq.popKeys() {
		count, err := client.ZCard(ctx, key).Result()
		if err != nil {
			return 0, err
		}
		total += count
	}
	return total, nil
}

func (rpq *RedisProxyQueue) GetActiveInstances() (int, error) {
	client, err := rpq.clientOrErr()
	if err != nil {
		return 0, err
	}
	return runtime.CountActiveInstances(rpq.baseContext(), client)
}

func (rpq *RedisProxyQueue) RequeueAll() (int64, error) {
	if rpq == nil {
		return 0, errors.New("redis proxy queue is nil")
	}

	client, err := rpq.clientOrErr()
	if err != nil {
		return 0, err
	}
	ctx := rpq.baseContext()
	interval := rpq.getEffectiveCheckInterval()
	if interval <= 0 {
		interval = time.Second
	}

	const batchSize int64 = 500

	var total int64
	now := time.Now()

	for _, key := range rpq.popKeys() {
		count, err := client.ZCard(ctx, key).Result()
		if err != nil {
			return total, fmt.Errorf("requeue all: count queue %s: %w", key, err)
		}
		if count == 0 {
			continue
		}

		members, err := client.ZRange(ctx, key, 0, count-1).Result()
		if err != nil {
			return total, fmt.Errorf("requeue all: list queue %s: %w", key, err)
		}

		for start := 0; start < len(members); start += int(batchSize) {
			end := start + int(batchSize)
			if end > len(members) {
				end = len(members)
			}

			args := make([]interface{}, 0, 2*(end-start))
			for index, member := range members[start:end] {
				position := int64(start + index)
				offset := (interval * time.Duration(position)) / time.Duration(count)
				args = append(args, member, now.Add(offset).UnixMilli())
			}

			// Compare and update together: a dequeue after ZRANGE must also be
			// protected. Even expired marked leases stay available for recovery.
			updated, err := requeueAllScript.Run(ctx, client, []string{key, proxyQueueHeadKey}, args...).Int64()
			if err != nil {
				return total, fmt.Errorf("requeue all: update queue %s: %w", key, err)
			}
			total += updated
		}
	}

	if err := rpq.refreshQueueHeads(); err != nil {
		return total, fmt.Errorf("requeue all: refresh queue heads: %w", err)
	}

	return total, nil
}

func (rpq *RedisProxyQueue) Close() error {
	return support.CloseRedisClient()
}

func (rpq *RedisProxyQueue) Reschedule(interval time.Duration) error {
	if rpq == nil {
		return errors.New("redis proxy queue is nil")
	}

	client, err := rpq.clientOrErr()
	if err != nil {
		return err
	}
	ctx := rpq.baseContext()

	if interval <= 0 {
		interval = time.Second
	}

	if err := client.Set(ctx, queueRescheduleStateKey, strconv.FormatInt(interval.Milliseconds(), 10), 0).Err(); err != nil {
		return fmt.Errorf("reschedule: failed to persist interval state: %w", err)
	}

	// Existing queue members keep their current due times and converge naturally
	// to the new interval as they are popped and requeued.
	if err := rpq.refreshQueueHeads(); err != nil {
		return fmt.Errorf("reschedule: failed to refresh queue heads: %w", err)
	}

	log.Debug("proxy queue interval updated; existing entries converge lazily", "interval", interval)
	return nil
}
func parseProxyPopResult(result interface{}) (proxyPopResult, error) {
	resSlice, ok := result.([]interface{})
	if !ok {
		return proxyPopResult{}, fmt.Errorf("unexpected lua result type %T", result)
	}
	if len(resSlice) != 5 {
		return proxyPopResult{}, fmt.Errorf("unexpected lua result length %d", len(resSlice))
	}

	foundFlag, err := coerceLuaInt64(resSlice[0])
	if err != nil {
		return proxyPopResult{}, fmt.Errorf("invalid found flag: %w", err)
	}

	if foundFlag == 0 {
		nextReadyMs, err := coerceLuaInt64(resSlice[3])
		if err != nil {
			return proxyPopResult{}, fmt.Errorf("invalid next-ready score: %w", err)
		}
		return proxyPopResult{
			Found:       false,
			NextReadyMs: nextReadyMs,
		}, nil
	}

	proxyJSON, err := coerceLuaString(resSlice[2])
	if err != nil {
		return proxyPopResult{}, fmt.Errorf("invalid proxy payload: %w", err)
	}
	member, err := coerceLuaString(resSlice[1])
	if err != nil {
		return proxyPopResult{}, fmt.Errorf("invalid proxy member: %w", err)
	}
	queueKey, err := coerceLuaString(resSlice[4])
	if err != nil {
		return proxyPopResult{}, fmt.Errorf("invalid proxy queue key: %w", err)
	}

	score, err := coerceLuaInt64(resSlice[3])
	if err != nil {
		return proxyPopResult{}, fmt.Errorf("invalid score: %w", err)
	}

	return proxyPopResult{
		Found:       true,
		Member:      member,
		ProxyJSON:   proxyJSON,
		ScoreMs:     score,
		NextReadyMs: -1,
		QueueKey:    queueKey,
	}, nil
}
