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
	"time"

	"magpie/internal/checkpace"
	"magpie/internal/domain"
	"magpie/internal/instanceid"
	queueutil "magpie/internal/jobs/queue"
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
	leaseScoreMarker = 0.5
	// requeueAllMinimumWindow keeps an administrator's requeue-all from turning
	// a large queue into one burst when the fastest workspace is very fast.
	requeueAllMinimumWindow = 15 * time.Minute
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
	// pace supplies the allowance snapshot that decides when an added route
	// is first due. A nil Pacer has no snapshot, so every route uses
	// checkpace.FallbackInterval, as before the first allowance load.
	// Requeues take their due time from the checker.
	pace *checkpace.Pacer
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

func init() {
	PublicProxyQueue = *NewRedisProxyQueue(nil, nil)
}

// NewRedisProxyQueue returns a queue on client that schedules routes with
// pace's allowance snapshot. pace may be nil for tools that pace nothing.
func NewRedisProxyQueue(client *redis.Client, pace *checkpace.Pacer) *RedisProxyQueue {
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
		pace:           pace,
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

// AddToQueue publishes payloads and schedules routes with active workspaces.
// Callers supply the complete active workspace list, not just the importing
// workspace. Imports may replace payloads while a checker is running; the Lua
// script preserves its lease. An error can leave earlier pipeline batches saved.
//
// A route is first due at the earliest cadence point any of its owners
// reaches, which spreads an import over its workspace's cadence. A route that
// is already queued keeps an earlier due time and is pulled forward to a
// sooner one, so a new owner with a faster cadence does not wait for the
// cadence of the owners that queued it.
func (rpq *RedisProxyQueue) AddToQueue(proxies []domain.Proxy) error {
	return rpq.addToQueue(proxies, false)
}

// AddToQueueNow publishes payloads like AddToQueue and makes the routes due at
// once, ahead of their owners' cadences. The next dequeue checks each route
// for every owner that has allowance left, and charges each of them. It serves
// an explicit request to check a route again; imports and lifecycle changes
// use AddToQueue, which never spends allowance early.
//
// Unlike AddToQueue it also takes a route from a worker that holds it, in
// whichever shard the lease is: the worker's completion finds its lease gone
// and does not reschedule the route, which stays due at once. Otherwise the
// request would be lost whenever it arrives during a check.
func (rpq *RedisProxyQueue) AddToQueueNow(proxies []domain.Proxy) error {
	return rpq.addToQueue(proxies, true)
}

// dueForEveryOwner is a due time before every cadence point an owner could
// still be waiting for: a route queued at it is checked for all its owners.
const dueForEveryOwner = int64(1)

func (rpq *RedisProxyQueue) addToQueue(proxies []domain.Proxy, dueNow bool) error {
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
	// New owners or routes change workspace check budgets; refresh soon.
	rpq.pace.RequestRefresh()
	pace := rpq.pace.Current()
	now := time.Now()
	batchSize := 500 // Adjust based on your Redis server capabilities

	for i, proxy := range queueable {
		// An explicit requeue also takes the route from a worker that holds it.
		due, take := dueForEveryOwner, "1"
		if !dueNow {
			due, take = rpq.pace.FirstRouteCheck(pace, proxy.ID, proxy.Workspaces, now).UnixMilli(), ""
		}
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
		pipe.Eval(ctx, luaAddScript, keys, hashKey, due, proxyJSON, take)

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

// RemoveFromQueue removes runtime payloads and scheduling state, not database
// routes. Worker removals compare both the lease and its payload snapshot so an
// intervening import survives. Administrative callers without a lease remove
// unconditionally. ErrProxyLeaseLost tells a worker to abandon its stale claim.
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

// GetNextProxyContext waits for a due route, atomically claims its lease and
// returns its previous scheduled time. The returned QueueLease is mutable and
// belongs to one worker; renewal and completion must not run concurrently on it.
// An error after claiming leaves the lease to expire so the route is recoverable.
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

// RequeueProxy completes the worker's lease and makes the route due again at
// nextCheck, using only sorted-set updates. It neither encodes nor rewrites
// the stored payload. The caller decides nextCheck from its owners' cadences
// (checkpace.RouteCycle.Next); a time in the past makes the route due at once.
// ErrProxyLeaseLost means a newer claim or removal fenced this worker out.
func (rpq *RedisProxyQueue) RequeueProxy(proxy domain.Proxy, nextCheck time.Time) error {
	return rpq.requeueProxy(proxy, nextCheck, false)
}

// RequeueProxyWithPayload persists changed ownership or credentials before
// rescheduling. The normal checker path should use RequeueProxy so each check
// only updates sorted-set scheduling state.
// If an import replaced the leased snapshot, completion merges the worker's
// ownership delta into that payload, preserving newly published workspaces and
// credentials. A stale lease cannot complete either form of requeue.
func (rpq *RedisProxyQueue) RequeueProxyWithPayload(proxy domain.Proxy, nextCheck time.Time) error {
	return rpq.requeueProxy(proxy, nextCheck, true)
}

func (rpq *RedisProxyQueue) requeueProxy(proxy domain.Proxy, nextCheck time.Time, persistPayload bool) error {
	client, err := rpq.clientOrErr()
	if err != nil {
		return err
	}
	ctx := rpq.baseContext()

	// Requeueing issues no command beyond the completion script.
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

// toDomainProxy retains readers for legacy owner lists and encrypted credentials.
// Current plaintext payloads reuse their stored hash without invoking the cipher.
// Fingerprint regeneration is reserved for legacy payloads lacking a current hash.
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
	return instanceid.CountActiveInstances(rpq.baseContext(), client)
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
	interval := requeueAllWindow(rpq.pace.Current())

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

// requeueAllWindow spreads every route over the fastest workspace cadence, so
// no route waits longer than it would have, bounded to avoid a burst. A route
// dequeued at such a time is checked only for owners that have reached a
// cadence point; otherwise the checker puts it back at its owners' next
// points. Requeueing everything therefore realigns the queue with the current
// cadences without spending any workspace's allowance early.
func requeueAllWindow(pace *checkpace.Snapshot) time.Duration {
	if pace == nil || pace.FastestCheckInterval <= 0 {
		return checkpace.FallbackInterval
	}
	return min(max(pace.FastestCheckInterval, requeueAllMinimumWindow), checkpace.MaxRouteInterval)
}

func (rpq *RedisProxyQueue) Close() error {
	return support.CloseRedisClient()
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
