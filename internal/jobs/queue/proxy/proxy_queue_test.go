package proxyqueue

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"magpie/internal/domain"
	"magpie/internal/security"
	"magpie/internal/support"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestQueueKeyForMember_DeterministicShard(t *testing.T) {
	q := &RedisProxyQueue{
		queueShardKeys: buildQueueShardKeys(8),
	}

	member := "member-a"
	key1 := q.queueKeyForMember(member)
	key2 := q.queueKeyForMember(member)
	if key1 != key2 {
		t.Fatalf("expected deterministic shard key, got %q and %q", key1, key2)
	}
	if key1 == legacyQueueKey {
		t.Fatalf("expected sharded key, got legacy queue key %q", key1)
	}
}

func TestDequeueWaitDuration(t *testing.T) {
	nowMs := int64(10_000)

	if got := dequeueWaitDuration(-1, nowMs); got != idleQueueSleep {
		t.Fatalf("expected idle sleep %s, got %s", idleQueueSleep, got)
	}

	if got := dequeueWaitDuration(nowMs, nowMs); got != minDequeueSleep {
		t.Fatalf("expected min sleep %s, got %s", minDequeueSleep, got)
	}

	if got := dequeueWaitDuration(nowMs+10_000, nowMs); got != maxDequeueSleep {
		t.Fatalf("expected max sleep %s, got %s", maxDequeueSleep, got)
	}
}

func TestParseProxyPopResult(t *testing.T) {
	found, err := parseProxyPopResult([]interface{}{int64(1), "member", "payload", int64(1234), "proxy_queue:2"})
	if err != nil {
		t.Fatalf("unexpected parse error for found result: %v", err)
	}
	if !found.Found || found.Member != "member" || found.ProxyJSON != "payload" || found.ScoreMs != 1234 || found.QueueKey != "proxy_queue:2" {
		t.Fatalf("unexpected found parse result: %#v", found)
	}

	empty, err := parseProxyPopResult([]interface{}{int64(0), "", "", int64(5555), int64(-1)})
	if err != nil {
		t.Fatalf("unexpected parse error for empty result: %v", err)
	}
	if empty.Found || empty.NextReadyMs != 5555 {
		t.Fatalf("unexpected empty parse result: %#v", empty)
	}
}

func TestMigrateDequeuedProxyMember_RekeysAndWritesCurrentPayload(t *testing.T) {
	configureProxyQueueEncryption(t)

	redisServer, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run failed: %v", err)
	}
	defer redisServer.Close()

	client := redis.NewClient(&redis.Options{Addr: redisServer.Addr()})
	defer client.Close()

	queue := NewRedisProxyQueue(client)
	ctx := context.Background()
	oldMember := "legacy-route-hash"
	oldPayload := `{"ID":12,"IP":"192.0.2.10","Port":8080,"Username":"CaseUser","Password":"CaseSecret","Hash":"bGVnYWN5LXJvdXRlLWhhc2g=","UserIDs":[5]}`
	if err := client.Set(ctx, proxyKeyPrefix+oldMember, oldPayload, 0).Err(); err != nil {
		t.Fatalf("seed legacy payload: %v", err)
	}
	if err := client.ZAdd(ctx, legacyQueueKey, redis.Z{Score: 1000, Member: oldMember}).Err(); err != nil {
		t.Fatalf("seed legacy queue member: %v", err)
	}
	if err := queue.refreshQueueHeads(); err != nil {
		t.Fatalf("refresh queue heads: %v", err)
	}

	popContext, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	proxy, _, err := queue.GetNextProxyContext(popContext)
	if err != nil {
		t.Fatalf("pop and migrate legacy proxy: %v", err)
	}
	newMember := string(proxy.Hash)
	if proxy.Username != "CaseUser" || proxy.Password != "CaseSecret" {
		t.Fatalf("migrated credentials = %q:%q", proxy.Username, proxy.Password)
	}

	if client.Exists(ctx, proxyKeyPrefix+oldMember).Val() != 0 {
		t.Fatal("legacy proxy payload key still exists")
	}
	if _, err := client.ZScore(ctx, legacyQueueKey, oldMember).Result(); !errors.Is(err, redis.Nil) {
		t.Fatalf("legacy sorted-set member still exists: %v", err)
	}
	newPayload, err := client.Get(ctx, proxyKeyPrefix+newMember).Result()
	if err != nil {
		t.Fatalf("load migrated payload: %v", err)
	}
	if !strings.Contains(newPayload, `"Username":"CaseUser"`) || !strings.Contains(newPayload, `"Password":"CaseSecret"`) {
		t.Fatalf("migrated payload does not contain plaintext credentials: %s", newPayload)
	}
	if _, err := client.ZScore(ctx, queue.queueKeyForMember(newMember), newMember).Result(); err != nil {
		t.Fatalf("new sorted-set member missing: %v", err)
	}
}

func TestNewQueuedProxy_DefaultStoresPlaintextCredentialsUserIDsAndHash(t *testing.T) {
	configureProxyQueueEncryption(t)

	proxy := domain.Proxy{
		ID:       12,
		IP:       "192.0.2.10",
		Port:     8080,
		Username: "u",
		Password: "p",
		Hash:     []byte("hash"),
		Users: []domain.User{
			{ID: 5, Timeout: 1000, Retries: 2},
			{ID: 9, Timeout: 2500, Retries: 5},
			{ID: 5, Timeout: 9999, Retries: 9},
		},
	}

	queued, err := newQueuedProxy(proxy)
	if err != nil {
		t.Fatalf("new queued proxy: %v", err)
	}
	if len(queued.UserIDs) != 2 || queued.UserIDs[0] != 5 || queued.UserIDs[1] != 9 {
		t.Fatalf("unexpected queued user IDs: %#v", queued.UserIDs)
	}
	if len(queued.Users) != 0 {
		t.Fatalf("expected no legacy user payload, got %#v", queued.Users)
	}
	if queued.Version != queuedProxyVersion {
		t.Fatalf("payload version = %d, want %d", queued.Version, queuedProxyVersion)
	}
	if !bytes.Equal(queued.Hash, proxy.Hash) {
		t.Fatalf("queued hash = %q, want %q", queued.Hash, proxy.Hash)
	}

	raw, err := json.Marshal(queued)
	if err != nil {
		t.Fatalf("marshal queued proxy: %v", err)
	}
	payload := string(raw)
	if !strings.Contains(payload, "\"UserIDs\":[5,9]") {
		t.Fatalf("expected compact UserIDs payload, got %s", payload)
	}
	if strings.Contains(payload, "\"Users\"") {
		t.Fatalf("expected Users field to be omitted in new payload, got %s", payload)
	}
	if !strings.Contains(payload, "\"Username\":\"u\"") || !strings.Contains(payload, "\"Password\":\"p\"") {
		t.Fatalf("queued payload does not contain plaintext credentials: %s", payload)
	}
	if queued.UsernameEncrypted != "" || queued.PasswordEncrypted != "" {
		t.Fatal("default queue payload unexpectedly encrypted credentials")
	}
}

func TestNewQueuedProxy_OptionalEncryptionStoresCiphertextOnce(t *testing.T) {
	configureProxyQueueEncryption(t)
	t.Setenv(envEncryptQueueCredentials, "true")

	proxy := domain.Proxy{
		ID:       12,
		IP:       "192.0.2.10",
		Port:     8080,
		Username: "u",
		Password: "p",
		Hash:     []byte("hash"),
	}
	queued, err := newQueuedProxy(proxy)
	if err != nil {
		t.Fatalf("new queued proxy: %v", err)
	}
	if queued.Username != "" || queued.Password != "" {
		t.Fatal("encrypted queue payload retained plaintext credentials")
	}
	if !security.IsProxySecretEncrypted(queued.UsernameEncrypted) || !security.IsProxySecretEncrypted(queued.PasswordEncrypted) {
		t.Fatal("optional queue encryption did not encrypt credentials")
	}
	if !bytes.Equal(queued.Hash, proxy.Hash) {
		t.Fatal("optional encryption payload did not retain the route hash")
	}
}

func TestQueuedProxyToDomainProxy_CurrentPlaintextPayloadReusesHashWithoutEncryptionKey(t *testing.T) {
	t.Setenv(envEncryptQueueCredentials, "false")
	t.Setenv("PROXY_ENCRYPTION_KEY", "")
	security.ResetProxyCipherForTests()
	t.Cleanup(security.ResetProxyCipherForTests)

	payload := queuedProxy{
		Version:  queuedProxyVersion,
		ID:       7,
		IP:       "198.51.100.7",
		Port:     8080,
		Username: "user",
		Password: "pass",
		Hash:     []byte("already-calculated-route-hash"),
		UserIDs:  []uint{4},
	}
	proxy, err := payload.toDomainProxy()
	if err != nil {
		t.Fatalf("decode current plaintext payload: %v", err)
	}
	if !bytes.Equal(proxy.Hash, payload.Hash) {
		t.Fatalf("decoded hash = %q, want stored hash %q", proxy.Hash, payload.Hash)
	}
	if proxy.Username != "user" || proxy.Password != "pass" {
		t.Fatalf("decoded credentials = %q:%q", proxy.Username, proxy.Password)
	}
	if payload.needsRewrite() {
		t.Fatal("current plaintext payload unexpectedly requires a rewrite")
	}
}

func TestGetNextProxy_RewritesEncryptedV1PayloadOnceWithoutRekeying(t *testing.T) {
	configureProxyQueueEncryption(t)

	redisServer, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run failed: %v", err)
	}
	defer redisServer.Close()

	client := redis.NewClient(&redis.Options{Addr: redisServer.Addr()})
	defer client.Close()

	proxy := domain.Proxy{
		ID:       27,
		IP:       "203.0.113.27",
		Port:     1080,
		Username: "legacy-user",
		Password: "legacy-pass",
		Users:    []domain.User{{ID: 3}},
	}
	if err := proxy.GenerateHash(); err != nil {
		t.Fatalf("generate route hash: %v", err)
	}
	usernameEncrypted, err := security.EncryptProxySecret(proxy.Username)
	if err != nil {
		t.Fatalf("encrypt username: %v", err)
	}
	passwordEncrypted, err := security.EncryptProxySecret(proxy.Password)
	if err != nil {
		t.Fatalf("encrypt password: %v", err)
	}

	legacyPayload, err := json.Marshal(queuedProxy{
		Version:           1,
		ID:                proxy.ID,
		IP:                proxy.IP,
		Port:              proxy.Port,
		UsernameEncrypted: usernameEncrypted,
		PasswordEncrypted: passwordEncrypted,
		UserIDs:           []uint{3},
	})
	if err != nil {
		t.Fatalf("marshal encrypted v1 payload: %v", err)
	}

	queue := NewRedisProxyQueue(client)
	ctx := context.Background()
	member := string(proxy.Hash)
	if err := client.Set(ctx, proxyKeyPrefix+member, legacyPayload, 0).Err(); err != nil {
		t.Fatalf("seed encrypted payload: %v", err)
	}
	queueKey := queue.queueKeyForMember(member)
	if err := client.ZAdd(ctx, queueKey, redis.Z{Score: 1000, Member: member}).Err(); err != nil {
		t.Fatalf("seed queue member: %v", err)
	}
	if err := queue.refreshQueueHeads(); err != nil {
		t.Fatalf("refresh queue heads: %v", err)
	}

	popContext, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	got, _, err := queue.GetNextProxyContext(popContext)
	if err != nil {
		t.Fatalf("pop encrypted v1 payload: %v", err)
	}
	if got.Username != proxy.Username || got.Password != proxy.Password {
		t.Fatalf("decoded credentials = %q:%q", got.Username, got.Password)
	}
	if !bytes.Equal(got.Hash, proxy.Hash) {
		t.Fatalf("decoded hash = %q, want %q", got.Hash, proxy.Hash)
	}

	rewrittenJSON, err := client.Get(ctx, proxyKeyPrefix+member).Bytes()
	if err != nil {
		t.Fatalf("load rewritten payload: %v", err)
	}
	var rewritten queuedProxy
	if err := json.Unmarshal(rewrittenJSON, &rewritten); err != nil {
		t.Fatalf("decode rewritten payload: %v", err)
	}
	if rewritten.Version != queuedProxyVersion {
		t.Fatalf("rewritten version = %d, want %d", rewritten.Version, queuedProxyVersion)
	}
	if rewritten.Username != proxy.Username || rewritten.Password != proxy.Password {
		t.Fatalf("rewritten plaintext credentials = %q:%q", rewritten.Username, rewritten.Password)
	}
	if rewritten.UsernameEncrypted != "" || rewritten.PasswordEncrypted != "" {
		t.Fatal("rewritten default payload retained encrypted credentials")
	}
	if !bytes.Equal(rewritten.Hash, proxy.Hash) {
		t.Fatalf("rewritten hash = %q, want %q", rewritten.Hash, proxy.Hash)
	}
}

func TestRequeueProxy_OnlyPersistsPayloadWhenRequested(t *testing.T) {
	configureProxyQueueEncryption(t)

	redisServer, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run failed: %v", err)
	}
	defer redisServer.Close()

	client := redis.NewClient(&redis.Options{Addr: redisServer.Addr()})
	defer client.Close()

	queue := NewRedisProxyQueue(client)
	ctx := context.Background()
	proxy := domain.Proxy{
		ID:       42,
		IP:       "198.51.100.42",
		Port:     8080,
		Username: "queue-user",
		Password: "queue-pass",
		Hash:     []byte("precomputed-route-hash"),
		Users:    []domain.User{{ID: 7}},
	}
	proxyKey := proxyKeyPrefix + string(proxy.Hash)
	const originalPayload = `{"sentinel":"must remain unchanged"}`
	if err := client.Set(ctx, proxyKey, originalPayload, 0).Err(); err != nil {
		t.Fatalf("seed proxy payload: %v", err)
	}
	if err := client.Set(ctx, queueRescheduleStateKey, "1000", 0).Err(); err != nil {
		t.Fatalf("seed queue interval: %v", err)
	}

	if err := queue.RequeueProxy(proxy, time.Now()); err != nil {
		t.Fatalf("schedule-only requeue: %v", err)
	}
	unchanged, err := client.Get(ctx, proxyKey).Result()
	if err != nil {
		t.Fatalf("load schedule-only payload: %v", err)
	}
	if unchanged != originalPayload {
		t.Fatalf("schedule-only requeue changed payload to %s", unchanged)
	}
	if _, err := client.ZScore(ctx, queue.queueKeyForMember(string(proxy.Hash)), string(proxy.Hash)).Result(); err != nil {
		t.Fatalf("schedule-only requeue did not update sorted set: %v", err)
	}

	proxy.Users = append(proxy.Users, domain.User{ID: 9})
	if err := queue.RequeueProxyWithPayload(proxy, time.Now()); err != nil {
		t.Fatalf("payload-persisting requeue: %v", err)
	}
	rewrittenJSON, err := client.Get(ctx, proxyKey).Bytes()
	if err != nil {
		t.Fatalf("load persisted payload: %v", err)
	}
	var rewritten queuedProxy
	if err := json.Unmarshal(rewrittenJSON, &rewritten); err != nil {
		t.Fatalf("decode persisted payload: %v", err)
	}
	if rewritten.Version != queuedProxyVersion || len(rewritten.UserIDs) != 2 {
		t.Fatalf("unexpected persisted payload: %#v", rewritten)
	}
}

func TestQueuedProxyToDomainProxy_HandlesLegacyUsersAndUserIDs(t *testing.T) {
	configureProxyQueueEncryption(t)

	fromUserIDs := queuedProxy{
		ID:      1,
		IP:      "198.51.100.2",
		Port:    9000,
		Hash:    []byte("h"),
		UserIDs: []uint{8, 4, 8},
	}
	got, err := fromUserIDs.toDomainProxy()
	if err != nil {
		t.Fatalf("decode UserIDs payload: %v", err)
	}
	if len(got.Users) != 2 || got.Users[0].ID != 8 || got.Users[1].ID != 4 {
		t.Fatalf("unexpected users from UserIDs payload: %#v", got.Users)
	}
	if got.Users[0].Timeout != 0 || got.Users[1].Retries != 0 {
		t.Fatalf("expected compact payload to not include checker settings, got %#v", got.Users)
	}

	fromLegacy := queuedProxy{
		ID:   2,
		IP:   "203.0.113.5",
		Port: 1080,
		Hash: []byte("legacy"),
		Users: []queuedProxyUser{
			{ID: 11},
			{ID: 7},
		},
	}
	gotLegacy, err := fromLegacy.toDomainProxy()
	if err != nil {
		t.Fatalf("decode legacy payload: %v", err)
	}
	if len(gotLegacy.Users) != 2 || gotLegacy.Users[0].ID != 11 || gotLegacy.Users[1].ID != 7 {
		t.Fatalf("unexpected users from legacy payload: %#v", gotLegacy.Users)
	}
}

func configureProxyQueueEncryption(t *testing.T) {
	t.Helper()
	t.Setenv(envEncryptQueueCredentials, "false")
	t.Setenv("PROXY_ENCRYPTION_KEY", "proxy-queue-test-encryption-key")
	security.ResetProxyCipherForTests()
	t.Cleanup(security.ResetProxyCipherForTests)
}

func TestParseIntervalStateMillis(t *testing.T) {
	fallback := 3 * time.Second
	if got := parseIntervalStateMillis("1500", fallback); got != 1500*time.Millisecond {
		t.Fatalf("parsed interval = %s, want 1500ms", got)
	}
	if got := parseIntervalStateMillis("", fallback); got != fallback {
		t.Fatalf("empty interval should fallback to %s, got %s", fallback, got)
	}
	if got := parseIntervalStateMillis("bad", fallback); got != fallback {
		t.Fatalf("invalid interval should fallback to %s, got %s", fallback, got)
	}
}

func TestApplyIntervalUpdateAsLeaderWithRunner_SkipsWhenNotLeader(t *testing.T) {
	called := false
	err := applyIntervalUpdateAsLeaderWithRunner(
		func(context.Context, string, time.Duration, func(context.Context) error) error {
			return support.ErrLeaderLockNotAcquired
		},
		"lock",
		time.Second,
		func(time.Duration) error {
			called = true
			return nil
		},
	)
	if err != nil {
		t.Fatalf("expected nil on lock-not-acquired, got %v", err)
	}
	if called {
		t.Fatal("reschedule should not run when leadership is not acquired")
	}
}

func TestApplyIntervalUpdateAsLeaderWithRunner_PropagatesRescheduleError(t *testing.T) {
	expected := errors.New("boom")
	err := applyIntervalUpdateAsLeaderWithRunner(
		func(_ context.Context, _ string, _ time.Duration, run func(context.Context) error) error {
			return run(context.Background())
		},
		"lock",
		time.Second,
		func(time.Duration) error {
			return expected
		},
	)
	if !errors.Is(err, expected) {
		t.Fatalf("expected reschedule error %v, got %v", expected, err)
	}
}

func TestRequeueAll_ReschedulesQueuedMembersAcrossInterval(t *testing.T) {
	redisServer, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run failed: %v", err)
	}
	defer redisServer.Close()

	client := redis.NewClient(&redis.Options{Addr: redisServer.Addr()})
	defer client.Close()

	queue := NewRedisProxyQueue(client)
	ctx := context.Background()

	members := []string{"member-a", "member-b", "member-c"}
	now := time.Now()
	for index, member := range members {
		score := float64(now.Add(-time.Duration(index+1) * time.Hour).UnixMilli())
		if err := client.ZAdd(ctx, legacyQueueKey, redis.Z{
			Score:  score,
			Member: member,
		}).Err(); err != nil {
			t.Fatalf("seed queue member %s: %v", member, err)
		}
	}

	if err := client.Set(ctx, queueRescheduleStateKey, "60000", 0).Err(); err != nil {
		t.Fatalf("seed interval state: %v", err)
	}

	before := time.Now().UnixMilli()
	count, err := queue.RequeueAll()
	if err != nil {
		t.Fatalf("RequeueAll failed: %v", err)
	}
	after := time.Now().UnixMilli()

	if count != int64(len(members)) {
		t.Fatalf("requeued count = %d, want %d", count, len(members))
	}

	scoredMembers, err := client.ZRangeWithScores(ctx, legacyQueueKey, 0, -1).Result()
	if err != nil {
		t.Fatalf("read rescheduled queue: %v", err)
	}
	if len(scoredMembers) != len(members) {
		t.Fatalf("rescheduled member count = %d, want %d", len(scoredMembers), len(members))
	}

	if scoredMembers[0].Score < float64(before) || scoredMembers[0].Score > float64(after) {
		t.Fatalf("first member score = %f, expected within [%d, %d]", scoredMembers[0].Score, before, after)
	}
	if scoredMembers[len(scoredMembers)-1].Score > float64(after+60000) {
		t.Fatalf("last member score = %f, expected to stay within the configured interval", scoredMembers[len(scoredMembers)-1].Score)
	}

	headEntries, err := client.ZRangeWithScores(ctx, proxyQueueHeadKey, 0, -1).Result()
	if err != nil {
		t.Fatalf("read queue heads: %v", err)
	}
	if len(headEntries) == 0 {
		t.Fatal("expected queue heads to be refreshed")
	}
	if headEntries[0].Member != legacyQueueKey {
		t.Fatalf("queue head member = %v, want %q", headEntries[0].Member, legacyQueueKey)
	}
}
