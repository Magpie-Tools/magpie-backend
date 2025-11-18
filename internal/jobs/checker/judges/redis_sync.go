package judges

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/charmbracelet/log"
	"github.com/redis/go-redis/v9"

	"magpie/internal/domain"
)

const (
	judgeRedisChannel        = "magpie:judges:updates"
	judgeRedisPublishTimeout = 5 * time.Second

	judgeEventTypeSetUserJudges    = "set_user_judges"
	judgeEventTypeAddJudgesToUsers = "add_judges_to_users"
)

type judgeSyncState struct {
	mu     sync.RWMutex
	client *redis.Client
	ctx    context.Context
	cancel context.CancelFunc
}

var (
	redisSyncState   judgeSyncState
	judgeSyncNodeID  = generateJudgeSyncNodeID()
	judgeSyncBackoff = time.Second
)

type judgeSyncEvent struct {
	Type    string           `json:"type"`
	Origin  string           `json:"origin"`
	UserID  uint             `json:"user_id,omitempty"`
	UserIDs []uint           `json:"user_ids,omitempty"`
	Judges  []judgeSyncJudge `json:"judges,omitempty"`
}

type judgeSyncJudge struct {
	ID    uint   `json:"id"`
	URL   string `json:"url"`
	Regex string `json:"regex"`
}

// EnableRedisSynchronization wires the judges cache to redis so changes are broadcasted across nodes.
func EnableRedisSynchronization(ctx context.Context, client *redis.Client) {
	if client == nil {
		log.Warn("Judge sync disabled: redis client is nil")
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}

	redisSyncState.mu.Lock()
	defer redisSyncState.mu.Unlock()

	if redisSyncState.client != nil {
		return
	}

	syncCtx, cancel := context.WithCancel(ctx)
	redisSyncState.client = client
	redisSyncState.ctx = syncCtx
	redisSyncState.cancel = cancel

	go subscribeToJudgeUpdates(syncCtx, client)
}

func subscribeToJudgeUpdates(ctx context.Context, client *redis.Client) {
	pubsub := client.Subscribe(ctx, judgeRedisChannel)
	defer pubsub.Close()

	for {
		msg, err := pubsub.ReceiveMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, redis.ErrClosed) || ctx.Err() != nil {
				return
			}
			log.Error("Judge sync: subscription error", "error", err)
			time.Sleep(judgeSyncBackoff)
			continue
		}

		var event judgeSyncEvent
		if err := json.Unmarshal([]byte(msg.Payload), &event); err != nil {
			log.Error("Judge sync: invalid payload", "error", err)
			continue
		}

		if event.Origin == judgeSyncNodeID {
			continue
		}

		handleJudgeSyncEvent(event)
	}
}

func handleJudgeSyncEvent(event judgeSyncEvent) {
	switch event.Type {
	case judgeEventTypeSetUserJudges:
		if event.UserID == 0 {
			return
		}
		jwr := buildJudgesFromPayload(event.Judges)
		setUserJudgesLocal(event.UserID, jwr)
	case judgeEventTypeAddJudgesToUsers:
		if len(event.UserIDs) == 0 {
			return
		}
		jwr := buildJudgesFromPayload(event.Judges)
		if len(jwr) == 0 {
			return
		}
		addJudgesToUsersLocal(event.UserIDs, jwr)
	default:
		log.Warn("Judge sync: unknown event type", "type", event.Type)
	}
}

func buildJudgesFromPayload(payload []judgeSyncJudge) []domain.JudgeWithRegex {
	if len(payload) == 0 {
		return nil
	}

	results := make([]domain.JudgeWithRegex, 0, len(payload))
	for _, item := range payload {
		judge := &domain.Judge{
			ID:         item.ID,
			FullString: item.URL,
		}
		if err := judge.SetUp(); err != nil {
			log.Warn("Judge sync: cannot set up judge", "url", item.URL, "error", err)
			continue
		}
		judge.UpdateIp()
		results = append(results, domain.JudgeWithRegex{
			Judge: judge,
			Regex: item.Regex,
		})
	}

	return results
}

func broadcastSetUserJudges(userID uint, judgesWithRegex []domain.JudgeWithRegex) {
	if userID == 0 {
		return
	}

	event := judgeSyncEvent{
		Type:   judgeEventTypeSetUserJudges,
		Origin: judgeSyncNodeID,
		UserID: userID,
		Judges: serializeJudges(judgesWithRegex),
	}
	publishJudgeEvent(event)
}

func broadcastAddJudgesToUsers(userIDs []uint, judgesWithRegex []domain.JudgeWithRegex) {
	if len(userIDs) == 0 {
		return
	}

	serialized := serializeJudges(judgesWithRegex)
	if len(serialized) == 0 {
		return
	}

	event := judgeSyncEvent{
		Type:    judgeEventTypeAddJudgesToUsers,
		Origin:  judgeSyncNodeID,
		UserIDs: userIDs,
		Judges:  serialized,
	}
	publishJudgeEvent(event)
}

func publishJudgeEvent(event judgeSyncEvent) {
	payload, err := json.Marshal(event)
	if err != nil {
		log.Error("Judge sync: failed to serialize event", "type", event.Type, "error", err)
		return
	}

	redisSyncState.mu.RLock()
	client := redisSyncState.client
	baseCtx := redisSyncState.ctx
	redisSyncState.mu.RUnlock()

	if client == nil {
		return
	}

	ctx := baseCtx
	if ctx == nil || ctx.Err() != nil {
		ctx = context.Background()
	}

	opCtx, cancel := context.WithTimeout(ctx, judgeRedisPublishTimeout)
	defer cancel()

	if err := client.Publish(opCtx, judgeRedisChannel, payload).Err(); err != nil {
		log.Error("Judge sync: failed to publish event", "type", event.Type, "error", err)
	}
}

func serializeJudges(judgesWithRegex []domain.JudgeWithRegex) []judgeSyncJudge {
	if len(judgesWithRegex) == 0 {
		return nil
	}

	result := make([]judgeSyncJudge, 0, len(judgesWithRegex))
	for _, jwr := range judgesWithRegex {
		if jwr.Judge == nil {
			continue
		}

		result = append(result, judgeSyncJudge{
			ID:    jwr.Judge.ID,
			URL:   jwr.Judge.FullString,
			Regex: jwr.Regex,
		})
	}
	return result
}

func generateJudgeSyncNodeID() string {
	hostname, _ := os.Hostname()
	return fmt.Sprintf("judges-sync:%s:%d:%d", hostname, os.Getpid(), time.Now().UnixNano())
}
