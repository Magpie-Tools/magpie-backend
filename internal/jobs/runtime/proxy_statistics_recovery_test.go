package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"magpie/internal/database"
	"magpie/internal/domain"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type statisticsAckHook struct {
	done chan struct{}
	once sync.Once
}

func (h *statisticsAckHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *statisticsAckHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (h *statisticsAckHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		err := next(ctx, cmd)
		if err == nil && cmd.Name() == "xdel" {
			h.once.Do(func() { close(h.done) })
		}
		return err
	}
}

func TestStatisticsPendingRecoveryFlushesPartialBatchOnce(t *testing.T) {
	for _, count := range []int{1, 37} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			db, client, cfg := statisticsRecoveryFixture(t)
			hook := &statisticsAckHook{done: make(chan struct{})}
			client.AddHook(hook)
			seedPendingStatistics(t, client, cfg, count)
			ctx := context.Background()
			workerCtx, cancel := context.WithCancel(ctx)
			done := make(chan struct{})
			go func() { defer close(done); runProxyStatisticsStreamWorker(workerCtx, "worker") }()
			defer func() {
				cancel()
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Error("statistics worker did not stop")
				}
			}()
			select {
			case <-hook.done:
			case <-time.After(5 * time.Second):
				t.Fatal("partial pending batch did not flush promptly")
			}
			var rows int64
			if err := db.Model(&domain.ProxyStatistic{}).Count(&rows).Error; err != nil || rows != int64(count) {
				t.Fatalf("pending events persisted %d times, want %d: %v", rows, count, err)
			}
			pending, err := client.XPending(ctx, cfg.streamKey, cfg.groupName).Result()
			if err != nil || pending.Count != 0 {
				t.Fatalf("pending entries not acknowledged: %+v %v", pending, err)
			}
		})
	}
}

func statisticsRecoveryFixture(t *testing.T) (*gorm.DB, *redis.Client, proxyStatisticStreamConfig) {
	t.Helper()
	dbCfg := &gorm.Config{Logger: logger.Default.LogMode(logger.Silent), DisableForeignKeyConstraintWhenMigrating: true}
	var db *gorm.DB
	var err error
	if dsn := os.Getenv("MAGPIE_TEST_POSTGRES_DSN"); dsn != "" {
		admin, openErr := gorm.Open(postgres.Open(dsn), dbCfg)
		if openErr != nil {
			t.Fatal(openErr)
		}
		adminSQL, _ := admin.DB()
		t.Cleanup(func() { _ = adminSQL.Close() })
		schema := fmt.Sprintf("statistics_recovery_%d", time.Now().UnixNano())
		if err := admin.Exec("CREATE SCHEMA " + schema).Error; err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = admin.Exec("DROP SCHEMA " + schema + " CASCADE").Error })
		db, err = gorm.Open(postgres.Open(dsn+" search_path="+schema), dbCfg)
	} else {
		db, err = gorm.Open(sqlite.Open(t.TempDir()+"/recovery.db"), dbCfg)
	}
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	if err := db.AutoMigrate(&domain.ProxyStatistic{}, &domain.ProxyStatisticEvent{}, &domain.ProxyDailyCheck{}, &domain.ProxyLatestStatistic{}, &domain.ProxyOverallStatus{}); err != nil {
		t.Fatal(err)
	}
	if err := db.Exec("INSERT INTO proxies (id, host, port, country, estimated_type) VALUES (1, '192.0.2.1', 8080, '', '')").Error; err != nil {
		t.Fatal(err)
	}
	oldDB := database.DB
	database.DB = db
	t.Cleanup(func() { database.DB = oldDB })

	var opts *redis.Options
	if url := os.Getenv("MAGPIE_TEST_REDIS_URL"); url != "" {
		opts, err = redis.ParseURL(url)
		if err != nil {
			t.Fatal(err)
		}
	} else {
		server := miniredis.RunT(t)
		opts = &redis.Options{Addr: server.Addr()}
	}
	client := redis.NewClient(opts)
	t.Cleanup(func() { _ = client.Close() })
	cfg := proxyStatisticStreamConfig{enabled: true, streamKey: "recovery:" + t.Name(), groupName: "checks"}
	oldClient, oldCfg := proxyStatisticStreamClient, proxyStatisticStreamCfg
	proxyStatisticStreamClient, proxyStatisticStreamCfg = client, cfg
	t.Cleanup(func() { proxyStatisticStreamClient, proxyStatisticStreamCfg = oldClient, oldCfg })
	ctx := context.Background()
	t.Cleanup(func() { _ = client.Del(ctx, cfg.streamKey).Err() })
	return db, client, cfg
}

func seedPendingStatistics(t *testing.T, client *redis.Client, cfg proxyStatisticStreamConfig, count int) {
	t.Helper()
	ctx := context.Background()
	if err := client.XGroupCreateMkStream(ctx, cfg.streamKey, cfg.groupName, "0").Err(); err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(domain.ProxyStatistic{ProxyID: 1, ProtocolID: 1, JudgeID: 1, Alive: true, CreatedAt: time.Now().UTC()})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < count; i++ {
		if err := client.XAdd(ctx, &redis.XAddArgs{Stream: cfg.streamKey, Values: map[string]any{"stat": string(payload)}}).Err(); err != nil {
			t.Fatal(err)
		}
	}
	if err := client.XReadGroup(ctx, &redis.XReadGroupArgs{Group: cfg.groupName, Consumer: "worker", Streams: []string{cfg.streamKey, ">"}, Count: int64(count), Block: -1}).Err(); err != nil {
		t.Fatal(err)
	}
}
