package database

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"gorm.io/gorm"
	"magpie/internal/domain"
)

// Run against an isolated PostgreSQL database. The baseline can overlay the
// previous acceptance helper and set MAGPIE_BENCH_LEDGER_LEGACY_SCHEMA=true.
func BenchmarkProxyStatisticEventLedgerPostgres(b *testing.B) {
	for _, workers := range []int{1, 4} {
		b.Run(fmt.Sprintf("batch5000/workers%d", workers), func(b *testing.B) {
			db := setupProxyIngestionPostgresTest(b)
			if os.Getenv("MAGPIE_BENCH_LEDGER_LEGACY_SCHEMA") == "true" {
				if err := db.Exec(`DROP TABLE proxy_statistic_events`).Error; err != nil {
					b.Fatal(err)
				}
				if err := db.Exec(`CREATE TABLE proxy_statistic_events (stream varchar(255), event_id varchar(64), created_at timestamptz, PRIMARY KEY(stream,event_id))`).Error; err != nil {
					b.Fatal(err)
				}
			}
			var sequence atomic.Int64
			var failure atomic.Pointer[error]
			b.ReportAllocs()
			b.ResetTimer()
			var wg sync.WaitGroup
			for worker := 0; worker < workers; worker++ {
				wg.Go(func() {
					for {
						batch := sequence.Add(1) - 1
						if batch >= int64(b.N) {
							return
						}
						stats := make([]domain.ProxyStatistic, 5000)
						for i := range stats {
							stats[i].EventStream = "benchmark:checks"
							stats[i].EventID = fmt.Sprintf("%d-0", 1700000000000+batch*5000+int64(i))
						}
						if err := db.Transaction(func(tx *gorm.DB) error {
							accepted, err := acceptProxyStatisticEvents(tx, stats)
							if err == nil && len(accepted) != len(stats) {
								err = fmt.Errorf("accepted %d events", len(accepted))
							}
							return err
						}); err != nil {
							failure.Store(&err)
							return
						}
					}
				})
			}
			wg.Wait()
			b.StopTimer()
			if err := failure.Load(); err != nil {
				b.Fatal(*err)
			}
			b.ReportMetric(float64(b.N*5000)/b.Elapsed().Seconds(), "events/s")
		})
	}
}
