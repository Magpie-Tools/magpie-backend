package database

import (
	"fmt"
	"testing"

	"magpie/internal/domain"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func TestProxyIngestionInsertParameterBudget(t *testing.T) {
	for _, count := range []int{1, 5461, 5462, 8191, 8192, 65536} {
		for _, managed := range []bool{false, true} {
			t.Run(fmt.Sprintf("rows=%d/managed=%t", count, managed), func(t *testing.T) {
				db, err := gorm.Open(postgres.New(postgres.Config{DSN: "host=localhost user=test dbname=test"}), &gorm.Config{
					DryRun: true, DisableAutomaticPing: true, SkipDefaultTransaction: true,
				})
				if err != nil {
					t.Fatal(err)
				}
				db = db.Session(&gorm.Session{SkipHooks: true})
				var rows, model any
				if managed {
					proxies := make([]domain.ManagedProxy, count)
					for i := range proxies {
						proxies[i] = domain.ManagedProxy{WorkspaceID: 1, ProxyID: uint64(i + 1)}
					}
					rows, model = proxies, domain.ManagedProxy{}
				} else {
					proxies := make([]domain.Proxy, count)
					// Explicit IDs consume parameters too, unlike DEFAULT in new route imports.
					for i := range proxies {
						proxies[i] = domain.Proxy{ID: uint64(i + 1), IP: "192.0.2.1", Port: 8080}
					}
					rows, model = proxies, domain.Proxy{}
				}
				batchSize, err := calculateCreateBatchSize(db, model, count)
				if err != nil {
					t.Fatal(err)
				}
				statements, inserted := 0, 0
				if err := db.Callback().Create().After("gorm:create").Register("test:parameter_budget", func(tx *gorm.DB) {
					statements++
					inserted += tx.Statement.ReflectValue.Len()
					if got := len(tx.Statement.Vars); got > maxParamsPerBatch {
						t.Errorf("insert binds %d parameters, maximum %d", got, maxParamsPerBatch)
					}
				}); err != nil {
					t.Fatal(err)
				}
				if err := db.Clauses(clause.OnConflict{DoNothing: true}).CreateInBatches(rows, batchSize).Error; err != nil {
					t.Fatal(err)
				}
				if statements == 0 || inserted != count {
					t.Fatalf("inserted %d rows in %d statements, want %d", inserted, statements, count)
				}
			})
		}
	}
}
