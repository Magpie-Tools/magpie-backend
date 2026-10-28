package database

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"magpie/internal/domain"
	"magpie/internal/security"

	"gorm.io/gorm"
)

func withProxyInventoryDatabases(t *testing.T, run func(*testing.T, *gorm.DB)) {
	t.Helper()
	for _, dialect := range []string{"sqlite", "postgres"} {
		t.Run(dialect, func(t *testing.T) {
			var db *gorm.DB
			if dialect == "postgres" {
				db = setupProxyIngestionPostgresTest(t)
			} else {
				db = setupRotatingProxyTestDB(t)
				t.Cleanup(security.ResetProxyCipherForTests)
				sqlDB, err := db.DB()
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = sqlDB.Close() })
			}
			run(t, db)
		})
	}
}

func seedProxyInventory(t *testing.T, db *gorm.DB) ([]domain.Workspace, []domain.Proxy) {
	t.Helper()
	owners := make([]domain.Workspace, 8)
	for i := range owners {
		owners[i] = domain.Workspace{Name: fmt.Sprintf("Inventory owner %d", i), Timeout: uint16(100 + i), Retries: uint8(i % 3)}
	}
	if err := db.Create(&owners).Error; err != nil {
		t.Fatal(err)
	}
	proxies := make([]domain.Proxy, 8)
	for i := range proxies {
		proxies[i] = domain.Proxy{ID: uint64((i + 1) * 10), IP: fmt.Sprintf("inventory-%d.test", i), Port: uint16(8000 + i), Username: fmt.Sprintf("CaseUser%d", i), Password: fmt.Sprintf("CaseSecret%d", i), Country: "DE", EstimatedType: "Datacenter"}
	}
	if err := db.Create(&proxies).Error; err != nil {
		t.Fatal(err)
	}
	add := func(index, owner int, state string) {
		row := domain.ManagedProxy{WorkspaceID: owners[owner].ID, ProxyID: proxies[index].ID, State: state, Username: proxies[index].Username, Password: proxies[index].Password}
		if err := db.Create(&row).Error; err != nil {
			t.Fatal(err)
		}
	}
	add(0, 0, domain.ManagedProxyStateActive)
	add(0, 1, domain.ManagedProxyStateActive)
	add(1, 0, domain.ManagedProxyStatePaused)
	add(2, 0, domain.ManagedProxyStateArchived)
	// The fourth physical route is unowned. The fifth has mixed owner states.
	add(4, 0, domain.ManagedProxyStatePaused)
	add(4, 1, domain.ManagedProxyStateActive)
	add(4, 2, domain.ManagedProxyStateArchived)
	for i := range owners {
		add(5, i, domain.ManagedProxyStateActive)
	}
	add(6, 0, domain.ManagedProxyStateActive)
	add(7, 0, domain.ManagedProxyStateActive)
	return owners, proxies
}

func TestProxyInventoryBatchesPreserveRoutesOwnersAndCredentials(t *testing.T) {
	withProxyInventoryDatabases(t, func(t *testing.T, db *gorm.DB) {
		owners, proxies := seedProxyInventory(t, db)
		var physical []domain.Proxy
		if err := db.Order("id").Find(&physical).Error; err != nil {
			t.Fatal(err)
		}
		originals := make(map[uint64]domain.Proxy)
		for i, p := range physical {
			p.Username, p.Password = proxies[i].Username, proxies[i].Password
			originals[p.ID] = p
		}
		var got []domain.Proxy
		var sizes []int
		var retained []domain.Proxy
		err := ForEachProxyBatch(2, func(batch []domain.Proxy) error {
			sizes = append(sizes, len(batch))
			got = append(got, batch...)
			if retained == nil {
				retained = batch
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		var ids []uint64
		for _, p := range got {
			ids = append(ids, p.ID)
		}
		if !reflect.DeepEqual(ids, []uint64{10, 50, 60, 70, 80}) || !reflect.DeepEqual(sizes, []int{2, 2, 1}) {
			t.Fatalf("routes %v in batches %v", ids, sizes)
		}
		if retained[0].ID != 10 || retained[1].ID != 50 {
			t.Fatal("later pages mutated a retained callback batch")
		}
		wantOwners := map[uint64][]uint{10: {owners[0].ID, owners[1].ID}, 50: {owners[1].ID}, 70: {owners[0].ID}, 80: {owners[0].ID}}
		for _, w := range owners {
			wantOwners[60] = append(wantOwners[60], w.ID)
		}
		for _, p := range got {
			want := originals[p.ID]
			if p.Username != want.Username || p.Password != want.Password || !reflect.DeepEqual(p.Hash, want.Hash) || p.GetHost() != want.GetHost() || p.Port != want.Port || p.Country != want.Country || p.EstimatedType != want.EstimatedType || !p.CreatedAt.Equal(want.CreatedAt) {
				t.Fatalf("physical fields or credentials changed for %d", p.ID)
			}
			var hydrated []uint
			for _, w := range p.Workspaces {
				hydrated = append(hydrated, w.ID)
				var want domain.Workspace
				if err := preloadCheckerWorkspaces(db).First(&want, w.ID).Error; err != nil {
					t.Fatal(err)
				}
				if w.Timeout != want.Timeout || w.Retries != want.Retries || w.HTTPProtocol != want.HTTPProtocol || w.HTTPSProtocol != want.HTTPSProtocol || w.FailureAction != want.FailureAction {
					t.Fatalf("checker settings changed for owner %d", w.ID)
				}
			}
			if !reflect.DeepEqual(hydrated, wantOwners[p.ID]) {
				t.Fatalf("owners for %d: %v want %v", p.ID, hydrated, wantOwners[p.ID])
			}
		}
	})
}

func TestProxyInventoryBatchesUseCurrentOwnership(t *testing.T) {
	withProxyInventoryDatabases(t, func(t *testing.T, db *gorm.DB) {
		owners, proxies := seedProxyInventory(t, db)
		var ids []uint64
		err := ForEachProxyBatch(1, func(batch []domain.Proxy) error {
			p := batch[0]
			ids = append(ids, p.ID)
			if p.ID == 10 {
				if err := db.Model(&domain.ManagedProxy{}).Where("workspace_id=? AND proxy_id=?", owners[0].ID, 20).UpdateColumn("state", domain.ManagedProxyStateActive).Error; err != nil {
					return err
				}
				if err := db.Model(&domain.ManagedProxy{}).Where("proxy_id=?", 50).UpdateColumn("state", domain.ManagedProxyStatePaused).Error; err != nil {
					return err
				}
				if err := db.Where("workspace_id=? AND proxy_id=?", owners[0].ID, 70).Delete(&domain.ManagedProxy{}).Error; err != nil {
					return err
				}
				return db.Create(&domain.ManagedProxy{WorkspaceID: owners[7].ID, ProxyID: 70, State: domain.ManagedProxyStateActive, Username: proxies[6].Username, Password: proxies[6].Password}).Error
			}
			if p.ID == 70 && (len(p.Workspaces) != 1 || p.Workspaces[0].ID != owners[7].ID) {
				t.Fatal("transferred route retained its former owner")
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(ids, []uint64{10, 20, 60, 70, 80}) {
			t.Fatalf("current ownership after page boundary: %v", ids)
		}
	})
}

func TestProxyInventoryBatchesPropagateErrors(t *testing.T) {
	withProxyInventoryDatabases(t, func(t *testing.T, db *gorm.DB) {
		seedProxyInventory(t, db)
		if err := ForEachProxyBatch(1, nil); err == nil {
			t.Fatal("nil callback accepted")
		}
		sentinel := errors.New("stop inventory callback")
		calls := 0
		if err := ForEachProxyBatch(1, func([]domain.Proxy) error { calls++; return sentinel }); !errors.Is(err, sentinel) || calls != 1 {
			t.Fatalf("callback error %v, calls %d", err, calls)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		DB = db.WithContext(ctx)
		defer func() { DB = db }()
		calls = 0
		if err := ForEachProxyBatch(1, func([]domain.Proxy) error { calls++; return nil }); !errors.Is(err, context.Canceled) || calls != 0 {
			t.Fatalf("query error %v, calls %d", err, calls)
		}
	})
}

func TestProxyInventoryBatchesDefaultSize(t *testing.T) {
	withProxyInventoryDatabases(t, func(t *testing.T, db *gorm.DB) {
		owner := domain.Workspace{Name: "Default inventory page"}
		if err := db.Create(&owner).Error; err != nil {
			t.Fatal(err)
		}
		proxies := make([]domain.Proxy, proxyIterationBatchSize+1)
		for i := range proxies {
			proxies[i] = domain.Proxy{IP: fmt.Sprintf("page-%d.test", i), Port: 8080}
		}
		if err := db.CreateInBatches(&proxies, 500).Error; err != nil {
			t.Fatal(err)
		}
		accesses := make([]domain.ManagedProxy, len(proxies))
		for i, p := range proxies {
			accesses[i] = domain.ManagedProxy{WorkspaceID: owner.ID, ProxyID: p.ID, State: domain.ManagedProxyStateActive}
		}
		if err := db.CreateInBatches(&accesses, 500).Error; err != nil {
			t.Fatal(err)
		}
		for _, size := range []int{0, -1} {
			var batches []int
			last := uint64(0)
			err := ForEachProxyBatch(size, func(page []domain.Proxy) error {
				batches = append(batches, len(page))
				for _, p := range page {
					if p.ID <= last {
						t.Fatal("keyset repeated or reversed a route")
					}
					last = p.ID
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(batches, []int{proxyIterationBatchSize, 1}) {
				t.Fatalf("default batches %v", batches)
			}
		}
	})
}
