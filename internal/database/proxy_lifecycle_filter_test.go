package database

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"testing"

	"magpie/internal/api/dto"
	"magpie/internal/domain"
)

func TestProxyLifecycleFilters(t *testing.T) {
	db := setupProxyTagTestDB(t)
	if err := db.AutoMigrate(&domain.ProxyLatestStatistic{}, &domain.ProxyStatistic{}, &domain.ProxyReputation{}); err != nil {
		t.Fatal(err)
	}
	user := domain.User{Email: "lifecycle-filters@example.test", Password: "hash", Role: "user"}
	if err := db.Create(&user).Error; err != nil {
		t.Fatal(err)
	}
	createTestWorkspaceForUser(t, db, user)
	ids := map[string][]uint{}
	for _, state := range []string{"active", "paused", "archived"} {
		for _, alive := range []bool{true, false} {
			proxy := domain.Proxy{IP: "192.0.2.1", Port: uint16(8000 + len(ids)*2)}
			if alive {
				proxy.Port++
			}
			if err := db.Create(&proxy).Error; err != nil {
				t.Fatal(err)
			}
			if err := db.Create(&domain.UserProxy{WorkspaceID: user.ID, ProxyID: proxy.ID, State: state}).Error; err != nil {
				t.Fatal(err)
			}
			if err := db.Create(&domain.UserProxyFilterIndex{WorkspaceID: user.ID, ProxyID: proxy.ID, Host: proxy.IP, IPAddress: proxy.IPAddress, Port: proxy.Port, State: state, Alive: alive, Country: "DE", CountryKey: "de"}).Error; err != nil {
				t.Fatal(err)
			}
			ids[state] = append(ids[state], uint(proxy.ID))
		}
	}
	// The same route can have a different lifecycle in another workspace.
	if err := db.Create(&domain.UserProxyFilterIndex{WorkspaceID: user.ID + 100, ProxyID: uint64(ids["active"][0]), State: "paused"}).Error; err != nil {
		t.Fatal(err)
	}

	for _, state := range []string{"active", "paused", "archived"} {
		t.Run(state, func(t *testing.T) {
			for _, search := range []string{"", "192.0.2.1", "DE"} {
				rows, total := GetProxyInfoPageWithFiltersAndOptions(user.ID, 1, 1, search, dto.ProxyListFilters{State: state}, ProxyPageQueryOptions{})
				if total != 2 || len(rows) != 1 || rows[0].State != state {
					t.Fatalf("search %q: rows=%+v total=%d", search, rows, total)
				}
				next, nextTotal := GetProxyInfoPageWithFiltersAndOptions(user.ID, 2, 1, search, dto.ProxyListFilters{State: state}, ProxyPageQueryOptions{})
				if nextTotal != 2 || len(next) != 1 || next[0].State != state || rows[0].Id == next[0].Id {
					t.Fatalf("second page search %q: rows=%+v total=%d", search, next, nextTotal)
				}
			}
			rows, total := GetProxyInfoPageWithFiltersAndOptions(user.ID, 1, 40, "", dto.ProxyListFilters{State: state, Status: "alive", Countries: []string{"de"}}, ProxyPageQueryOptions{})
			if total != 1 || len(rows) != 1 || uint(rows[0].Id) != ids[state][0] {
				t.Fatalf("combined filters: rows=%+v total=%d", rows, total)
			}

			deletedIDs, err := collectProxyIDsForDeletion(user.ID, dto.DeleteSettings{Filter: true, State: state})
			sort.Slice(deletedIDs, func(i, j int) bool { return deletedIDs[i] < deletedIDs[j] })
			if err != nil || !reflect.DeepEqual(deletedIDs, ids[state]) {
				t.Fatalf("delete selection: ids=%v err=%v", deletedIDs, err)
			}
			var exported []uint
			err = StreamProxiesForExport(context.Background(), user.ID, dto.ExportSettings{Filter: true, State: state}, 1, func(batch []domain.Proxy) error {
				for _, proxy := range batch {
					exported = append(exported, uint(proxy.ID))
				}
				return nil
			})
			if err != nil || !reflect.DeepEqual(exported, ids[state]) {
				t.Fatalf("export selection: ids=%v err=%v", exported, err)
			}
		})
	}
	t.Run("paused_and_archived", func(t *testing.T) {
		states := []string{"paused", "archived", "paused"}
		rows, total := GetProxyInfoPageWithFiltersAndOptions(user.ID, 1, 40, "192.0.2.1", dto.ProxyListFilters{States: states}, ProxyPageQueryOptions{})
		if total != 4 || len(rows) != 4 {
			t.Fatalf("rows=%d total=%d, want 4", len(rows), total)
		}
		for _, row := range rows {
			if row.State != "paused" && row.State != "archived" {
				t.Fatalf("unexpected state %q", row.State)
			}
		}
		rows, total = GetProxyInfoPageWithFiltersAndOptions(user.ID, 1, 40, "", dto.ProxyListFilters{States: states, Status: "alive"}, ProxyPageQueryOptions{})
		if total != 2 || len(rows) != 2 {
			t.Fatalf("alive rows=%d total=%d, want 2", len(rows), total)
		}
		want := append(append([]uint{}, ids["paused"]...), ids["archived"]...)
		selected, err := collectProxyIDsForDeletion(user.ID, dto.DeleteSettings{Filter: true, States: states})
		sort.Slice(selected, func(i, j int) bool { return selected[i] < selected[j] })
		if err != nil || !reflect.DeepEqual(selected, want) {
			t.Fatalf("delete ids=%v err=%v", selected, err)
		}
		var exported []uint
		err = StreamProxiesForExport(context.Background(), user.ID, dto.ExportSettings{Filter: true, States: states}, 2, func(batch []domain.Proxy) error {
			for _, proxy := range batch {
				exported = append(exported, uint(proxy.ID))
			}
			return nil
		})
		if err != nil || !reflect.DeepEqual(exported, want) {
			t.Fatalf("export ids=%v err=%v", exported, err)
		}
	})

	for _, state := range []string{"", "all", "invalid"} {
		t.Run(fmt.Sprintf("unfiltered_%s", state), func(t *testing.T) {
			rows, total := GetProxyInfoPageWithFiltersAndOptions(user.ID, 1, 40, "", dto.ProxyListFilters{State: state}, ProxyPageQueryOptions{})
			if total != 6 || len(rows) != 6 {
				t.Fatalf("rows=%d total=%d, want 6", len(rows), total)
			}
		})
	}
}
