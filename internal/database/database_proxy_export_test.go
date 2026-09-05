package database

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"magpie/internal/api/dto"
	"magpie/internal/domain"
)

func TestProxyExportBatchesAndCancellation(t *testing.T) {
	db := setupProxyTagTestDB(t)
	if err := db.AutoMigrate(&domain.ProxyLatestStatistic{}, &domain.ProxyStatistic{}, &domain.ProxyReputation{}); err != nil {
		t.Fatal(err)
	}
	user := domain.User{Email: "export@example.test", Password: "hash", Role: "user"}
	if err := db.Create(&user).Error; err != nil {
		t.Fatal(err)
	}
	createTestWorkspaceForUser(t, db, user)
	var want []uint64
	for i := 0; i < 5; i++ {
		proxy := domain.Proxy{IP: "192.0.2.1", Port: uint16(8000 + i)}
		if err := db.Create(&proxy).Error; err != nil {
			t.Fatal(err)
		}
		if err := db.Create(&domain.UserProxy{WorkspaceID: user.ID, ProxyID: proxy.ID}).Error; err != nil {
			t.Fatal(err)
		}
		if err := db.Create(&domain.UserProxyFilterIndex{WorkspaceID: user.ID, ProxyID: proxy.ID, Alive: true}).Error; err != nil {
			t.Fatal(err)
		}
		want = append(want, proxy.ID)
	}
	settings := dto.ExportSettings{Filter: true, ProxyStatus: "alive"}
	var got []uint64
	var sizes []int
	err := StreamProxiesForExport(context.Background(), user.ID, settings, 2, func(batch []domain.Proxy) error {
		sizes = append(sizes, len(batch))
		for _, proxy := range batch {
			got = append(got, proxy.ID)
		}
		return nil
	})
	if err != nil || !reflect.DeepEqual(got, want) || !reflect.DeepEqual(sizes, []int{2, 2, 1}) {
		t.Fatalf("ids=%v sizes=%v err=%v", got, sizes, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var calls int
	err = StreamProxiesForExport(ctx, user.ID, settings, 2, func(batch []domain.Proxy) error {
		calls++
		cancel()
		return nil
	})
	if !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatalf("cancellation: calls=%d err=%v", calls, err)
	}
	calls = 0
	err = StreamProxiesForExport(ctx, user.ID, settings, 2, func([]domain.Proxy) error {
		calls++
		return nil
	})
	if !errors.Is(err, context.Canceled) || calls != 0 {
		t.Fatalf("canceled transaction: calls=%d err=%v", calls, err)
	}
}
