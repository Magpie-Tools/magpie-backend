package database

import (
	"context"
	"reflect"
	"testing"
	"time"
)

func TestReadModelProjectionWindowSlowSuccessBackoff(t *testing.T) {
	for _, tc := range []struct {
		name    string
		window  readModelProjectionWindow
		pairs   int
		elapsed time.Duration
		timeout time.Duration
		budget  int
		want    int
	}{
		{"ordinary", readModelProjectionWindow{}, 5000, 15*time.Second + time.Nanosecond, 30 * time.Second, 5000, 2500},
		{"learned", readModelProjectionWindow{limit: 2500, fastCommits: 1}, 2500, 20 * time.Second, 30 * time.Second, 5000, 1250},
		{"half_boundary", readModelProjectionWindow{}, 5000, 15 * time.Second, 30 * time.Second, 5000, 5000},
		{"short_tail", readModelProjectionWindow{limit: 2500}, 2, 20 * time.Second, 30 * time.Second, 5000, 2500},
		{"operator_ceiling", readModelProjectionWindow{limit: 2500}, 5, 20 * time.Second, 30 * time.Second, 5, 2},
		{"one_pair", readModelProjectionWindow{limit: 1}, 1, 20 * time.Second, 30 * time.Second, 5000, 1},
		{"invalid_elapsed", readModelProjectionWindow{}, 5000, 0, 30 * time.Second, 5000, 5000},
		{"invalid_timeout", readModelProjectionWindow{}, 5000, 20 * time.Second, 0, 5000, 5000},
		{"invalid_budget", readModelProjectionWindow{limit: 2500}, 2500, 20 * time.Second, 30 * time.Second, 0, 2500},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := tc.window
			w.committed(tc.pairs, tc.elapsed, tc.timeout, tc.budget)
			if got := w.pairLimit(5000); got != tc.want || w.fastCommits != 0 {
				t.Fatal("unexpected next width or retained fast evidence", got, w, tc.want)
			}
		})
	}
}

func TestReadModelProjectionWindowSlowSuccessKeepsGrowthAndFreshBatch(t *testing.T) {
	w := readModelProjectionWindow{}
	w.committed(5000, 20*time.Second, 30*time.Second, 5000)
	w.committed(2500, time.Second, 30*time.Second, 5000)
	if w.pairLimit(5000) != 2500 {
		t.Fatal("one fast commit restored the width", w)
	}
	w.committed(2500, time.Second, 30*time.Second, 5000)
	if w.pairLimit(5000) != 5000 {
		t.Fatal("two fast commits did not restore the configured ceiling", w)
	}
	fresh := readModelProjectionWindow{}
	if fresh.pairLimit(5000) != 5000 {
		t.Fatal("another batch inherited a learned width", fresh)
	}
}

func TestReadModelProjectionWindowSlowSuccessPreservesCommittedPrefix(t *testing.T) {
	b := &readModelProjectionBatch{pairs: make([]readModelProjectionPair, 10000)}
	end := b.prefixEnd(5000)
	b.commitPrefix(end)
	b.window.committed(end, 20*time.Second, 30*time.Second, 5000)
	if b.next != 5000 || b.retryEnd != 0 || b.retryWidth != 0 || b.prefixEnd(5000) != 7500 {
		t.Fatal("successful backoff lost progress or fabricated failed work", b)
	}
	if b.retainDeadlinePrefix(context.Canceled, context.DeadlineExceeded, context.DeadlineExceeded, 5000, 7500) {
		t.Fatal("backoff changed parent cancellation handling")
	}
}

func TestReadModelProjectionWindowSlowSuccessPostgres(t *testing.T) {
	db, proxyID, _ := seedReadModelFanoutTest(t)
	t.Setenv("READ_MODEL_REFRESH_BATCH_SIZE", "4")
	t.Setenv("READ_MODEL_REFRESH_MAX_BATCHES", "1")
	t.Setenv("READ_MODEL_REFRESH_TIMEOUT_SECONDS", "1")
	before := readModelSubprefixAuthoritativeState(t, db)
	attempts := captureReadModelSubprefixAttempts(t, db)
	if err := db.Exec(`CREATE FUNCTION test_slow_success() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN PERFORM pg_sleep(0.15); RETURN NEW; END $$;
		CREATE TRIGGER test_slow_success BEFORE INSERT ON user_proxy_filter_indexes
		FOR EACH ROW EXECUTE FUNCTION test_slow_success()`).Error; err != nil {
		t.Fatal(err)
	}
	flushReadModelDirtyProxyIDs(context.Background(), false)
	requireReadModelSubprefixState(t, 4, 8, 0, 0)
	if got := readModelSubprefixRowCount(t, db, proxyID); got != 4 {
		t.Fatal("slow successful statement did not commit its complete prefix", got)
	}
	finishReadModelSubprefixDrain(t)
	if got := attempts.snapshot(); !reflect.DeepEqual(got, []int{4, 2, 2}) {
		t.Fatal("slow successful statement repeated its width or lost pairs", got)
	}
	if got := readModelSubprefixRowCount(t, db, proxyID); got != 8 || before != readModelSubprefixAuthoritativeState(t, db) {
		t.Fatal("backoff changed authoritative data or missed an owner", got)
	}
	t.Log("controlled slow-success case: 3 projection statements/table checks and 1 owner lookup; ordinary 4-pair reference: 2 statements/table checks and 1 owner lookup; no throughput claim")
}
