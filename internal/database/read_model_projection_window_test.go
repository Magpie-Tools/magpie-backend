package database

import (
	"context"
	"reflect"
	"testing"
	"time"
)

func TestReadModelProjectionWindowKeepsDeadlineLimitAcrossIntervals(t *testing.T) {
	b := &readModelProjectionBatch{pairs: make([]readModelProjectionPair, 40000)}
	if !b.retainDeadlinePrefix(nil, context.DeadlineExceeded, context.DeadlineExceeded, 0, 5000) {
		t.Fatal("actual child deadline did not select a smaller window")
	}
	for b.next < len(b.pairs) {
		start := b.next
		end := b.prefixEnd(5000)
		if end-start != 2500 {
			t.Fatal("batch forgot deadline pressure at interval boundary", start, end)
		}
		b.commitPrefix(end)
		if b.retryEnd == 0 {
			b.window.committed(end-start, 10*time.Second, 30*time.Second, 5000)
		}
	}
	fresh := &readModelProjectionBatch{pairs: make([]readModelProjectionPair, 5000)}
	if fresh.prefixEnd(5000) != 5000 {
		t.Fatal("a completed batch throttled a new owner batch")
	}
}

func TestReadModelProjectionWindowRequiresConsecutiveFastFullCommits(t *testing.T) {
	w := readModelProjectionWindow{}
	w.deadline(5000)
	w.committed(2500, time.Second, 30*time.Second, 5000)
	if w.pairLimit(5000) != 2500 {
		t.Fatal("one fast commit restored the width")
	}
	w.committed(2500, 8*time.Second, 30*time.Second, 5000)
	w.committed(2500, time.Second, 30*time.Second, 5000)
	if w.pairLimit(5000) != 2500 {
		t.Fatal("a slow commit did not break the fast sequence")
	}
	w.failed()
	w.committed(2500, time.Second, 30*time.Second, 5000)
	if w.pairLimit(5000) != 2500 {
		t.Fatal("an error did not break the fast sequence")
	}
	w.committed(1, time.Nanosecond, 30*time.Second, 5000)
	w.committed(2500, time.Second, 30*time.Second, 5000)
	if w.pairLimit(5000) != 2500 {
		t.Fatal("a short tail was counted as a full fast commit")
	}
	w.committed(2500, 7500*time.Millisecond, 30*time.Second, 5000)
	if w.pairLimit(5000) != 5000 {
		t.Fatal("two fast full commits did not restore normal width")
	}
}

func TestReadModelProjectionWindowRecoversFromOnePairWithoutPinningBatch(t *testing.T) {
	w := readModelProjectionWindow{}
	w.deadline(2)
	commits := 0
	for w.pairLimit(5000) < 5000 && commits < 32 {
		pairs := w.pairLimit(5000)
		w.committed(pairs, time.Millisecond, 30*time.Second, 5000)
		commits++
	}
	if w.pairLimit(5000) != 5000 || commits != 26 {
		t.Fatal("one small recovery pinned the remaining batch", w, commits)
	}
}

func TestReadModelProjectionWindowRespectsBudgetAndInvalidTiming(t *testing.T) {
	for _, budget := range []int{1, 2, 5, 5000} {
		w := readModelProjectionWindow{}
		w.deadline(5000)
		if got := w.pairLimit(budget); got > budget || got < 1 {
			t.Fatal("learned window exceeded operator pair ceiling", got, budget)
		}
		for range 2 {
			w.committed(w.pairLimit(budget), time.Millisecond, time.Second, budget)
		}
		if w.pairLimit(budget) > budget {
			t.Fatal("growth exceeded the operator ceiling", w, budget)
		}
	}
	for _, elapsed := range []time.Duration{0, -time.Second, 7500*time.Millisecond + time.Nanosecond} {
		w := readModelProjectionWindow{}
		w.deadline(5000)
		for range 2 {
			w.committed(2500, elapsed, 30*time.Second, 5000)
		}
		if w.pairLimit(5000) != 2500 {
			t.Fatal("invalid or insufficient timing evidence grew width", elapsed)
		}
	}
}

func TestReadModelProjectionWindowRetainsSinglePairFailureAndCursor(t *testing.T) {
	b := &readModelProjectionBatch{pairs: make([]readModelProjectionPair, 4)}
	b.retainDeadlinePrefix(nil, context.DeadlineExceeded, context.DeadlineExceeded, 0, 4)
	b.retainDeadlinePrefix(nil, context.DeadlineExceeded, context.DeadlineExceeded, 0, 2)
	before := *b
	if b.retainDeadlinePrefix(nil, context.DeadlineExceeded, context.DeadlineExceeded, 0, 1) || !reflect.DeepEqual(*b, before) || b.next != 0 {
		t.Fatal("one-pair deadline changed progress or fabricated recovery", b)
	}
}

func TestReadModelProjectionWindowSlowCommitsPostgres(t *testing.T) {
	db, proxyID, owners := seedReadModelFanoutTest(t)
	t.Setenv("READ_MODEL_REFRESH_BATCH_SIZE", "2")
	t.Setenv("READ_MODEL_REFRESH_MAX_BATCHES", "4")
	t.Setenv("READ_MODEL_REFRESH_TIMEOUT_SECONDS", "1")
	attempts := captureReadModelSubprefixAttempts(t, db)
	removeTrigger := installReadModelSubprefixDeadlineTrigger(t, db, owners[1])
	flushReadModelDirtyProxyIDs(context.Background(), false)
	requireReadModelSubprefixState(t, 0, 8, 2, 1)
	if err := removeTrigger(); err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`CREATE FUNCTION test_slow_projection_window() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN PERFORM pg_sleep(0.3); RETURN NEW; END $$;
		CREATE TRIGGER test_slow_projection_window BEFORE INSERT ON user_proxy_filter_indexes
		FOR EACH ROW EXECUTE FUNCTION test_slow_projection_window()`).Error; err != nil {
		t.Fatal(err)
	}
	flushReadModelDirtyProxyIDs(context.Background(), false)
	requireReadModelSubprefixState(t, 4, 8, 0, 0)
	if got := attempts.snapshot(); !reflect.DeepEqual(got, []int{2, 1, 1, 1, 1}) {
		t.Fatal("slow successful commits prematurely retried the failed width", got)
	}
	flushReadModelDirtyProxyIDs(context.Background(), false)
	if readModelProxyWorkPending() || readModelSubprefixRowCount(t, db, proxyID) != 8 {
		t.Fatal("smaller statements lost work or fabricated a completed drain")
	}
	if got := attempts.snapshot(); !reflect.DeepEqual(got, []int{2, 1, 1, 1, 1, 1, 1, 1, 1}) {
		t.Fatal("slow batch repeated a known failed width", got)
	}
}
