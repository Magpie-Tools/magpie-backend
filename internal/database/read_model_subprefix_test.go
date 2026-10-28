package database

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestReadModelSubprefixNormalSuccessKeepsDefaultWidth(t *testing.T) {
	batch := &readModelProjectionBatch{pairs: make([]readModelProjectionPair, 20000)}
	for _, want := range []int{5000, 10000, 15000, 20000} {
		end := batch.prefixEnd(5000)
		if end != want {
			t.Fatalf("ordinary prefix ends at %d, want %d", end, want)
		}
		batch.commitPrefix(end)
		if batch.retryEnd != 0 || batch.retryWidth != 0 {
			t.Fatal("ordinary success entered retry mode", batch.retryEnd, batch.retryWidth)
		}
	}
}

func TestReadModelSubprefixDeadlineRetainsContiguousInterval(t *testing.T) {
	batch := &readModelProjectionBatch{pairs: make([]readModelProjectionPair, 16)}
	if !batch.retainDeadlinePrefix(nil, context.DeadlineExceeded, context.DeadlineExceeded, 0, 8) || batch.next != 0 {
		t.Fatal("deadline did not retain the uncommitted interval", batch)
	}
	if end := batch.prefixEnd(8); end != 4 {
		t.Fatal("first retry did not halve the statement", end)
	}
	batch.commitPrefix(4)
	if batch.retryEnd != 8 || batch.retryWidth != 4 {
		t.Fatal("partial success cleared the failed interval too early", batch)
	}
	if !batch.retainDeadlinePrefix(nil, context.DeadlineExceeded, context.DeadlineExceeded, 4, 8) || batch.next != 4 {
		t.Fatal("a second deadline lost the committed prefix", batch)
	}
	if end := batch.prefixEnd(8); end != 6 {
		t.Fatal("second retry did not shrink the remaining statement", end)
	}
	batch.commitPrefix(6)
	if end := batch.prefixEnd(8); end != 8 {
		t.Fatal("retry crossed the original failed interval", end)
	}
	batch.commitPrefix(8)
	if batch.retryEnd != 0 || batch.retryWidth != 0 || batch.prefixEnd(8) != 10 {
		t.Fatal("completed interval forgot its measured deadline width", batch)
	}
}

func TestReadModelSubprefixOddIntervalEndsBeforeUntouchedSuffix(t *testing.T) {
	batch := &readModelProjectionBatch{pairs: make([]readModelProjectionPair, 12)}
	if !batch.retainDeadlinePrefix(nil, context.DeadlineExceeded, context.DeadlineExceeded, 0, 5) {
		t.Fatal("odd-width deadline was not retained")
	}
	for _, want := range []int{2, 4, 5} {
		if got := batch.prefixEnd(5); got != want {
			t.Fatalf("retry ends at %d, want %d", got, want)
		}
		batch.commitPrefix(want)
	}
	if got := batch.prefixEnd(5); got != 7 {
		t.Fatal("odd interval forgot the reduced width before fast commits", got)
	}
}

func TestReadModelSubprefixOnlyLiveParentChildDeadlineCanSplit(t *testing.T) {
	for _, tc := range []struct {
		name       string
		parentErr  error
		attemptErr error
		start      int
		end        int
	}{
		{"parent_canceled", context.Canceled, context.DeadlineExceeded, 0, 4},
		{"parent_deadline", context.DeadlineExceeded, context.DeadlineExceeded, 0, 4},
		{"child_canceled", nil, context.Canceled, 0, 4},
		{"permanent_sql_error", nil, errors.New("constraint violation"), 0, 4},
		{"no_child_deadline", nil, nil, 0, 4},
		{"single_pair", nil, context.DeadlineExceeded, 0, 1},
		{"empty_suffix", nil, context.DeadlineExceeded, 0, 0},
		{"wrong_cursor", nil, context.DeadlineExceeded, 1, 4},
		{"past_owner_pairs", nil, context.DeadlineExceeded, 0, 9},
	} {
		t.Run(tc.name, func(t *testing.T) {
			batch := &readModelProjectionBatch{pairs: make([]readModelProjectionPair, 8)}
			before := *batch
			if batch.retainDeadlinePrefix(tc.parentErr, tc.attemptErr, context.DeadlineExceeded, tc.start, tc.end) || !reflect.DeepEqual(*batch, before) {
				t.Fatal("nonrecoverable attempt changed retained progress", batch)
			}
		})
	}
}

func TestReadModelSubprefixSinglePairFailureKeepsExistingRetryInterval(t *testing.T) {
	batch := &readModelProjectionBatch{pairs: make([]readModelProjectionPair, 8), next: 3, retryEnd: 6, retryWidth: 1}
	before := *batch
	if batch.retainDeadlinePrefix(nil, context.DeadlineExceeded, context.DeadlineExceeded, 3, 4) || !reflect.DeepEqual(*batch, before) {
		t.Fatal("one-pair deadline was dropped or skipped", batch)
	}
	if batch.prefixEnd(5000) != 4 {
		t.Fatal("one-pair failure widened or skipped its retry")
	}
}

func TestReadModelSubprefixCancellationPreservesExistingRetryInterval(t *testing.T) {
	batch := &readModelProjectionBatch{pairs: make([]readModelProjectionPair, 12), next: 2, retryEnd: 8, retryWidth: 2}
	before := *batch
	if batch.retainDeadlinePrefix(context.Canceled, context.DeadlineExceeded, context.DeadlineExceeded, 2, 4) || !reflect.DeepEqual(*batch, before) {
		t.Fatal("parent cancellation changed an existing retained interval", batch)
	}
	if batch.retainDeadlinePrefix(nil, context.Canceled, context.Canceled, 2, 4) || !reflect.DeepEqual(*batch, before) {
		t.Fatal("child cancellation changed an existing retained interval", batch)
	}
}

func TestReadModelSubprefixPermanentErrorAtDeadlineDoesNotSplit(t *testing.T) {
	batch := &readModelProjectionBatch{pairs: make([]readModelProjectionPair, 8)}
	before := *batch
	if batch.retainDeadlinePrefix(nil, context.DeadlineExceeded, errors.New("constraint violation"), 0, 4) || !reflect.DeepEqual(*batch, before) {
		t.Fatal("permanent error racing a child deadline triggered subdivision", batch)
	}
	if batch.retainDeadlinePrefix(nil, context.DeadlineExceeded, context.Canceled, 0, 4) || !reflect.DeepEqual(*batch, before) {
		t.Fatal("unrecognized cancellation was reclassified as a known deadline", batch)
	}
	wrapped := errors.Join(errors.New("driver timeout"), context.DeadlineExceeded)
	if !batch.retainDeadlinePrefix(nil, context.DeadlineExceeded, wrapped, 0, 4) {
		t.Fatal("wrapped actual child deadline was not recognized")
	}
}
