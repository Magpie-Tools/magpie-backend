package database

import "time"

// A window belongs only to one retained owner batch. A deadline halves the
// attempted width. A successful full-width commit using more than half of the
// child budget also halves the next width, without creating a retry interval.
// Two full-width commits using at most a quarter of the budget permit growth.
// Releasing the batch releases this learning too.
type readModelProjectionWindow struct {
	limit       int
	fastCommits uint8
}

func (w *readModelProjectionWindow) pairLimit(budget int) int {
	if w.limit > 0 {
		return min(w.limit, budget)
	}
	return budget
}

func (w *readModelProjectionWindow) deadline(attempted int) {
	w.limit = max(1, attempted/2)
	w.fastCommits = 0
}

func (w *readModelProjectionWindow) failed() {
	w.fastCommits = 0
}

func (w *readModelProjectionWindow) committed(pairs int, elapsed, timeout time.Duration, budget int) {
	if budget <= 0 || pairs < w.pairLimit(budget) || elapsed <= 0 || timeout <= 0 {
		w.fastCommits = 0
		return
	}
	if elapsed > timeout/2 && pairs > 1 {
		w.deadline(pairs)
		return
	}
	if w.limit == 0 {
		return
	}
	if elapsed > timeout/4 {
		w.fastCommits = 0
		return
	}
	w.fastCommits++
	if w.fastCommits < 2 {
		return
	}
	w.fastCommits = 0
	if w.limit >= budget-w.limit {
		w.limit = 0 // normal configured width
	} else {
		w.limit *= 2
	}
}
