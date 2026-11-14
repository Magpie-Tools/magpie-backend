package checkpace

import (
	"math"
	"time"
)

// A cadence is realised as a grid: fixed points in time, in Unix milliseconds,
// at which one workspace is due on one route or source. A grid depends only on
// the cadence and on the item and workspace IDs, so every instance computes the
// same points without per-route or per-workspace state, and an item's points
// do not move while its workspace's cadence stays the same.
//
// A cadence a workspace or its plan chose (a fixed interval, the unlimited
// default, a floor) is constant, and its grid has one boundary per interval.
//
// An automatic cadence is recomputed at every refresh and rarely repeats to
// the millisecond. A grid spaced by that value would move all of its points at
// every refresh, which turns a regular cadence into random sampling. Its grid
// is therefore built from spans that are the workspace's floor times a power of
// two. Those grids are nested: every boundary of a span is also a boundary of
// half that span. The grid takes the first span that is not shorter than the
// cadence, keeps all of its boundaries, and keeps the midpoint between two
// boundaries for the share of slots that makes the average spacing equal the
// cadence. A changing cadence then only changes which midpoints are kept, and
// crossing into the next span changes nothing at the crossing itself. One
// item's checks are a span or half a span apart: more than half the cadence and
// at most twice the cadence.
type grid struct {
	// span is the distance in milliseconds between the boundaries that are
	// always kept. Zero means the workspace has no cadence (it is exhausted).
	span int64
	// extra is the share of midpoints kept, as a fraction of 1<<64. Zero
	// keeps none, which makes the grid exactly periodic.
	extra uint64
}

// maxGridSpan bounds the doubling in spreadGrid far above any computed cadence.
const maxGridSpan = int64(4 * maxComputedInterval / time.Millisecond)

// fixedGrid has one boundary per interval.
func fixedGrid(interval time.Duration) grid {
	return grid{span: max(interval.Milliseconds(), 1)}
}

// spreadGrid is the grid of an automatic cadence above its floor.
func spreadGrid(interval, floor time.Duration) grid {
	span := max(floor.Milliseconds(), 1)
	target := float64(interval) / float64(time.Millisecond)
	if target <= float64(span) {
		return grid{span: span}
	}
	for float64(span) < target && span < maxGridSpan {
		span *= 2
	}
	// One boundary and a share of one midpoint per span: (1 + share) checks
	// per span must be one check per target.
	share := float64(span)/target - 1
	if share <= 0 {
		return grid{span: span}
	}
	// Half the span is shorter than the target, so the share is below one;
	// rounding may still reach it.
	if share >= 1 {
		return grid{span: span, extra: math.MaxUint64}
	}
	return grid{span: span, extra: uint64(share*(1<<63)) << 1}
}

// phaseKey spreads a workspace's items across its grid. It depends on the item
// only, so the grids of all workspaces on one item are laid over each other:
// equal cadences have the same points, a fixed interval's points are points of
// every interval that divides it, and the points of an automatic cadence are
// points of every faster automatic cadence on the same floor. Workspaces that
// are due at the same point share one check of the item.
func phaseKey(itemID uint64) uint64 {
	return mix(itemID)
}

// due reports whether the grid has a kept point in (from, to], in Unix
// milliseconds.
func (g grid) due(key uint64, from, to int64) bool {
	if g.span <= 0 || to <= from {
		return false
	}
	// Spans divide each other, so this offset places an item's points of a
	// longer span on its points of every shorter one.
	offset := int64(key % uint64(g.span))
	if g.extra == 0 {
		return floorDiv(to+offset, g.span) != floorDiv(from+offset, g.span)
	}
	half := g.span / 2
	first, last := floorDiv(from+offset, half)+1, floorDiv(to+offset, half)
	switch {
	case last < first:
		return false
	case last > first:
		// Two neighbouring slots always include a boundary.
		return true
	default:
		return first%2 == 0 || g.keeps(key, first)
	}
}

// next returns the first kept point after a time, in Unix milliseconds. The
// grid must have a span.
func (g grid) next(key uint64, after int64) int64 {
	offset := int64(key % uint64(g.span))
	if g.extra == 0 {
		return (floorDiv(after+offset, g.span)+1)*g.span - offset
	}
	half := g.span / 2
	slot := floorDiv(after+offset, half) + 1
	if slot%2 != 0 && !g.keeps(key, slot) {
		slot++
	}
	return slot*half - offset
}

// keeps decides one midpoint. The decision depends on the slot, not on earlier
// decisions, so a slightly different share changes only the slots whose hash
// lies between the two shares.
func (g grid) keeps(key uint64, slot int64) bool {
	return mix(key^uint64(slot)*0xd6e8feb86659fd93) < g.extra
}

func floorDiv(value, divisor int64) int64 {
	quotient := value / divisor
	if value%divisor != 0 && (value < 0) != (divisor < 0) {
		quotient--
	}
	return quotient
}

// mix is the SplitMix64 finalizer, used only to spread phases.
func mix(value uint64) uint64 {
	value += 0x9e3779b97f4a7c15
	value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9
	value = (value ^ (value >> 27)) * 0x94d049bb133111eb
	return value ^ (value >> 31)
}
