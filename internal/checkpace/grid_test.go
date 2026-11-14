package checkpace

import (
	"math"
	"testing"
	"time"
)

const gridTestItems = 2_000

// points lists a grid's kept points for one item in (from, to].
func points(g grid, key uint64, from, to int64) []int64 {
	var out []int64
	for at := g.next(key, from); at <= to; at = g.next(key, at) {
		out = append(out, at)
	}
	return out
}

func TestFixedGridHasOnePointPerIntervalSpreadAcrossItems(t *testing.T) {
	g := fixedGrid(10 * time.Minute)
	span := (10 * time.Minute).Milliseconds()
	start := testNow.UnixMilli()
	// Ten equal parts of the interval should each hold about a tenth of the items.
	var parts [10]int
	for item := uint64(1); item <= gridTestItems; item++ {
		key := phaseKey(item)
		got := points(g, key, start, start+5*span)
		if len(got) != 5 {
			t.Fatalf("item %d has %d points in five intervals", item, len(got))
		}
		for index := 1; index < len(got); index++ {
			if got[index]-got[index-1] != span {
				t.Fatalf("item %d: points %d apart, want %d", item, got[index]-got[index-1], span)
			}
		}
		parts[(got[0]-start-1)*10/span]++
	}
	for part, count := range parts {
		if count < gridTestItems/10*7/10 || count > gridTestItems/10*13/10 {
			t.Fatalf("part %d of the interval holds %d of %d items: %v", part, count, gridTestItems, parts)
		}
	}
}

// Workspaces that own the same item share its checks when they are due at the
// same points. The grids of one item are therefore laid over each other: the
// points of a slower cadence are points of a faster one wherever the cadences
// allow it.
func TestGridsOfOneItemAreNested(t *testing.T) {
	start := testNow.UnixMilli()
	for _, pair := range []struct {
		name         string
		slower, fast grid
	}{
		{"equal fixed intervals", fixedGrid(10 * time.Minute), fixedGrid(10 * time.Minute)},
		{"a fixed interval and one that divides it", fixedGrid(time.Hour), fixedGrid(5 * time.Minute)},
		{"automatic cadences in the same span", spreadGrid(55*time.Minute, time.Minute), spreadGrid(47*time.Minute, time.Minute)},
		{"automatic cadences in different spans", spreadGrid(100*time.Hour, time.Minute), spreadGrid(47*time.Minute, time.Minute)},
		{"an automatic cadence and its floor", spreadGrid(47*time.Minute, time.Minute), fixedGrid(time.Minute)},
		{"an automatic cadence and a fixed power of two of its floor", spreadGrid(47*time.Minute, time.Minute), fixedGrid(8 * time.Minute)},
	} {
		for item := uint64(1); item <= 200; item++ {
			key := phaseKey(item)
			slower := points(pair.slower, key, start, start+20*pair.slower.span)
			if len(slower) < 20 {
				t.Fatalf("%s: item %d has %d points", pair.name, item, len(slower))
			}
			for _, point := range slower {
				if !pair.fast.due(key, point-1, point) {
					t.Fatalf("%s: item %d is due at %d for the slower cadence only", pair.name, item, point)
				}
			}
		}
	}
}

func TestGridDueAgreesWithNext(t *testing.T) {
	start := testNow.UnixMilli()
	for _, g := range []grid{fixedGrid(7 * time.Minute), spreadGrid(47*time.Minute, time.Minute), spreadGrid(96*time.Hour, 90*time.Second)} {
		for item := uint64(1); item <= 200; item++ {
			key := phaseKey(item)
			from := start + int64(mix(item)%uint64(g.span))
			for _, length := range []int64{0, 1, g.span / 7, g.span / 2, g.span, 3 * g.span} {
				to := from + length
				if got, want := g.due(key, from, to), g.next(key, from) <= to; got != want {
					t.Fatalf("grid %+v item %d: due(%d, %d) = %v, but the next point is at %d", g, item, from, to, got, g.next(key, from))
				}
			}
			// A point is in every window that ends at it and in none that starts at it.
			point := g.next(key, from)
			if !g.due(key, point-1, point) || g.due(key, point, point) || g.next(key, point) <= point {
				t.Fatalf("grid %+v item %d: point %d is not a half-open boundary", g, item, point)
			}
		}
	}
	if (grid{}).due(1, 0, math.MaxInt64/2) {
		t.Fatal("a workspace without a cadence has no points")
	}
}

func TestSpreadGridAveragesItsCadenceWithBoundedGaps(t *testing.T) {
	for _, cadence := range []time.Duration{61 * time.Second, 47 * time.Minute, 64 * time.Minute, 96 * time.Hour} {
		g := spreadGrid(cadence, time.Minute)
		if float64(g.span) < float64(cadence.Milliseconds()) || g.span/2 >= cadence.Milliseconds() {
			t.Fatalf("cadence %s got span %dms: want the first span that is not shorter", cadence, g.span)
		}
		start := testNow.UnixMilli()
		length := 400 * g.span
		total := 0
		for item := uint64(1); item <= 200; item++ {
			got := points(g, phaseKey(item), start, start+length)
			total += len(got)
			for index := 1; index < len(got); index++ {
				if gap := got[index] - got[index-1]; gap != g.span && gap != g.span/2 {
					t.Fatalf("cadence %s: points %dms apart, want %d or %d", cadence, gap, g.span/2, g.span)
				}
			}
		}
		want := 200 * float64(length) / float64(cadence.Milliseconds())
		if math.Abs(float64(total)-want) > want*0.01 {
			t.Fatalf("cadence %s: %d points, want about %.0f", cadence, total, want)
		}
	}
	// At its floor an automatic cadence is exactly periodic.
	if g := spreadGrid(time.Minute, time.Minute); g.extra != 0 || g.span != time.Minute.Milliseconds() {
		t.Fatalf("floor cadence grid = %+v", g)
	}
	if g := spreadGrid(30*time.Second, 90*time.Second); g.extra != 0 || g.span != (90*time.Second).Milliseconds() {
		t.Fatalf("grid below the floor = %+v", g)
	}
	// Rounding must not turn "keep every midpoint" into "keep none".
	if g := spreadGrid(time.Minute+time.Nanosecond, time.Minute); g.extra < math.MaxUint64/2 {
		t.Fatalf("a cadence just above half the span keeps %d/2^64 of the midpoints", g.extra)
	}
}

// An automatic cadence is recomputed at every refresh. Its points must stay
// where they are, apart from the few midpoints that the changed share adds or
// drops, or a regular cadence degrades into random sampling.
func TestSpreadGridPointsDoNotMoveWhenTheCadenceChanges(t *testing.T) {
	start := testNow.UnixMilli()
	differing := func(a, b grid, length int64) (int, int) {
		total, different := 0, 0
		for item := uint64(1); item <= 400; item++ {
			key := phaseKey(item)
			inB := map[int64]bool{}
			for _, point := range points(b, key, start, start+length) {
				inB[point] = true
			}
			for _, point := range points(a, key, start, start+length) {
				total++
				if !inB[point] {
					different++
				}
				delete(inB, point)
			}
			different += len(inB)
		}
		return total, different
	}
	for _, change := range []struct {
		name     string
		from, to time.Duration
		limit    float64 // share of points that may differ
	}{
		{"half a percent", 47 * time.Minute, 47*time.Minute + 14*time.Second, 0.02},
		{"a millisecond", 100 * time.Minute, 100*time.Minute + time.Millisecond, 0.001},
		// 64 minutes is a span: the grid changes shape there, not its points.
		{"across a span", 64*time.Minute - 10*time.Second, 64*time.Minute + 10*time.Second, 0.02},
	} {
		a, b := spreadGrid(change.from, time.Minute), spreadGrid(change.to, time.Minute)
		total, different := differing(a, b, 200*a.span)
		if float64(different) > float64(total)*change.limit {
			t.Fatalf("%s: %d of %d points differ between %s and %s", change.name, different, total, change.from, change.to)
		}
	}
	// A longer span keeps a subset of a shorter span's boundaries.
	short, long := grid{span: 60_000}, grid{span: 480_000}
	for item := uint64(1); item <= 200; item++ {
		key := phaseKey(item)
		for _, point := range points(long, key, start, start+20*long.span) {
			if !short.due(key, point-1, point) {
				t.Fatalf("item %d: the long span's point %d is not on the short span", item, point)
			}
		}
	}
}
