package reputation

import (
	"sort"
	"strings"
	"time"
)

type Metrics struct {
	TotalChecks       int
	SuccessfulChecks  int
	ResponseTimesMS   []uint16
	LatestCheck       *time.Time
	LatestSuccess     *time.Time
	BestAnonymity     string
	EstimatedType     string
	FailureStreak     uint16
	SampleWindowHours float64
}

type Weights struct {
	Uptime    float64
	Recency   float64
	Latency   float64
	Anonymity float64
	Failures  float64
}

type ScoreResult struct {
	Score   float64
	Label   string
	Signals map[string]any
}

const (
	labelGood    = "good"
	labelNeutral = "neutral"
	labelPoor    = "poor"
)

var defaultWeights = Weights{
	Uptime:    0.45,
	Recency:   0.2,
	Latency:   0.15,
	Anonymity: 0.1,
	Failures:  0.1,
}

func Score(metrics Metrics, now time.Time, customWeights *Weights) ScoreResult {
	w := defaultWeights
	if customWeights != nil {
		w = *customWeights
		normaliseWeights(&w)
	}

	uptimeScore := calculateUptimeScore(metrics)
	recencyScore := calculateRecencyScore(metrics, now)
	latencyScore := calculateLatencyScore(metrics)
	anonymityScore := calculateAnonymityScore(metrics)
	failuresScore := calculateFailureScore(metrics)

	score := clamp01(
		w.Uptime*uptimeScore+
			w.Recency*recencyScore+
			w.Latency*latencyScore+
			w.Anonymity*anonymityScore+
			w.Failures*failuresScore,
	) * 100

	label := labelFromScore(score)

	result := ScoreResult{
		Score: score,
		Label: label,
	}

	signals := map[string]any{
		"uptime_score":     uptimeScore,
		"uptime_ratio":     ratio(metrics.SuccessfulChecks, metrics.TotalChecks),
		"recency_score":    recencyScore,
		"latency_score":    latencyScore,
		"anonymity_score":  anonymityScore,
		"anonymity":        sanitize(metrics.BestAnonymity),
		"estimated_type":   sanitize(metrics.EstimatedType),
		"failures_score":   failuresScore,
		"failure_streak":   metrics.FailureStreak,
		"sample_checks":    metrics.TotalChecks,
		"sample_successes": metrics.SuccessfulChecks,
		"sample_window_h":  metrics.SampleWindowHours,
	}

	if minutes, ok := minutesSince(metrics.LatestSuccess, now); ok {
		signals["recency_minutes"] = minutes
	}
	if medianMs, ok := median(metrics.ResponseTimesMS); ok {
		signals["latency_median_ms"] = medianMs
	}

	result.Signals = signals
	return result
}

func normaliseWeights(w *Weights) {
	total := w.Uptime + w.Recency + w.Latency + w.Anonymity + w.Failures
	if total <= 0 {
		*w = defaultWeights
		return
	}
	w.Uptime /= total
	w.Recency /= total
	w.Latency /= total
	w.Anonymity /= total
	w.Failures /= total
}

func calculateUptimeScore(m Metrics) float64 {
	if m.TotalChecks == 0 {
		return 0
	}
	return ratio(m.SuccessfulChecks, m.TotalChecks)
}

func calculateRecencyScore(m Metrics, now time.Time) float64 {
	if m.LatestSuccess == nil {
		return 0
	}
	const (
		fullScoreWindow = 30 * time.Minute
		maxWindow       = 6 * time.Hour
	)
	age := now.Sub(*m.LatestSuccess)
	if age <= fullScoreWindow {
		return 1
	}
	if age >= maxWindow {
		return 0
	}
	return clamp01(1 - age.Seconds()/maxWindow.Seconds())
}

func calculateLatencyScore(m Metrics) float64 {
	if len(m.ResponseTimesMS) == 0 {
		return 0
	}

	medianMs, _ := median(m.ResponseTimesMS)
	switch {
	case medianMs <= 400:
		return 1
	case medianMs >= 3000:
		return 0
	default:
		return clamp01(1 - (float64(medianMs)-250)/2750)
	}
}

func calculateAnonymityScore(m Metrics) float64 {
	anonymityScale := map[string]float64{
		"elite":       1.0,
		"anonymous":   0.8,
		"transparent": 0.3,
		"":            0.5,
	}
	typeScale := map[string]float64{
		"residential": 1.0,
		"isp":         0.9,
		"mobile":      0.85,
		"datacenter":  0.4,
		"":            0.6,
	}

	anonymityScore := anonymityScale[strings.ToLower(m.BestAnonymity)]
	if anonymityScore == 0 {
		anonymityScore = anonymityScale[""]
	}
	typeScore := typeScale[strings.ToLower(m.EstimatedType)]
	if typeScore == 0 {
		typeScore = typeScale[""]
	}

	return clamp01((anonymityScore + typeScore) / 2)
}

func calculateFailureScore(m Metrics) float64 {
	if m.FailureStreak == 0 {
		return 1
	}
	const maxPenaltyStreak = 5
	if m.FailureStreak >= maxPenaltyStreak {
		return 0
	}
	return clamp01(1 - float64(m.FailureStreak)/float64(maxPenaltyStreak))
}

func labelFromScore(score float64) string {
	switch {
	case score >= 80:
		return labelGood
	case score >= 40:
		return labelNeutral
	default:
		return labelPoor
	}
}

func ratio(success, total int) float64 {
	if total == 0 {
		return 0
	}
	return clamp01(float64(success) / float64(total))
}

func median(values []uint16) (float64, bool) {
	if len(values) == 0 {
		return 0, false
	}
	sorted := make([]uint16, len(values))
	copy(sorted, values)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i] < sorted[j]
	})
	mid := len(sorted) / 2
	if len(sorted)%2 == 0 {
		return float64(sorted[mid-1]+sorted[mid]) / 2, true
	}
	return float64(sorted[mid]), true
}

func clamp01(value float64) float64 {
	if value < 0 {
		return 0
	}
	if value > 1 {
		return 1
	}
	return value
}

func minutesSince(ts *time.Time, now time.Time) (float64, bool) {
	if ts == nil {
		return 0, false
	}
	return now.Sub(*ts).Minutes(), true
}

func sanitize(value string) string {
	return strings.TrimSpace(strings.ToLower(value))
}
