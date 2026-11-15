package dashboard

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"magpie/internal/api/dto"
)

// payloadVersion is the format of the stored payload and health columns. A
// reader asks only for rows of its own version, so instances of adjacent
// releases with different formats build separate rows instead of misreading
// each other's. Version 2 moved the health parts out of the payload into
// their own column, which outlives builds that cannot compute them.
const payloadVersion int16 = 2

const (
	// recentCandidateLimit is how many recent-check candidates a build keeps,
	// the largest limit recentProxyChecks accepts.
	recentCandidateLimit = 50
	// fastestLimit is how many fastest alive routes a build keeps, the
	// largest limit fastestAliveProxies accepts.
	fastestLimit = 100
)

// payload is the inventory of one published build, valid at any checker
// generation. The health parts are stored in their own column (see
// healthPart).
type payload struct {
	Inventory inventoryPart `json:"inventory"`
}

type inventoryPart struct {
	TotalScraped        int64               `json:"total_scraped"`
	ProxyCount          int64               `json:"proxy_count"`
	CountryBreakdown    []countryCount      `json:"country_breakdown"`
	ReputationBreakdown reputationBreakdown `json:"reputation_breakdown"`
	TopReputationProxy  *topReputationProxy `json:"top_reputation_proxy,omitempty"`
}

type countryCount struct {
	Country string `json:"country"`
	Count   uint   `json:"count"`
}

type reputationBreakdown struct {
	Good    uint `json:"good"`
	Neutral uint `json:"neutral"`
	Poor    uint `json:"poor"`
	Unknown uint `json:"unknown"`
}

type topReputationProxy struct {
	ProxyID uint64  `json:"proxy_id"`
	IP      string  `json:"ip"`
	Port    uint16  `json:"port"`
	Score   float32 `json:"score"`
	Label   string  `json:"label"`
}

// healthPart is the evidence-derived part of the dashboard. It was computed
// under the row's checker_generation; it is current while that is the
// committed generation and the workspace is not dirty, and is otherwise
// served as the previous result, marked outdated.
type healthPart struct {
	AliveCount        int64                   `json:"alive_count"`
	JudgeValidProxies []judgeCount            `json:"judge_valid_proxies"`
	RecentCandidates  []recentCandidate       `json:"recent_candidates"`
	Fastest           []dto.ProxyFastestAlive `json:"fastest"`
}

type judgeCount struct {
	JudgeURL           string `json:"judge_url"`
	EliteProxies       uint   `json:"elite_proxies"`
	AnonymousProxies   uint   `json:"anonymous_proxies"`
	TransparentProxies uint   `json:"transparent_proxies"`
}

// recentCandidate is one of the first recentCandidateLimit routes in the
// read model's recency order (Ordinal, from 1), with the keys of the final
// order: alive first, then the latest check with missing checks last, then
// the route ID.
type recentCandidate struct {
	Ordinal      int        `json:"ordinal"`
	ID           uint64     `json:"id"`
	IP           string     `json:"ip"`
	Port         uint16     `json:"port"`
	ResponseTime uint16     `json:"response_time"`
	Alive        bool       `json:"alive"`
	LatestCheck  *time.Time `json:"latest_check,omitempty"`
}

// recentChecks returns what the recent-checks query returns for limit: the
// candidates the query's LIMIT would select, in its final order.
func (h *healthPart) recentChecks(limit int) []dto.ProxyRecentCheck {
	selected := make([]recentCandidate, 0, min(limit, len(h.RecentCandidates)))
	for _, candidate := range h.RecentCandidates {
		if candidate.Ordinal <= limit {
			selected = append(selected, candidate)
		}
	}
	sort.SliceStable(selected, func(i, j int) bool { return recentBefore(selected[i], selected[j]) })
	result := make([]dto.ProxyRecentCheck, 0, len(selected))
	for _, candidate := range selected {
		check := dto.ProxyRecentCheck{ID: candidate.ID, IP: candidate.IP, Port: candidate.Port, ResponseTime: candidate.ResponseTime, Alive: candidate.Alive}
		if candidate.LatestCheck != nil {
			check.LatestCheck = *candidate.LatestCheck
		}
		result = append(result, check)
	}
	return result
}

// recentBefore is ORDER BY alive DESC, latest_check DESC NULLS LAST, id ASC.
func recentBefore(a, b recentCandidate) bool {
	if a.Alive != b.Alive {
		return a.Alive
	}
	switch {
	case a.LatestCheck == nil && b.LatestCheck != nil:
		return false
	case a.LatestCheck != nil && b.LatestCheck == nil:
		return true
	case a.LatestCheck != nil && !a.LatestCheck.Equal(*b.LatestCheck):
		return a.LatestCheck.After(*b.LatestCheck)
	}
	return a.ID < b.ID
}

// fastestAlive returns the first limit routes of the stored order, which has
// a total order through its route-ID tie-breaker.
func (h *healthPart) fastestAlive(limit int) []dto.ProxyFastestAlive {
	selected := h.Fastest[:min(limit, len(h.Fastest))]
	// A computed empty list stays an empty list; nil means unavailable.
	return append(make([]dto.ProxyFastestAlive, 0, len(selected)), selected...)
}

// encodeSnapshot encodes a build's inventory payload and, when it computed
// them, its health parts. Together they are at most maxPayloadBytes.
func encodeSnapshot(value payload, health *healthPart) ([]byte, []byte, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, nil, err
	}
	var encodedHealth []byte
	if health != nil {
		if encodedHealth, err = json.Marshal(health); err != nil {
			return nil, nil, err
		}
	}
	if size := len(encoded) + len(encodedHealth); size > maxPayloadBytes {
		return nil, nil, fmt.Errorf("dashboard: payload of %d bytes exceeds the %d byte limit", size, maxPayloadBytes)
	}
	return encoded, encodedHealth, nil
}

func decodePayload(encoded []byte) (*payload, error) {
	if len(encoded) == 0 {
		return nil, nil
	}
	var value payload
	if err := json.Unmarshal(encoded, &value); err != nil {
		return nil, fmt.Errorf("dashboard: decode snapshot payload: %w", err)
	}
	return &value, nil
}

func decodeHealth(encoded []byte) (*healthPart, error) {
	if len(encoded) == 0 {
		return nil, nil
	}
	var value healthPart
	if err := json.Unmarshal(encoded, &value); err != nil {
		return nil, fmt.Errorf("dashboard: decode snapshot health: %w", err)
	}
	return &value, nil
}
