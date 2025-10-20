package dto

import "time"

type ProxySnapshotEntry struct {
	Count      int64     `json:"count"`
	RecordedAt time.Time `json:"recorded_at"`
}
