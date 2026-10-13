package domain

import "time"

// ProxyDailyCheck stores pre-aggregated check counts per proxy per UTC day.
type ProxyDailyCheck struct {
	ProxyID     uint64    `gorm:"primaryKey;autoIncrement:false;index:idx_daily_checks_retention,priority:2"`
	Day         time.Time `gorm:"type:date;primaryKey;index:idx_daily_checks_retention,priority:1"`
	ChecksCount int64     `gorm:"not null;default:0"`
	CreatedAt   time.Time `gorm:"autoCreateTime"`
	UpdatedAt   time.Time `gorm:"autoUpdateTime"`
}

func (ProxyDailyCheck) TableName() string {
	return "proxy_daily_checks"
}

// ProxyCheckTotal keeps compacted daily history in one row per route.
type ProxyCheckTotal struct {
	ProxyID         uint64    `gorm:"primaryKey;autoIncrement:false"`
	ChecksCount     int64     `gorm:"not null;default:0"`
	CompactedBefore time.Time `gorm:"not null"`
}
