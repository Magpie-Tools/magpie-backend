package domain

import "time"

// One cache entry per canonical literal IP, shared by all route ports.
type IPGeolocationLookup struct {
	IP               string     `gorm:"primaryKey;size:45;index:idx_ipgeolocation_due,priority:2" json:"ip"`
	Country          string     `gorm:"size:128;not null;default:''" json:"country"`
	EstimatedType    string     `gorm:"size:20;not null;default:''" json:"estimated_type"`
	Payload          string     `gorm:"type:text;not null;default:''" json:"-"`
	SecurityIncluded bool       `gorm:"not null;default:false" json:"security_included"`
	Policy           uint32     `gorm:"not null;default:0" json:"-"`
	ApplyPending     bool       `gorm:"not null;default:false;index:idx_ipgeolocation_apply" json:"-"`
	ApplyCursor      uint64     `gorm:"not null;default:0" json:"-"`
	CheckedAt        *time.Time `json:"checked_at"`
	ExpiresAt        *time.Time `json:"expires_at"`
	NextLookupAt     time.Time  `gorm:"not null;index:idx_ipgeolocation_due,priority:1" json:"-"`
	CreatedAt        time.Time  `gorm:"autoCreateTime" json:"-"`
}

func (IPGeolocationLookup) TableName() string { return "ipgeolocation_lookups" }

// Persistent pacing and credit reservations survive restarts and leader changes.
type IPGeolocationState struct {
	ID              uint       `gorm:"primaryKey" json:"-"`
	DiscoveryCursor uint64     `gorm:"not null;default:0" json:"-"`
	NextDiscoveryAt *time.Time `json:"-"`
	Policy          uint32     `gorm:"not null;default:0" json:"-"`
	BudgetDay       string     `gorm:"size:10;not null;default:''" json:"budget_day"`
	UsedCredits     int        `gorm:"not null;default:0" json:"used_credits"`
	NextRequestAt   *time.Time `json:"next_request_at"`
	LastCheckedAt   *time.Time `json:"last_checked_at"`
	LastError       string     `gorm:"size:512;not null;default:''" json:"last_error"`
}

func (IPGeolocationState) TableName() string { return "ipgeolocation_states" }
