package domain

import (
	"gorm.io/gorm"
	"time"
)

const (
	AlertMetricUsableRoutes = "usable_routes"
	AlertMetricSuccessRate  = "success_rate"
	AlertMetricLatency      = "latency_ms"
)

// Alerts consume existing measurements outside the checker and queue loops.
type AlertRule struct {
	ID              uint64     `gorm:"primaryKey" json:"id"`
	WorkspaceID     uint       `gorm:"not null;index" json:"workspace_id"`
	Workspace       Workspace  `gorm:"constraint:OnDelete:CASCADE" json:"-"`
	Name            string     `gorm:"not null;size:120" json:"name"`
	RotatorID       *uint64    `gorm:"index" json:"rotator_id"`
	Metric          string     `gorm:"not null;size:24" json:"metric"`
	Threshold       float64    `gorm:"not null" json:"threshold"`
	Enabled         bool       `gorm:"not null" json:"enabled"`
	DestinationIDs  []uint64   `gorm:"serializer:json;type:jsonb" json:"destination_ids"`
	Revision        uint64     `gorm:"not null;default:1" json:"revision"`
	Status          string     `gorm:"not null;size:24;default:'unknown'" json:"status"`
	UnknownReason   string     `gorm:"size:120" json:"unknown_reason"`
	LastValue       *float64   `json:"last_value"`
	SampleCount     int64      `json:"sample_count"`
	LastEvaluatedAt *time.Time `json:"last_evaluated_at"`
	// Scheduling survives timeouts and leadership changes without treating an
	// attempted measurement as a successful observation.
	LastEvaluationAttemptAt *time.Time     `json:"-"`
	BreachSince             *time.Time     `json:"breach_since"`
	RecoverySince           *time.Time     `json:"recovery_since"`
	ActiveIncidentID        *uint64        `json:"active_incident_id"`
	CreatedAt               time.Time      `json:"created_at"`
	UpdatedAt               time.Time      `json:"updated_at"`
	DeletedAt               gorm.DeletedAt `gorm:"index" json:"-"`
}

type AlertDestination struct {
	ID                     uint64         `gorm:"primaryKey" json:"id"`
	WorkspaceID            uint           `gorm:"not null;index" json:"-"`
	Workspace              Workspace      `gorm:"constraint:OnDelete:CASCADE" json:"-"`
	Name                   string         `gorm:"not null;size:120" json:"name"`
	Kind                   string         `gorm:"not null;size:16" json:"kind"`
	Enabled                bool           `gorm:"not null" json:"enabled"`
	MentionMode            string         `gorm:"not null;size:16;default:'none'" json:"mention_mode"`
	MentionID              string         `gorm:"not null;size:32;default:''" json:"mention_id"`
	TargetEncrypted        string         `gorm:"not null;type:text" json:"-"`
	SigningSecretEncrypted string         `gorm:"type:text" json:"-"`
	CreatedAt              time.Time      `json:"created_at"`
	UpdatedAt              time.Time      `json:"updated_at"`
	DeletedAt              gorm.DeletedAt `gorm:"index" json:"-"`
}

type AlertIncident struct {
	ID           uint64     `gorm:"primaryKey" json:"id"`
	WorkspaceID  uint       `gorm:"not null;index:idx_alert_incidents_workspace_opened,priority:1" json:"workspace_id"`
	Workspace    Workspace  `gorm:"constraint:OnDelete:CASCADE" json:"-"`
	RuleID       uint64     `gorm:"not null;index" json:"rule_id"`
	RuleRevision uint64     `gorm:"not null" json:"rule_revision"`
	RuleName     string     `gorm:"not null;size:120" json:"rule_name"`
	ScopeName    string     `gorm:"not null;size:120" json:"scope_name"`
	Metric       string     `gorm:"not null;size:24" json:"metric"`
	Threshold    float64    `json:"threshold"`
	OpeningValue float64    `json:"opening_value"`
	ClosingValue *float64   `json:"closing_value"`
	OpenedAt     time.Time  `gorm:"not null;index:idx_alert_incidents_workspace_opened,priority:2,sort:desc" json:"opened_at"`
	ClosedAt     *time.Time `gorm:"index" json:"closed_at"`
	CloseReason  string     `gorm:"size:32" json:"close_reason"`
}

type AlertDelivery struct {
	ID              uint64        `gorm:"primaryKey" json:"id"`
	WorkspaceID     uint          `gorm:"not null;index" json:"-"`
	Workspace       Workspace     `gorm:"constraint:OnDelete:CASCADE" json:"-"`
	IncidentID      uint64        `gorm:"not null;index" json:"incident_id"`
	Incident        AlertIncident `gorm:"constraint:OnDelete:CASCADE" json:"-"`
	DestinationID   uint64        `gorm:"not null;index" json:"destination_id"`
	DestinationName string        `gorm:"size:120" json:"destination_name"`
	Kind            string        `gorm:"size:16" json:"kind"`
	Event           string        `gorm:"size:16" json:"event"`
	Payload         string        `gorm:"not null;type:text" json:"-"`
	Status          string        `gorm:"not null;size:24;index:idx_alert_delivery_due,priority:1" json:"status"`
	Attempts        int           `gorm:"not null;default:0" json:"attempts"`
	LastError       string        `gorm:"size:120" json:"last_error"`
	NextAttemptAt   time.Time     `gorm:"not null;index:idx_alert_delivery_due,priority:2" json:"-"`
	ClaimToken      string        `gorm:"size:64" json:"-"`
	LastAttemptAt   *time.Time    `json:"last_attempt_at"`
	SentAt          *time.Time    `json:"sent_at"`
	CreatedAt       time.Time     `json:"created_at"`
}
