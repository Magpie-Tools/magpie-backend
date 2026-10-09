package dto

import "time"

// Alert scope selection needs metadata only, never credentials or pool counts.
type AlertRotator struct {
	ID       uint64 `json:"id"`
	Name     string `json:"name"`
	Protocol string `json:"protocol"`
}

type AlertRuleWrite struct {
	Name           string   `json:"name"`
	RotatorID      *uint64  `json:"rotator_id"`
	Metric         string   `json:"metric"`
	Threshold      *float64 `json:"threshold"`
	Enabled        bool     `json:"enabled"`
	DestinationIDs []uint64 `json:"destination_ids"`
}

type AlertDestinationWrite struct {
	Name          string  `json:"name"`
	Kind          string  `json:"kind"`
	Enabled       bool    `json:"enabled"`
	Target        *string `json:"target"`
	SigningSecret *string `json:"signing_secret"`
	MentionMode   *string `json:"mention_mode"`
	MentionID     *string `json:"mention_id"`
}

// Secret targets, including email addresses, are write-only.
type AlertDestination struct {
	ID                uint64 `json:"id"`
	Name              string `json:"name"`
	Kind              string `json:"kind"`
	Enabled           bool   `json:"enabled"`
	TargetConfigured  bool   `json:"target_configured"`
	SigningConfigured bool   `json:"signing_configured"`
	MentionMode       string `json:"mention_mode"`
	MentionID         string `json:"mention_id"`
}

type AlertEvent struct {
	Version          int       `json:"version"`
	IncidentID       uint64    `json:"incident_id"`
	WorkspaceID      uint      `json:"workspace_id"`
	WorkspaceName    string    `json:"workspace_name"`
	RuleID           uint64    `json:"rule_id"`
	RuleName         string    `json:"rule_name"`
	ScopeName        string    `json:"scope_name"`
	RotatorID        *uint64   `json:"rotator_id"`
	MeasurementScope string    `json:"measurement_scope"`
	Protocol         string    `json:"protocol,omitempty"`
	Metric           string    `json:"metric"`
	Threshold        float64   `json:"threshold"`
	Value            float64   `json:"value"`
	Event            string    `json:"event"`
	OccurredAt       time.Time `json:"occurred_at"`
}
