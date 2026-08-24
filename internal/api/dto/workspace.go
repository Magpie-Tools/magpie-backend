package dto

import "time"

type WorkspaceCapacity struct {
	ActiveRoutes     uint64  `json:"active_routes"`
	StoredRoutes     uint64  `json:"stored_routes"`
	IncludedRoutes   uint64  `json:"included_routes"`
	AdditionalRoutes uint64  `json:"additional_routes"`
	OverageRoutes    uint64  `json:"overage_routes"`
	ActivationLimit  *uint64 `json:"activation_limit"`
	OverageMode      string  `json:"overage_mode"`
}

type WorkspaceSubscription struct {
	PlanCode                    string     `json:"plan_code"`
	Status                      string     `json:"status"`
	IncludedOperators           uint32     `json:"included_operators"`
	StatisticsRetentionDays     uint32     `json:"statistics_retention_days"`
	MinimumCheckIntervalSeconds uint32     `json:"minimum_check_interval_seconds"`
	CurrentPeriodStart          *time.Time `json:"current_period_start,omitempty"`
	CurrentPeriodEnd            *time.Time `json:"current_period_end,omitempty"`
	CancelAtPeriodEnd           bool       `json:"cancel_at_period_end"`
}

type Workspace struct {
	ID           uint                  `json:"id"`
	Name         string                `json:"name"`
	Personal     bool                  `json:"personal"`
	Role         string                `json:"role"`
	BillingAdmin bool                  `json:"billing_admin"`
	IsDefault    bool                  `json:"is_default"`
	Capacity     WorkspaceCapacity     `json:"capacity"`
	Subscription WorkspaceSubscription `json:"subscription"`
	CreatedAt    time.Time             `json:"created_at"`
}

type WorkspaceCreateRequest struct {
	Name string `json:"name"`
}

type WorkspaceUpdateRequest struct {
	Name string `json:"name"`
}

type WorkspaceMember struct {
	UserID       uint      `json:"user_id"`
	Email        string    `json:"email"`
	Role         string    `json:"role"`
	BillingAdmin bool      `json:"billing_admin"`
	JoinedAt     time.Time `json:"joined_at"`
}

type WorkspaceMemberCreateRequest struct {
	Email        string `json:"email"`
	Role         string `json:"role"`
	BillingAdmin bool   `json:"billing_admin"`
}

type WorkspaceMemberUpdateRequest struct {
	Role         string `json:"role"`
	BillingAdmin bool   `json:"billing_admin"`
}

type ManagedProxyLifecycleRequest struct {
	State string `json:"state"`
}
