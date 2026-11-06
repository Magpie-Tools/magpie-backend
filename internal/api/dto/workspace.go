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

// WorkspaceAllowances reports monthly check and scrape allowances, metered
// usage and the cadence Magpie derived from them. Values come from the most
// recent allowance snapshot and can lag usage by about a minute.
type WorkspaceAllowances struct {
	// Unlimited workspaces have no monthly limit; their monthly_* value is
	// not enforced. They run at a fixed interval, or hourly until they
	// choose one (needs_*_interval).
	UnlimitedChecks       bool   `json:"unlimited_checks"`
	UnlimitedScrapes      bool   `json:"unlimited_scrapes"`
	NeedsCheckInterval    bool   `json:"needs_check_interval"`
	NeedsScrapeInterval   bool   `json:"needs_scrape_interval"`
	MonthlyChecks         uint64 `json:"monthly_checks"`
	UsedChecks            uint64 `json:"used_checks"`
	ChecksPerCycle        uint64 `json:"checks_per_cycle"`
	CheckIntervalSeconds  uint64 `json:"check_interval_seconds"`
	ChecksExhausted       bool   `json:"checks_exhausted"`
	MonthlyScrapes        uint64 `json:"monthly_scrapes"`
	UsedScrapes           uint64 `json:"used_scrapes"`
	ScrapesPerCycle       uint64 `json:"scrapes_per_cycle"`
	ScrapeIntervalSeconds uint64 `json:"scrape_interval_seconds"`
	ScrapesExhausted      bool   `json:"scrapes_exhausted"`
	// Requested intervals; zero means automatic spreading.
	RequestedCheckIntervalSeconds  uint32 `json:"requested_check_interval_seconds"`
	RequestedScrapeIntervalSeconds uint32 `json:"requested_scrape_interval_seconds"`
	MinimumCheckIntervalSeconds    uint64 `json:"minimum_check_interval_seconds"`
	MinimumScrapeIntervalSeconds   uint64 `json:"minimum_scrape_interval_seconds"`
	// Usage over a full month at the current cadence, and when a fixed
	// interval uses up the remaining allowance (absent if it lasts).
	ProjectedMonthlyChecks  uint64     `json:"projected_monthly_checks"`
	ChecksRunOutAt          *time.Time `json:"checks_run_out_at,omitempty"`
	ProjectedMonthlyScrapes uint64     `json:"projected_monthly_scrapes"`
	ScrapesRunOutAt         *time.Time `json:"scrapes_run_out_at,omitempty"`
	InheritsChecks          bool       `json:"inherits_checks"`
	InheritsScrapes         bool       `json:"inherits_scrapes"`
	PeriodStart             time.Time  `json:"period_start"`
	PeriodEnd               time.Time  `json:"period_end"`
	ComputedAt              *time.Time `json:"computed_at,omitempty"`
}

// WorkspaceCadenceUpdateRequest sets a workspace's requested intervals in
// seconds. Zero spreads the monthly allowance automatically.
type WorkspaceCadenceUpdateRequest struct {
	CheckIntervalSeconds  uint32 `json:"check_interval_seconds"`
	ScrapeIntervalSeconds uint32 `json:"scrape_interval_seconds"`
}

// WorkspaceAllowanceUpdateRequest sets a workspace's own allowances. Zero
// inherits the instance default. Unlimited removes the limit and ignores the
// matching monthly value.
type WorkspaceAllowanceUpdateRequest struct {
	MonthlyChecks    uint64 `json:"monthly_checks"`
	MonthlyScrapes   uint64 `json:"monthly_scrapes"`
	UnlimitedChecks  bool   `json:"unlimited_checks"`
	UnlimitedScrapes bool   `json:"unlimited_scrapes"`
}

type WorkspaceSubscription struct {
	PlanCode                    string     `json:"plan_code"`
	Status                      string     `json:"status"`
	IncludedOperators           uint32     `json:"included_operators"`
	StatisticsRetentionDays     uint32     `json:"statistics_retention_days"`
	MinimumCheckIntervalSeconds uint32     `json:"minimum_check_interval_seconds"`
	MonthlyChecks               uint64     `json:"monthly_checks"`
	MonthlyScrapes              uint64     `json:"monthly_scrapes"`
	UnlimitedChecks             bool       `json:"unlimited_checks"`
	UnlimitedScrapes            bool       `json:"unlimited_scrapes"`
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
	Allowances   WorkspaceAllowances   `json:"allowances"`
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

type WorkspaceMemberUpdateRequest struct {
	Role         string `json:"role"`
	BillingAdmin bool   `json:"billing_admin"`
}

type WorkspaceInvitation struct {
	ID                 uint      `json:"id"`
	WorkspaceID        uint      `json:"workspace_id"`
	WorkspaceName      string    `json:"workspace_name"`
	InviteeUserID      uint      `json:"invitee_user_id"`
	InviteeEmail       string    `json:"invitee_email"`
	InviterUserID      *uint     `json:"inviter_user_id,omitempty"`
	InviterEmail       string    `json:"inviter_email"`
	Role               string    `json:"role"`
	BillingAdmin       bool      `json:"billing_admin"`
	NotificationStatus string    `json:"notification_status"`
	ExpiresAt          time.Time `json:"expires_at"`
	CreatedAt          time.Time `json:"created_at"`
	UpdatedAt          time.Time `json:"updated_at"`
}

type WorkspaceInvitationCreateRequest struct {
	Email        string `json:"email"`
	Role         string `json:"role"`
	BillingAdmin bool   `json:"billing_admin"`
}

type WorkspaceInvitationUpdateRequest struct {
	Role         string `json:"role"`
	BillingAdmin bool   `json:"billing_admin"`
}

type WorkspaceInvitationResponse struct {
	Invitation WorkspaceInvitation `json:"invitation"`
	Warning    string              `json:"warning,omitempty"`
}

type WorkspaceInvitationAcceptance struct {
	WorkspaceID   uint   `json:"workspace_id"`
	WorkspaceName string `json:"workspace_name"`
}

type ManagedProxyLifecycleRequest struct {
	State string `json:"state"`
}
