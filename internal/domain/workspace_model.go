package domain

import (
	"errors"
	"strings"
	"time"

	"magpie/internal/api/dto"
)

const (
	WorkspaceRoleOwner    = "owner"
	WorkspaceRoleAdmin    = "admin"
	WorkspaceRoleOperator = "operator"
	WorkspaceRoleViewer   = "viewer"

	WorkspaceSubscriptionStatusActive   = "active"
	WorkspaceSubscriptionStatusTrialing = "trialing"
	WorkspaceSubscriptionStatusPastDue  = "past_due"
	WorkspaceSubscriptionStatusCanceled = "canceled"

	WorkspaceOverageDisabled  = "disabled"
	WorkspaceOverageAllowed   = "allowed"
	WorkspaceOverageUnlimited = "unlimited"
)

var (
	ErrInvalidWorkspaceRole        = errors.New("invalid workspace role")
	ErrInvalidWorkspaceOverageMode = errors.New("invalid workspace overage mode")
)

// Workspace is the ownership boundary for operational resources and settings.
// Global administrator privileges remain on User and do not imply workspace membership.
type Workspace struct {
	ID       uint   `gorm:"primaryKey;autoIncrement"`
	Name     string `gorm:"not null;size:120"`
	Personal bool   `gorm:"not null;default:false;index"`

	HTTPProtocol               bool   `gorm:"not null;default:false"`
	HTTPSProtocol              bool   `gorm:"not null;default:true"`
	SOCKS4Protocol             bool   `gorm:"not null;default:false"`
	SOCKS5Protocol             bool   `gorm:"not null;default:false"`
	Timeout                    uint16 `gorm:"not null;default:7500"`
	Retries                    uint8  `gorm:"not null;default:2"`
	UseHttpsForSocks           bool   `gorm:"not null;default:true"`
	TransportProtocol          string `gorm:"not null;default:'tcp'"`
	AutoRemoveFailingProxies   bool   `gorm:"not null;default:false"`
	AutoRemoveFailureThreshold uint8  `gorm:"not null;default:3"`

	Memberships  []WorkspaceMembership `gorm:"foreignKey:WorkspaceID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;" json:"-"`
	Subscription WorkspaceSubscription `gorm:"foreignKey:WorkspaceID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;" json:"-"`
	Proxies      []Proxy               `gorm:"many2many:user_proxies;joinForeignKey:WorkspaceID;joinReferences:ProxyID;" json:"-"`
	Judges       []Judge               `gorm:"many2many:user_judges;joinForeignKey:WorkspaceID;joinReferences:JudgeID;" json:"-"`
	ScrapeSites  []ScrapeSite          `gorm:"many2many:user_scrape_site;joinForeignKey:WorkspaceID;joinReferences:ScrapeSiteID;" json:"-"`

	CreatedAt time.Time `gorm:"autoCreateTime"`
	UpdatedAt time.Time `gorm:"autoUpdateTime"`
}

func (workspace Workspace) GetProtocolMap() map[string]int {
	protocols := make(map[string]int)
	if workspace.HTTPProtocol {
		protocols["http"] = 1
	}
	if workspace.HTTPSProtocol {
		protocols["https"] = 2
	}
	if workspace.SOCKS4Protocol {
		protocols["socks4"] = 3
	}
	if workspace.SOCKS5Protocol {
		protocols["socks5"] = 4
	}
	return protocols
}

func (workspace Workspace) ToUserSettings(judges []dto.SimpleUserJudge, sources []string, preference WorkspaceMemberPreference) dto.UserSettings {
	return dto.UserSettings{
		HTTPProtocol:               workspace.HTTPProtocol,
		HTTPSProtocol:              workspace.HTTPSProtocol,
		SOCKS4Protocol:             workspace.SOCKS4Protocol,
		SOCKS5Protocol:             workspace.SOCKS5Protocol,
		Timeout:                    workspace.Timeout,
		Retries:                    workspace.Retries,
		UseHttpsForSocks:           workspace.UseHttpsForSocks,
		TransportProtocol:          workspace.TransportProtocol,
		AutoRemoveFailingProxies:   workspace.AutoRemoveFailingProxies,
		AutoRemoveFailureThreshold: workspace.AutoRemoveFailureThreshold,
		SimpleUserJudges:           judges,
		ScrapingSources:            sources,
		ProxyListColumns:           NormalizeProxyListColumns(preference.ProxyListColumns.Clone()),
		ScrapeSourceProxyColumns:   NormalizeScrapeSourceProxyColumns(preference.ScrapeSourceProxyColumns.Clone()),
		ScrapeSourceListColumns:    NormalizeScrapeSourceListColumns(preference.ScrapeSourceListColumns.Clone()),
	}
}

type WorkspaceMembership struct {
	WorkspaceID  uint   `gorm:"primaryKey;index:idx_workspace_memberships_user_default,priority:2"`
	UserID       uint   `gorm:"primaryKey;index;index:idx_workspace_memberships_user_default,priority:1"`
	Role         string `gorm:"not null;size:16;check:role IN ('owner','admin','operator','viewer')"`
	BillingAdmin bool   `gorm:"not null;default:false"`
	IsDefault    bool   `gorm:"not null;default:false;index:idx_workspace_memberships_user_default,priority:3"`

	Workspace Workspace `gorm:"constraint:OnUpdate:CASCADE,OnDelete:CASCADE;" json:"-"`
	User      User      `gorm:"constraint:OnUpdate:CASCADE,OnDelete:CASCADE;" json:"-"`

	CreatedAt time.Time `gorm:"autoCreateTime"`
	UpdatedAt time.Time `gorm:"autoUpdateTime"`
}

func (membership *WorkspaceMembership) Normalize() error {
	membership.Role = strings.ToLower(strings.TrimSpace(membership.Role))
	if !IsWorkspaceRole(membership.Role) {
		return ErrInvalidWorkspaceRole
	}
	if membership.Role == WorkspaceRoleOwner {
		membership.BillingAdmin = true
	}
	return nil
}

func IsWorkspaceRole(role string) bool {
	switch strings.ToLower(strings.TrimSpace(role)) {
	case WorkspaceRoleOwner, WorkspaceRoleAdmin, WorkspaceRoleOperator, WorkspaceRoleViewer:
		return true
	default:
		return false
	}
}

func WorkspaceRoleRank(role string) int {
	switch strings.ToLower(strings.TrimSpace(role)) {
	case WorkspaceRoleOwner:
		return 4
	case WorkspaceRoleAdmin:
		return 3
	case WorkspaceRoleOperator:
		return 2
	case WorkspaceRoleViewer:
		return 1
	default:
		return 0
	}
}

type WorkspaceMemberPreference struct {
	WorkspaceID uint `gorm:"primaryKey"`
	UserID      uint `gorm:"primaryKey;index"`

	ProxyListColumns         StringList `gorm:"type:jsonb;default:'[]'"`
	ScrapeSourceProxyColumns StringList `gorm:"type:jsonb;default:'[]'"`
	ScrapeSourceListColumns  StringList `gorm:"type:jsonb;default:'[]'"`

	CreatedAt time.Time `gorm:"autoCreateTime"`
	UpdatedAt time.Time `gorm:"autoUpdateTime"`
}

// WorkspaceSubscription stores an entitlement snapshot. A future billing
// provider can update it without changing resource ownership or membership.
type WorkspaceSubscription struct {
	WorkspaceID uint `gorm:"primaryKey"`

	PlanCode string `gorm:"not null;size:64;default:'self-hosted'"`
	Status   string `gorm:"not null;size:24;default:'active';index"`

	IncludedActiveRoutes        uint64 `gorm:"not null;default:0"`
	AdditionalActiveRoutes      uint64 `gorm:"not null;default:0"`
	OverageMode                 string `gorm:"not null;size:16;default:'unlimited'"`
	OverageActiveRoutes         uint64 `gorm:"not null;default:0"`
	IncludedOperators           uint32 `gorm:"not null;default:0"`
	StatisticsRetentionDays     uint32 `gorm:"not null;default:0"`
	MinimumCheckIntervalSeconds uint32 `gorm:"not null;default:0"`

	BillingProvider        string `gorm:"size:32;default:''" json:"-"`
	ProviderCustomerID     string `gorm:"size:191;default:''" json:"-"`
	ProviderSubscriptionID string `gorm:"size:191;default:''" json:"-"`
	CurrentPeriodStart     *time.Time
	CurrentPeriodEnd       *time.Time
	CancelAtPeriodEnd      bool `gorm:"not null;default:false"`

	CreatedAt time.Time `gorm:"autoCreateTime"`
	UpdatedAt time.Time `gorm:"autoUpdateTime"`
}

func (subscription *WorkspaceSubscription) Normalize() error {
	subscription.PlanCode = strings.TrimSpace(subscription.PlanCode)
	if subscription.PlanCode == "" {
		subscription.PlanCode = "self-hosted"
	}
	subscription.Status = strings.ToLower(strings.TrimSpace(subscription.Status))
	if subscription.Status == "" {
		subscription.Status = WorkspaceSubscriptionStatusActive
	}
	subscription.OverageMode = strings.ToLower(strings.TrimSpace(subscription.OverageMode))
	switch subscription.OverageMode {
	case WorkspaceOverageDisabled, WorkspaceOverageAllowed, WorkspaceOverageUnlimited:
		return nil
	default:
		return ErrInvalidWorkspaceOverageMode
	}
}

func (subscription WorkspaceSubscription) IncludedCapacity() uint64 {
	return subscription.IncludedActiveRoutes + subscription.AdditionalActiveRoutes
}

func (subscription WorkspaceSubscription) ActivationLimit() (uint64, bool) {
	switch subscription.OverageMode {
	case WorkspaceOverageUnlimited:
		return 0, true
	case WorkspaceOverageAllowed:
		return subscription.IncludedCapacity() + subscription.OverageActiveRoutes, false
	default:
		return subscription.IncludedCapacity(), false
	}
}

type WorkspaceUsagePeriod struct {
	WorkspaceID uint      `gorm:"primaryKey"`
	PeriodStart time.Time `gorm:"primaryKey"`
	PeriodEnd   time.Time `gorm:"not null;index"`

	ActiveRoutes     uint64 `gorm:"not null;default:0"`
	PeakActiveRoutes uint64 `gorm:"not null;default:0"`
	CheckAttempts    uint64 `gorm:"not null;default:0"`
	ManagedRequests  uint64 `gorm:"not null;default:0"`
	ManagedBytes     uint64 `gorm:"not null;default:0"`

	CreatedAt time.Time `gorm:"autoCreateTime"`
	UpdatedAt time.Time `gorm:"autoUpdateTime"`
}
