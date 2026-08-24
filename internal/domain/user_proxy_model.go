package domain

import (
	"errors"
	"strings"
	"time"

	"magpie/internal/security"

	"gorm.io/gorm"
)

const (
	ManagedProxyStateActive   = "active"
	ManagedProxyStatePaused   = "paused"
	ManagedProxyStateArchived = "archived"

	ManagedProxyPauseReasonManual   = "manual"
	ManagedProxyPauseReasonCapacity = "capacity"
	ManagedProxyPauseReasonFailure  = "failure"
)

var ErrInvalidManagedProxyState = errors.New("invalid managed proxy state")

type ManagedProxy struct {
	WorkspaceID         uint   `gorm:"column:workspace_id;primaryKey;index:idx_managed_proxies_workspace_state,priority:1"`
	ProxyID             uint64 `gorm:"primaryKey;index:idx_user_proxies_proxy_id"`
	Username            string `gorm:"-"`
	Password            string `gorm:"-"`
	UsernameEncrypted   string `gorm:"column:username;default:''" json:"-"`
	PasswordEncrypted   string `gorm:"column:password;default:''" json:"-"`
	ConsecutiveFailures uint16 `gorm:"not null;default:0"`
	State               string `gorm:"not null;size:16;default:'active';index:idx_managed_proxies_workspace_state,priority:2"`
	PauseReason         string `gorm:"not null;size:24;default:''"`
	ActivatedAt         *time.Time
	PausedAt            *time.Time
	ArchivedAt          *time.Time
	CreatedAt           time.Time `gorm:"autoCreateTime"`
	UpdatedAt           time.Time `gorm:"autoUpdateTime"`

	TagAssignments []ProxyTagAssignment `gorm:"foreignKey:WorkspaceID,ProxyID;references:WorkspaceID,ProxyID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;" json:"-"`
}

func (access *ManagedProxy) BeforeSave(_ *gorm.DB) error {
	access.State = strings.ToLower(strings.TrimSpace(access.State))
	if access.State == "" {
		access.State = ManagedProxyStateActive
	}
	switch access.State {
	case ManagedProxyStateActive, ManagedProxyStatePaused, ManagedProxyStateArchived:
	default:
		return ErrInvalidManagedProxyState
	}

	username, err := security.EncryptProxySecret(access.Username)
	if err != nil {
		return err
	}
	password, err := security.EncryptProxySecret(access.Password)
	if err != nil {
		return err
	}

	access.UsernameEncrypted = username
	access.PasswordEncrypted = password
	return nil
}

func (access *ManagedProxy) AfterFind(_ *gorm.DB) error {
	username, _, err := security.DecryptProxySecret(access.UsernameEncrypted)
	if err != nil {
		return err
	}
	password, _, err := security.DecryptProxySecret(access.PasswordEncrypted)
	if err != nil {
		return err
	}

	access.Username = username
	access.Password = password
	return nil
}

func (ManagedProxy) TableName() string {
	return "user_proxies"
}

// UserProxy is kept as a source-compatible type name for integrations that
// compile against Magpie's Go packages. New code should use ManagedProxy.
type UserProxy = ManagedProxy
