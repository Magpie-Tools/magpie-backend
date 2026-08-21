package domain

import (
	"time"

	"magpie/internal/security"

	"gorm.io/gorm"
)

type UserProxy struct {
	UserID              uint      `gorm:"primaryKey"`
	ProxyID             uint64    `gorm:"primaryKey;index:idx_user_proxies_proxy_id"`
	Username            string    `gorm:"-"`
	Password            string    `gorm:"-"`
	UsernameEncrypted   string    `gorm:"column:username;default:''" json:"-"`
	PasswordEncrypted   string    `gorm:"column:password;default:''" json:"-"`
	ConsecutiveFailures uint16    `gorm:"not null;default:0"`
	CreatedAt           time.Time `gorm:"autoCreateTime"`
}

func (access *UserProxy) BeforeSave(_ *gorm.DB) error {
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

func (access *UserProxy) AfterFind(_ *gorm.DB) error {
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

func (UserProxy) TableName() string {
	return "user_proxies"
}
