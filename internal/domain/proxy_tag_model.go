package domain

import (
	"errors"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"gorm.io/gorm"
)

const (
	ProxyTagNameMaxLength = 40
	ProxyTagDefaultColor  = "#64748B"
)

var (
	ErrProxyTagNameRequired = errors.New("proxy tag name is required")
	ErrProxyTagNameTooLong  = errors.New("proxy tag name is too long")
	ErrProxyTagColorInvalid = errors.New("proxy tag color must use #RRGGBB")
	proxyTagColorPattern    = regexp.MustCompile(`^#[0-9A-Fa-f]{6}$`)
)

type ProxyTag struct {
	ID      uint64 `gorm:"primaryKey;autoIncrement"`
	UserID  uint   `gorm:"not null;index;uniqueIndex:idx_proxy_tags_user_name,priority:1" json:"-"`
	Name    string `gorm:"size:40;not null"`
	NameKey string `gorm:"size:40;not null;uniqueIndex:idx_proxy_tags_user_name,priority:2" json:"-"`
	Color   string `gorm:"size:7;not null;default:'#64748B'"`

	User        User                 `gorm:"constraint:OnUpdate:CASCADE,OnDelete:CASCADE;" json:"-"`
	Assignments []ProxyTagAssignment `gorm:"foreignKey:ProxyTagID;references:ID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;" json:"-"`

	CreatedAt time.Time `gorm:"autoCreateTime"`
	UpdatedAt time.Time `gorm:"autoUpdateTime"`
}

func (tag *ProxyTag) BeforeSave(_ *gorm.DB) error {
	return tag.Normalize()
}

func (tag *ProxyTag) Normalize() error {
	name := strings.Join(strings.Fields(strings.TrimSpace(tag.Name)), " ")
	if name == "" {
		return ErrProxyTagNameRequired
	}
	if utf8.RuneCountInString(name) > ProxyTagNameMaxLength {
		return ErrProxyTagNameTooLong
	}

	color := strings.ToUpper(strings.TrimSpace(tag.Color))
	if color == "" {
		color = ProxyTagDefaultColor
	}
	if !proxyTagColorPattern.MatchString(color) {
		return ErrProxyTagColorInvalid
	}

	tag.Name = name
	tag.NameKey = strings.ToLower(name)
	tag.Color = color
	return nil
}

type ProxyTagAssignment struct {
	UserID     uint   `gorm:"primaryKey;index:idx_proxy_tag_assignments_user_tag,priority:1"`
	ProxyID    uint64 `gorm:"primaryKey"`
	ProxyTagID uint64 `gorm:"primaryKey;index:idx_proxy_tag_assignments_user_tag,priority:2"`

	CreatedAt time.Time `gorm:"autoCreateTime"`
}

func (ProxyTagAssignment) TableName() string {
	return "proxy_tag_assignments"
}
