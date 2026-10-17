package domain

import (
	"strconv"
	"strings"
	"time"
)

// NotificationRead stores an account's first read of an event, independently of
// the selected workspace or browser. Event IDs are namespaced by their source.
type NotificationRead struct {
	UserID         uint      `gorm:"primaryKey;autoIncrement:false"`
	NotificationID string    `gorm:"primaryKey;size:64"`
	ReadAt         time.Time `gorm:"not null;autoCreateTime"`
	User           User      `gorm:"constraint:OnDelete:CASCADE" json:"-"`
}

func ParseNotificationID(value string) (kind string, eventID uint64, ok bool) {
	kind, rawID, found := strings.Cut(value, "-")
	if !found || (kind != "invitation" && kind != "alert" && kind != "release") {
		return "", 0, false
	}
	// PostgreSQL IDs use signed bigint. Require one canonical spelling so a
	// second key cannot make a read event appear unread again.
	id, err := strconv.ParseUint(rawID, 10, 63)
	if err != nil || id == 0 || strconv.FormatUint(id, 10) != rawID {
		return "", 0, false
	}
	return kind, id, true
}
