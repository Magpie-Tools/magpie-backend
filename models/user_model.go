package models

type User struct {
	ID       uint   `gorm:"primaryKey;autoIncrement"`
	Email    string `gorm:"uniqueIndex;not null;size:255"` // Max length of 255 characters
	Password string `gorm:"not null;size:100" json:"-"`    // Max length of 100 characters, exclude from JSON responses
	Role     string `gorm:"not null;default:'user';check:role IN ('user', 'admin')"`
}
