package dto

import "time"

type ProxyInfoRow struct {
	Id             int       `gorm:"column:id"`
	IPEncrypted    string    `gorm:"column:ip_encrypted"`
	Port           uint16    `gorm:"column:port"`
	EstimatedType  string    `gorm:"column:estimated_type"`
	ResponseTime   uint16    `gorm:"column:response_time"`
	Country        string    `gorm:"column:country"`
	AnonymityLevel string    `gorm:"column:anonymity_level"`
	Protocol       string    `gorm:"column:protocol"`
	Alive          bool      `gorm:"column:alive"`
	LatestCheck    time.Time `gorm:"column:latest_check"`
}
