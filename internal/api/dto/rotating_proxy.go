package dto

import "time"

type RotatingProxy struct {
	ID              uint64     `json:"id"`
	Name            string     `json:"name"`
	Protocol        string     `json:"protocol"`
	AliveProxyCount int        `json:"alive_proxy_count"`
	ListenPort      uint16     `json:"listen_port"`
	AuthRequired    bool       `json:"auth_required"`
	AuthUsername    string     `json:"auth_username,omitempty"`
	AuthPassword    string     `json:"auth_password,omitempty"`
	ListenHost      string     `json:"listen_host,omitempty"`
	ListenAddress   string     `json:"listen_address,omitempty"`
	LastRotationAt  *time.Time `json:"last_rotation_at,omitempty"`
	LastServedProxy string     `json:"last_served_proxy,omitempty"`
	CreatedAt       time.Time  `json:"created_at"`
}

type RotatingProxyCreateRequest struct {
	Name         string `json:"name"`
	Protocol     string `json:"protocol"`
	AuthRequired bool   `json:"auth_required"`
	AuthUsername string `json:"auth_username,omitempty"`
	AuthPassword string `json:"auth_password,omitempty"`
}

type RotatingProxyNext struct {
	ProxyID  uint64 `json:"proxy_id"`
	IP       string `json:"ip"`
	Port     uint16 `json:"port"`
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
	HasAuth  bool   `json:"has_auth"`
	Protocol string `json:"protocol"`
}
