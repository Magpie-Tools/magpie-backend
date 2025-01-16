package models

import "fmt"

type Proxy struct {
	IP       string
	Port     int
	Username string
	Password string
}

func (proxy Proxy) GetFullProxy() string {
	return fmt.Sprintf("%s:%d", proxy.Username, proxy.Port)
}
