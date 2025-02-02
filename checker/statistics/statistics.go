package statistics

import (
	"sync/atomic"
)

var (
	proxyCount atomic.Int64
)

func GetProxyCount() int64 {
	return proxyCount.Load()
}

func SetProxyCount(count int64) {
	proxyCount.Store(count)
}

func IncreaseProxyCount(count int64) {
	proxyCount.Add(count)
}
