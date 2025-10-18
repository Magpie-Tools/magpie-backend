package support

import "sync"

const (
	defaultRotatingProxyPortStart = 20000
	defaultRotatingProxyPortEnd   = 20100
	envPortStart                  = "ROTATING_PROXY_PORT_START"
	envPortEnd                    = "ROTATING_PROXY_PORT_END"
)

var (
	portRangeOnce sync.Once
	portStart     int
	portEnd       int
)

func loadRotatingProxyPortRange() {
	start := GetEnvInt(envPortStart, defaultRotatingProxyPortStart)
	end := GetEnvInt(envPortEnd, defaultRotatingProxyPortEnd)

	if start <= 0 {
		start = defaultRotatingProxyPortStart
	}
	if end <= 0 {
		end = defaultRotatingProxyPortEnd
	}
	if end < start {
		start, end = end, start
	}

	portStart = start
	portEnd = end
}

func GetRotatingProxyPortRange() (int, int) {
	portRangeOnce.Do(loadRotatingProxyPortRange)
	return portStart, portEnd
}
