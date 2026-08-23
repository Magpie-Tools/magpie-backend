package dto

type AddProxiesDetails struct {
	SubmittedCount      int `json:"submittedCount"`
	ParsedCount         int `json:"parsedCount"`
	InvalidFormatCount  int `json:"invalidFormatCount"`
	InvalidAddressCount int `json:"invalidAddressCount"`
	// InvalidIPCount is retained as an alias for InvalidAddressCount.
	InvalidIPCount int `json:"invalidIpCount"`
	// InvalidIPv4Count is kept for response compatibility. Manual imports accept IPv6.
	InvalidIPv4Count int   `json:"invalidIpv4Count"`
	InvalidPortCount int   `json:"invalidPortCount"`
	BlacklistedCount int   `json:"blacklistedCount"`
	ProcessingMs     int64 `json:"processingMs"`
}

type AddProxiesResponse struct {
	ProxyCount int               `json:"proxyCount"`
	Details    AddProxiesDetails `json:"details"`
}
