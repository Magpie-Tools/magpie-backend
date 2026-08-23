package dto

type ProxyFilterOptions struct {
	Countries       []string   `json:"countries"`
	Types           []string   `json:"types"`
	AnonymityLevels []string   `json:"anonymityLevels"`
	Tags            []ProxyTag `json:"tags"`
}
