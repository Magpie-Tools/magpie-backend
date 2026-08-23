package dto

type ProxyTag struct {
	ID    uint64 `json:"id"`
	Name  string `json:"name"`
	Color string `json:"color"`
}

type ProxyTagWriteRequest struct {
	Name  string `json:"name"`
	Color string `json:"color"`
}

type ProxyTagAssignmentRequest struct {
	TagIDs []uint64 `json:"tagIds"`
}

type ProxyTagAssignmentResponse struct {
	Tags []ProxyTag `json:"tags"`
}
