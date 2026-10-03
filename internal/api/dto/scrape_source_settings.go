package dto

type ScrapeSourceSettings struct {
	FetchMode  *string   `json:"fetch_mode,omitempty"`
	AutoTagIDs *[]uint64 `json:"auto_tag_ids,omitempty"`
}
