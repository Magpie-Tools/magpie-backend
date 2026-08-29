package database

import "testing"

func TestNormalizeScrapeSitePage(t *testing.T) {
	tests := []struct {
		name         string
		page         int
		pageSize     int
		wantPage     int
		wantPageSize int
	}{
		{name: "keeps requested values", page: 3, pageSize: 100, wantPage: 3, wantPageSize: 100},
		{name: "uses default size when omitted", page: 1, pageSize: 0, wantPage: 1, wantPageSize: 40},
		{name: "uses default size above maximum", page: 1, pageSize: 101, wantPage: 1, wantPageSize: 40},
		{name: "normalizes invalid page", page: 0, pageSize: 20, wantPage: 1, wantPageSize: 20},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			page, pageSize := normalizeScrapeSitePage(tt.page, tt.pageSize)
			if page != tt.wantPage || pageSize != tt.wantPageSize {
				t.Fatalf("normalizeScrapeSitePage(%d, %d) = (%d, %d), want (%d, %d)", tt.page, tt.pageSize, page, pageSize, tt.wantPage, tt.wantPageSize)
			}
		})
	}
}
