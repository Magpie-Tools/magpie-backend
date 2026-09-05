package scraper

import (
	"errors"
	"magpie/internal/support"
	"testing"
)

func TestValidateScrapeRemoteIP_RejectsPrivateAddresses(t *testing.T) {
	err := validateScrapeRemoteIP("127.0.0.1")
	if !errors.Is(err, support.ErrUnsafeOutboundTarget) {
		t.Fatalf("validateScrapeRemoteIP err = %v, want ErrUnsafeOutboundTarget", err)
	}
}
