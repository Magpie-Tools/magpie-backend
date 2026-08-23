package domain

import (
	"errors"
	"strings"
	"testing"
)

func TestProxyTagNormalize(t *testing.T) {
	tag := ProxyTag{Name: "  Premium   Europe  ", Color: "#22c55e"}
	if err := tag.Normalize(); err != nil {
		t.Fatalf("Normalize() error = %v", err)
	}
	if tag.Name != "Premium Europe" || tag.NameKey != "premium europe" {
		t.Fatalf("normalized name/key = %q/%q", tag.Name, tag.NameKey)
	}
	if tag.Color != "#22C55E" {
		t.Fatalf("normalized color = %q, want #22C55E", tag.Color)
	}
}

func TestProxyTagNormalizeRejectsInvalidValues(t *testing.T) {
	if err := (&ProxyTag{Name: " ", Color: ProxyTagDefaultColor}).Normalize(); !errors.Is(err, ErrProxyTagNameRequired) {
		t.Fatalf("blank name error = %v", err)
	}
	if err := (&ProxyTag{Name: strings.Repeat("a", ProxyTagNameMaxLength+1), Color: ProxyTagDefaultColor}).Normalize(); !errors.Is(err, ErrProxyTagNameTooLong) {
		t.Fatalf("long name error = %v", err)
	}
	if err := (&ProxyTag{Name: "Valid", Color: "green"}).Normalize(); !errors.Is(err, ErrProxyTagColorInvalid) {
		t.Fatalf("invalid color error = %v", err)
	}
}
