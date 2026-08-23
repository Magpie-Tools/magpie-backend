package server

import (
	"reflect"
	"testing"
)

func TestParseStrictProxyTagIDs(t *testing.T) {
	ids, err := parseStrictProxyTagIDs([]string{"3", "2,3", "7"})
	if err != nil {
		t.Fatalf("parse tag IDs: %v", err)
	}
	if !reflect.DeepEqual(ids, []uint64{3, 2, 7}) {
		t.Fatalf("tag IDs = %#v, want [3 2 7]", ids)
	}

	if _, err := parseStrictProxyTagIDs([]string{"4", "nope"}); err == nil {
		t.Fatal("expected malformed tag ID to fail")
	}
	if _, err := parseStrictProxyTagIDs([]string{"0"}); err == nil {
		t.Fatal("expected zero tag ID to fail")
	}
}
