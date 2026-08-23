package server

import "testing"

func TestFormatRotatingProxyListenAddress(t *testing.T) {
	testCases := map[string]string{
		"127.0.0.1":     "127.0.0.1:19000",
		"2001:db8::1":   "[2001:db8::1]:19000",
		"[2001:db8::1]": "[2001:db8::1]:19000",
		"":              "19000",
	}

	for host, expected := range testCases {
		if got := formatRotatingProxyListenAddress(host, 19000); got != expected {
			t.Errorf("formatRotatingProxyListenAddress(%q) = %q, want %q", host, got, expected)
		}
	}
}
