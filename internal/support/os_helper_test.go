package support

import "testing"

func TestGetEnv(t *testing.T) {
	t.Setenv("MAGPIE_TEST_ENV", "value")
	if got := GetEnv("MAGPIE_TEST_ENV", "fallback"); got != "value" {
		t.Fatalf("GetEnv returned %s, want value", got)
	}

	if got := GetEnv("MAGPIE_TEST_ENV_MISSING", "fallback"); got != "fallback" {
		t.Fatalf("GetEnv returned %s, want fallback", got)
	}
}

func TestHashStringDeterministic(t *testing.T) {
	if got1, got2 := HashString("input"), HashString("input"); got1 != got2 {
		t.Fatal("HashString returned different values for the same input")
	}

	if HashString("input") == HashString("different") {
		t.Fatal("HashString returned same value for different inputs")
	}
}
