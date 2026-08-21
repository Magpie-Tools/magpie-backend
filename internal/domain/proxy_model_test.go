package domain

import (
	"bytes"
	"testing"

	"magpie/internal/security"
)

func TestProxySetIP(t *testing.T) {
	var proxy Proxy
	if err := proxy.SetIP("192.168.10.5"); err != nil {
		t.Fatalf("SetIP returned error: %v", err)
	}

	if got := proxy.GetIp(); got != "192.168.10.5" {
		t.Fatalf("GetIp returned %s, want 192.168.10.5", got)
	}

	if err := proxy.SetIP("not.an.ip"); err == nil {
		t.Fatal("expected error for invalid IP, got nil")
	}

	if err := proxy.SetIP("::1"); err == nil {
		t.Fatal("expected error for IPv6 address, got nil")
	}
}

func TestProxyGenerateHash(t *testing.T) {
	t.Setenv("PROXY_ENCRYPTION_KEY", "unit-test-encryption-key")
	security.ResetProxyCipherForTests()
	t.Cleanup(security.ResetProxyCipherForTests)

	proxy1 := Proxy{Port: 8080, Username: "User", Password: "Secret"}
	if err := proxy1.SetIP("10.0.0.1"); err != nil {
		t.Fatalf("SetIP returned error: %v", err)
	}

	if err := proxy1.GenerateHash(); err != nil {
		t.Fatalf("GenerateHash returned error: %v", err)
	}
	if len(proxy1.Hash) != 32 {
		t.Fatalf("GenerateHash produced hash with length %d, want 32", len(proxy1.Hash))
	}

	hashCopy := append([]byte(nil), proxy1.Hash...)

	proxy2 := Proxy{Port: 8080, Username: "user", Password: "secret"}
	if err := proxy2.SetIP("10.0.0.1"); err != nil {
		t.Fatalf("SetIP returned error: %v", err)
	}
	if err := proxy2.GenerateHash(); err != nil {
		t.Fatalf("GenerateHash returned error: %v", err)
	}

	if bytes.Equal(hashCopy, proxy2.Hash) {
		t.Fatal("GenerateHash must preserve username/password casing")
	}
}

func TestProxyGetters(t *testing.T) {
	proxy := Proxy{Port: 3128}
	if err := proxy.SetIP("8.8.8.8"); err != nil {
		t.Fatalf("SetIP returned error: %v", err)
	}
	proxy.Username = "name"
	proxy.Password = "pass"

	if got := proxy.GetFullProxy(); got != "8.8.8.8:3128" {
		t.Fatalf("GetFullProxy returned %s, want 8.8.8.8:3128", got)
	}

	if !proxy.HasAuth() {
		t.Fatal("HasAuth returned false for proxy with credentials")
	}

	proxy.Password = ""
	if proxy.HasAuth() {
		t.Fatal("HasAuth returned true when password missing")
	}
}

func TestUserProxyBeforeSaveEncryptsAndAfterFindDecrypts(t *testing.T) {
	t.Setenv("PROXY_ENCRYPTION_KEY", "unit-test-encryption-key")
	security.ResetProxyCipherForTests()
	t.Cleanup(security.ResetProxyCipherForTests)

	access := UserProxy{UserID: 7, ProxyID: 12, Username: "user", Password: "secret"}
	if err := access.BeforeSave(nil); err != nil {
		t.Fatalf("BeforeSave returned error: %v", err)
	}

	if access.UsernameEncrypted == "" {
		t.Fatal("BeforeSave did not populate UsernameEncrypted")
	}
	if !security.IsProxySecretEncrypted(access.UsernameEncrypted) {
		t.Fatalf("UsernameEncrypted %q does not have encryption prefix", access.UsernameEncrypted)
	}

	if access.PasswordEncrypted == "" {
		t.Fatal("BeforeSave did not populate PasswordEncrypted")
	}
	if !security.IsProxySecretEncrypted(access.PasswordEncrypted) {
		t.Fatalf("PasswordEncrypted %q does not have encryption prefix", access.PasswordEncrypted)
	}

	decrypted := UserProxy{
		UsernameEncrypted: access.UsernameEncrypted,
		PasswordEncrypted: access.PasswordEncrypted,
	}
	if err := decrypted.AfterFind(nil); err != nil {
		t.Fatalf("AfterFind returned error: %v", err)
	}
	if decrypted.Username != "user" {
		t.Fatalf("AfterFind returned username %q, want user", decrypted.Username)
	}
	if decrypted.Password != "secret" {
		t.Fatalf("AfterFind returned password %q, want secret", decrypted.Password)
	}
}
