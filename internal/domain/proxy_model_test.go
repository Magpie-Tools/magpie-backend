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
	proxy1 := Proxy{Port: 8080, Username: "User", Password: "Secret"}
	if err := proxy1.SetIP("10.0.0.1"); err != nil {
		t.Fatalf("SetIP returned error: %v", err)
	}

	proxy1.GenerateHash()
	if len(proxy1.Hash) != 32 {
		t.Fatalf("GenerateHash produced hash with length %d, want 32", len(proxy1.Hash))
	}

	hashCopy := append([]byte(nil), proxy1.Hash...)

	proxy2 := Proxy{Port: 8080, Username: "user", Password: "secret"}
	if err := proxy2.SetIP("10.0.0.1"); err != nil {
		t.Fatalf("SetIP returned error: %v", err)
	}
	proxy2.GenerateHash()

	if !bytes.Equal(hashCopy, proxy2.Hash) {
		t.Fatal("GenerateHash should ignore username/password casing differences")
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

func TestProxyBeforeSaveEncryptsAndAfterFindDecrypts(t *testing.T) {
	t.Setenv("PROXY_ENCRYPTION_KEY", "unit-test-encryption-key")
	security.ResetProxyCipherForTests()

	proxy := Proxy{Port: 8080, Username: "user", Password: "secret"}

	if err := proxy.BeforeSave(nil); err != nil {
		t.Fatalf("BeforeSave returned error: %v", err)
	}

	if proxy.PasswordEncrypted == "" {
		t.Fatal("BeforeSave did not populate PasswordEncrypted")
	}
	if !security.IsProxySecretEncrypted(proxy.PasswordEncrypted) {
		t.Fatalf("PasswordEncrypted %q does not have encryption prefix", proxy.PasswordEncrypted)
	}

	decrypted := Proxy{PasswordEncrypted: proxy.PasswordEncrypted}
	if err := decrypted.AfterFind(nil); err != nil {
		t.Fatalf("AfterFind returned error: %v", err)
	}
	if decrypted.Password != "secret" {
		t.Fatalf("AfterFind returned password %q, want secret", decrypted.Password)
	}
}
