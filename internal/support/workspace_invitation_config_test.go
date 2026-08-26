package support

import (
	"testing"
	"time"
)

func TestReadWorkspaceInvitationConfigDefaultsAndBounds(t *testing.T) {
	t.Setenv(envPublicAppURL, "https://magpie.example/base")
	t.Setenv(envWorkspaceInvitationTTLHours, "")

	cfg, err := ReadWorkspaceInvitationConfig()
	if err != nil {
		t.Fatalf("read default config: %v", err)
	}
	if cfg.TTL != 7*24*time.Hour {
		t.Fatalf("default TTL = %v, want 168h", cfg.TTL)
	}
	url, err := BuildWorkspaceInvitationsURL(cfg)
	if err != nil {
		t.Fatalf("build invitations URL: %v", err)
	}
	if url != "https://magpie.example/base/invitations" {
		t.Fatalf("invitations URL = %q", url)
	}

	t.Setenv(envWorkspaceInvitationTTLHours, "0")
	if _, err := ReadWorkspaceInvitationConfig(); err == nil {
		t.Fatal("zero TTL unexpectedly accepted")
	}
	t.Setenv(envWorkspaceInvitationTTLHours, "721")
	if _, err := ReadWorkspaceInvitationConfig(); err == nil {
		t.Fatal("TTL above 30 days unexpectedly accepted")
	}
}

func TestRequireWorkspaceInvitationConfigNeedsPublicURLForConfiguredMail(t *testing.T) {
	t.Setenv(envWorkspaceInvitationTTLHours, "")
	t.Setenv(envPublicAppURL, "")
	t.Setenv(envMailFromAddress, "magpie@example.test")
	t.Setenv(envMailFromName, "Magpie")
	t.Setenv(envSMTPHost, "smtp.example.test")
	t.Setenv(envSMTPPort, "587")
	t.Setenv(envSMTPUsername, "")
	t.Setenv(envSMTPPassword, "")

	if err := RequireWorkspaceInvitationConfigValid(); err == nil {
		t.Fatal("configured invitation email unexpectedly accepted without PUBLIC_APP_URL")
	}

	t.Setenv(envPublicAppURL, "https://magpie.example")
	if err := RequireWorkspaceInvitationConfigValid(); err != nil {
		t.Fatalf("valid invitation configuration rejected: %v", err)
	}
}
