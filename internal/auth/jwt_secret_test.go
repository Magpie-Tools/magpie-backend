package auth

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func TestAutomaticJWTSecretSurvivesRestartAndRejectsPublishedKey(t *testing.T) {
	for _, configured := range []string{"", legacyComposeJWTSecret} {
		t.Run(configured, func(t *testing.T) {
			resetJWTStateForTests(t)
			t.Chdir(t.TempDir())
			t.Setenv(jwtSecretEnv, configured)
			token, err := GenerateJWT(7, "admin")
			if err != nil {
				t.Fatal(err)
			}
			stored, err := os.ReadFile("data/jwt_secret")
			if err != nil || len(stored) != 65 {
				t.Fatalf("stored key bytes=%d err=%v", len(stored), err)
			}
			info, err := os.Stat("data/jwt_secret")
			if err != nil || info.Mode().Perm() != 0600 {
				t.Fatalf("secret permissions: %v %v", info, err)
			}
			jwtSecretOnce, jwtKey, jwtKeyErr = sync.Once{}, nil, nil
			if _, err := parseSignedJWT(token); err != nil {
				t.Fatalf("restart invalidated token: %v", err)
			}
			forged, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"user_id": 7, "role": "admin", "exp": time.Now().Add(time.Hour).Unix()}).SignedString([]byte(legacyComposeJWTSecret))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := parseSignedJWT(forged); err == nil {
				t.Fatal("accepted a token signed with the published secret")
			}
		})
	}
}

func TestJWTSecretConcurrentCreationUsesOneCompleteKey(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data", "jwt_secret")
	keys := make(chan string, 16)
	var workers sync.WaitGroup
	for i := 0; i < cap(keys); i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			key, err := loadOrCreateJWTSecret(path)
			if err != nil {
				t.Error(err)
				return
			}
			keys <- key
		}()
	}
	workers.Wait()
	close(keys)
	var first string
	for key := range keys {
		if len(key) != 64 {
			t.Fatalf("partial key: %d bytes", len(key))
		}
		if first == "" {
			first = key
		}
		if key != first {
			t.Fatal("concurrent starts selected different keys")
		}
	}
	other, err := loadOrCreateJWTSecret(filepath.Join(t.TempDir(), "jwt_secret"))
	if err != nil || first == other {
		t.Fatalf("installations must have distinct keys: %v", err)
	}
}

func TestCustomJWTSecretDoesNotCreateAutomaticKey(t *testing.T) {
	resetJWTStateForTests(t)
	t.Chdir(t.TempDir())
	secret := "custom-secret-kept-across-upgrades-123456789"
	t.Setenv(jwtSecretEnv, secret)
	key, err := jwtSigningKey()
	if err != nil || string(key) != secret {
		t.Fatalf("custom key changed: %v", err)
	}
	if _, err := os.Stat("data/jwt_secret"); !os.IsNotExist(err) {
		t.Fatalf("automatic file created for custom key: %v", err)
	}
}
