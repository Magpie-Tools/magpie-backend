package auth

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const legacyComposeJWTSecret = "magpie-local-compose-jwt-secret-2026"

// Publish a complete file atomically without replacing another process's key.
// A persistent data volume keeps automatically generated keys stable on restart.
func loadOrCreateJWTSecret(path string) (string, error) {
	read := func() (string, error) {
		data, err := os.ReadFile(path)
		if err != nil {
			return "", err
		}
		secret := strings.TrimSpace(string(data))
		if len(secret) < jwtSecretMinLength || secret == legacyComposeJWTSecret {
			return "", fmt.Errorf("invalid stored JWT secret in %s", path)
		}
		if err := os.Chmod(path, 0600); err != nil {
			return "", fmt.Errorf("protect stored JWT secret: %w", err)
		}
		return secret, nil
	}
	if secret, err := read(); err == nil {
		return secret, nil
	} else if !os.IsNotExist(err) {
		return "", err
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return "", fmt.Errorf("create JWT secret directory: %w", err)
	}
	if err := os.Chmod(dir, 0700); err != nil {
		return "", fmt.Errorf("protect JWT secret directory: %w", err)
	}
	var random [32]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", fmt.Errorf("generate JWT secret: %w", err)
	}
	secret := hex.EncodeToString(random[:])
	file, err := os.CreateTemp(dir, ".jwt-secret-*")
	if err != nil {
		return "", err
	}
	defer os.Remove(file.Name())
	defer file.Close()
	if _, err := file.WriteString(secret + "\n"); err != nil {
		return "", err
	}
	if err := file.Sync(); err != nil {
		return "", err
	}
	if err := file.Close(); err != nil {
		return "", err
	}
	if err := os.Link(file.Name(), path); err != nil && !os.IsExist(err) {
		return "", fmt.Errorf("persist JWT secret: %w", err)
	}
	return read()
}
