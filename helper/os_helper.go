package helper

import (
	"crypto/sha1"
	"encoding/hex"
	"os"
	"strconv"
)

func GetEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}

func HashString(input string) uint64 {
	h := sha1.New()
	h.Write([]byte(input))
	hash := h.Sum(nil)

	// Use first 8 bytes of the hash to create a uint64
	hashStr := hex.EncodeToString(hash[:8])
	hashUint, _ := strconv.ParseUint(hashStr, 16, 64)
	return hashUint
}
