package redis_queue

import (
	"context"
	"fmt"
	"github.com/charmbracelet/log"
	"os"
	"time"
)

var instanceID = generateInstanceID() // Implement a function to generate a unique ID

func generateInstanceID() string {
	hostname, _ := os.Hostname()
	return fmt.Sprintf("%s-%d-%d", hostname, os.Getpid(), time.Now().UnixNano())
}

func startInstanceHeartbeat() {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		key := "magpie:instance:" + instanceID
		// Set key with 30s TTL, renew every 15s
		err := PublicProxyQueue.client.SetEx(context.Background(), key, "alive", 30*time.Second).Err()
		if err != nil {
			log.Error("Failed to update instance heartbeat", "error", err)
		}
		<-ticker.C
	}
}
