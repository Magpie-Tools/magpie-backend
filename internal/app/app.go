package app

import (
	"context"
	"flag"
	"fmt"
	"os"
	"runtime/debug"
	"strconv"
	"strings"

	"github.com/charmbracelet/log"
	"github.com/joho/godotenv"

	"magpie/internal/app/bootstrap"
	"magpie/internal/app/server"
	"magpie/internal/config"
	proxyqueue "magpie/internal/jobs/queue/proxy"
	sitequeue "magpie/internal/jobs/queue/sites"
	"magpie/internal/jobs/runtime"
	"magpie/internal/support"
)

const (
	defaultBackendPort  = 8082
	defaultFrontendPort = 8084
)

func Run() error {
	if err := godotenv.Load(); err != nil {
		log.Warn("No .env file found. Falling back to system environment variables.")
	}

	log.SetLevel(log.DebugLevel)
	debug.SetMaxThreads(9999999999)

	backendPortFlag := flag.Int("backend-port", defaultBackendPort, "Port for API server")
	frontendPortFlag := flag.Int("frontend-port", defaultFrontendPort, "Port for frontend static server")
	serveFEFlag := flag.Bool("serve-frontend", true, "Serve the Angular bundle on the API port")
	productionFlag := flag.Bool("production", false, "Run in production mode")
	flag.Parse()

	config.SetProductionMode(*productionFlag)

	backendPort := resolvePort("BACKEND_PORT", "backend-port", *backendPortFlag)
	frontendPort := resolvePort("FRONTEND_PORT", "frontend-port", *frontendPortFlag)

	serveFrontend := *serveFEFlag
	if v := os.Getenv("SERVE_FRONTEND"); strings.EqualFold(v, "false") {
		serveFrontend = false
	}

	redisClient, err := support.GetRedisClient()
	if err != nil {
		return fmt.Errorf("failed to get redis client: %w", err)
	}

	heartbeatCancel := runtime.LaunchInstanceHeartbeat(context.Background(), redisClient)
	defer heartbeatCancel()

	bootstrap.Setup()

	defer func() {
		if err := proxyqueue.PublicProxyQueue.Close(); err != nil {
			log.Warn("error closing proxy queue", "error", err)
		}
		if err := sitequeue.PublicScrapeSiteQueue.Close(); err != nil {
			log.Warn("error closing scrape-site queue", "error", err)
		}
	}()

	if !serveFrontend {
		go func() {
			if err := server.ServeFrontend(frontendPort); err != nil {
				log.Error("frontend server terminated", "error", err)
			}
		}()
	}

	return server.OpenRoutes(backendPort, serveFrontend)
}

func resolvePort(primaryEnv, legacyEnv string, fallback int) int {
	if port := readPort(primaryEnv); port != 0 {
		return port
	}
	if port := readPort(legacyEnv); port != 0 {
		return port
	}
	return fallback
}

func readPort(envKey string) int {
	raw := os.Getenv(envKey)
	if raw == "" {
		return 0
	}
	port, err := strconv.Atoi(raw)
	if err != nil || port == 0 {
		log.Warn("invalid port override", "env", envKey, "value", raw)
		return 0
	}
	return port
}
