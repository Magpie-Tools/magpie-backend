package main

import (
	"flag"
	"github.com/charmbracelet/log"
	"github.com/joho/godotenv"
	"magpie/checker/redis_queue"
	"magpie/routing"
	redis_queue2 "magpie/scraper/redis_queue"
	"magpie/settings"
	"magpie/setup"
	"os"
	"runtime/debug"
	"strconv"
)

func init() {
	err := godotenv.Load()
	if err != nil {
		log.Warn("No .env file found. Falling back to system environment variables.")
	}

}

func main() {
	log.Info("Starting Program")
	log.SetLevel(log.DebugLevel)

	debug.SetMaxThreads(9999999999)

	apiPortFlag := flag.Int("backend-port", 8082, "Port for API server")
	frontendPortFlag := flag.Int("frontend-portBackend", 8084, "Port for frontend static server")
	productionFlag := flag.Bool("production", false, "Run in production mode")
	flag.Parse()

	settings.SetProductionMode(*productionFlag)

	portBackend, err := strconv.Atoi(os.Getenv("backend-port"))

	if err != nil || portBackend == 0 {
		portBackend = *apiPortFlag
	}

	portFrontend, err := strconv.Atoi(os.Getenv("frontend-portBackend"))

	if err != nil || portFrontend == 0 {
		portFrontend = *frontendPortFlag
	}

	setup.Setup()

	defer func() {
		if err := redis_queue.PublicProxyQueue.Close(); err != nil {
			log.Warn("error closing proxy queue", "error", err)
		}

		if err := redis_queue2.PublicScrapeSiteQueue.Close(); err != nil {
			log.Warn("error closing scrape‑site queue", "error", err)
		}
	}()

	go routing.ServeFrontend(portFrontend)
	routing.OpenRoutes(portBackend)
}
