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

	portFlag := flag.Int("port", 8082, "Port to listen on")
	productionFlag := flag.Bool("production", false, "Run in production mode")
	flag.Parse()

	settings.SetProductionMode(*productionFlag)

	port, err := strconv.Atoi(os.Getenv("PORT"))

	if err != nil || port == 0 {
		port = *portFlag
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

	routing.OpenRoutes(port)
}
