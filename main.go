package main

import (
	"flag"
	"github.com/charmbracelet/log"
	"github.com/joho/godotenv"
	"magpie/checker"
	"magpie/database"
	"magpie/helper"
	"magpie/routing"
	"magpie/settings"
	"os"
	"runtime/debug"
	"strconv"
	"time"
)

func init() {
	err := godotenv.Load()
	if err != nil {
		log.Warn("No .env file found. Falling back to system environment variables.")
	}

}

func main() {
	//logFile, err := os.OpenFile("output.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	//if err != nil {
	//	log.Fatalf("Failed to open log file: %v", err)
	//}
	//defer logFile.Close()
	//
	//multiWriter := io.MultiWriter(os.Stdout, logFile)
	//log.SetOutput(multiWriter)

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

	settings.ReadSettings()
	setup()

	routing.OpenRoutes(port)
}

func setup() {
	database.SetupDB()
	judgeSetup()

	go func() {
		cfg := settings.GetConfig()

		if cfg.Checker.CurrentIp == "" && cfg.Checker.IpLookup == "" {
			return
		}

		for cfg.Checker.CurrentIp == "" {
			html, err := checker.DefaultRequest(cfg.Checker.IpLookup)
			if err != nil {
				log.Error("Error checking IP address:", err)
			}

			cfg = settings.GetConfig()
			if cfg.Checker.CurrentIp == "" {
				cfg.Checker.CurrentIp = helper.FindIP(html)
				settings.SetConfig(cfg)
				log.Infof("Found IP! Current IP: %s", cfg.Checker.CurrentIp)
			}

			time.Sleep(3 * time.Second)
		}

	}()

	// Routines

	go checker.StartJudgeRoutine()
}

func judgeSetup() {
	cfg := settings.GetConfig()

	for _, judge := range cfg.Checker.Judges {
		err := checker.CreateAndAddJudgeToHandler(judge.URL, judge.Regex)
		if err != nil {
			log.Warn("Error creating and adding judge to handler:", err)
		}
	}
}
