package main

import (
	"github.com/charmbracelet/log"
	"magpie/checker"
	"magpie/helper"
	"magpie/routing"
	"magpie/settings"
	"runtime/debug"
	"time"
)

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

	settings.ReadSettings()
	setup()

	routing.OpenRoutes(8080)
}

func setup() {
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
}
