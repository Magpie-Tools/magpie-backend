package main

import (
	"github.com/charmbracelet/log"
	"magpie/routing"
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

	log.SetLevel(log.DebugLevel)

	routing.OpenRoutes(8080)
}
