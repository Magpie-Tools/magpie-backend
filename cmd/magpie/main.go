package main

import (
	"github.com/charmbracelet/log"
	"magpie/internal/app"
)

func main() {
	if err := app.Run(); err != nil {
		log.Fatal("application terminated", "error", err)
	}
}
