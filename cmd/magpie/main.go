package main

import (
	"magpie/internal/app"

	"github.com/charmbracelet/log"
)

func main() {
	if err := app.Run(); err != nil {
		log.Fatal("application terminated", "error", err)
	}
}
