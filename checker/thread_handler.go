package checker

import (
	"magpie/settings"
	"sync/atomic"
	"time"
)

var (
	currentThreads atomic.Uint32
	stopChannel    = make(chan struct{}) // Signal to stop threads
)

func Dispatcher() {
	for {
		targetThreads := settings.Config.Checker.Threads

		// Start threads if currentThreads is less than targetThreads
		for currentThreads.Load() < targetThreads {
			go work()
			currentThreads.Add(1)
		}

		// Stop threads if currentThreads is greater than targetThreads
		for currentThreads.Load() > targetThreads {
			stopChannel <- struct{}{}
			currentThreads.Add(^uint32(0))
		}

		time.Sleep(2 * time.Second)
	}
}

func work() {
	for {
		select {
		case <-stopChannel:
			// Exit the work loop if a stop signal is received
			return
		default:
			_, err := PublicProxyQueue.GetNextProxy()
			if err != nil {
				//TODO dispatcher will automatically launch new threads when the queue is stopping. Make it not do that
				return
			}

			// Perform proxy checking or other tasks
			time.Sleep(settings.GetTimeBetweenChecks())
		}
	}
}
