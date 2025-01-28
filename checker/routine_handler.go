package checker

import (
	"github.com/charmbracelet/log"
	"magpie/settings"
	"time"
)

func StartJudgeRoutine() {
	for {
		judgeMap := GetAllJudgeEntries()
		betweenTime := getTimeBetweenChecks(getCountOfJudgeEntries(judgeMap))

		for _, judgeEntries := range judgeMap {
			for _, judge := range judgeEntries.list {
				judge.UpdateIp()

				log.Debug(betweenTime.String())

				time.Sleep(betweenTime)
			}
		}

	}
}

func getCountOfJudgeEntries(entries map[string]*judgeEntry) uint64 {
	totalCount := 0

	for _, entry := range entries {
		if entry != nil {
			totalCount += len(entry.list)
		}
	}

	return uint64(totalCount)
}

func getTimeBetweenChecks(count uint64) time.Duration {
	return time.Duration(settings.CalculateMillisecondsOfCheckingPeriod(settings.GetConfig())/count) * time.Millisecond
}
