package checker

import (
	"magpie/settings"
	"time"
)

func StartJudgeRoutine() {
	for {
		judgeEntriesList := GetSortedJudgeEntries()
		betweenTime := getTimeBetweenJudgeChecks(uint64(len(judgeEntriesList)))

		for _, judgeEntries := range judgeEntriesList {
			for _, judge := range judgeEntries.list {
				judge.UpdateIp()

				time.Sleep(betweenTime)
			}
		}

	}
}

func getTimeBetweenJudgeChecks(count uint64) time.Duration {
	var periodTime uint64

	if settings.InProductionMode {
		periodTime = settings.CalculateMillisecondsOfCheckingPeriod(settings.GetConfig().Checker.JudgeTimer) / count
	} else {
		periodTime = settings.CalculateMillisecondsOfCheckingPeriod(settings.GetConfig().Timer) / count / 2 // Twice per period_time
	}

	return time.Duration(periodTime) * time.Millisecond
}
