package judges

import (
	"magpie/settings"
	"time"
)

func StartJudgeRoutine() {
	for {
		judgeList := GetSortedJudgesByID()
		if len(judgeList) > 0 {
			betweenTime := getTimeBetweenJudgeChecks(uint64(len(judgeList)))

			for _, judge := range judgeList {
				judge.UpdateIp()

				time.Sleep(betweenTime)
			}
		} else {
			time.Sleep(2 * time.Second)
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
