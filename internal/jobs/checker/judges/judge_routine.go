package judges

import (
	"magpie/internal/config"
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

	if config.InProductionMode {
		periodTime = config.CalculateMillisecondsOfCheckingPeriod(config.GetConfig().Checker.JudgeTimer) / count
	} else {
		periodTime = config.CalculateMillisecondsOfCheckingPeriod(config.GetConfig().Checker.CheckerTimer) / count / 2 // Twice per period_time
	}

	return time.Duration(periodTime) * time.Millisecond
}
