package judges

import (
	"context"
	"magpie/internal/config"
	"time"
)

func StartJudgeRoutine(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		judgeList := GetSortedJudgesByID()
		if len(judgeList) > 0 {
			betweenTime := getTimeBetweenJudgeChecks(uint64(len(judgeList)))

			for _, judge := range judgeList {
				select {
				case <-ctx.Done():
					return
				default:
				}

				judge.UpdateIp()

				select {
				case <-ctx.Done():
					return
				case <-time.After(betweenTime):
				}
			}
		} else {
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second):
			}
		}
	}
}

func getTimeBetweenJudgeChecks(count uint64) time.Duration {
	// Judge health follows its own timer; checker cadence is now derived from
	// workspace allowances and no longer suits a judge refresh period.
	period := config.CalculateMillisecondsOfCheckingPeriod(config.GetConfig().Checker.JudgeTimer)
	if period == 0 {
		period = uint64(time.Hour / time.Millisecond)
	}
	periodTime := period / max(count, 1)

	return time.Duration(periodTime) * time.Millisecond
}
