package setup

import (
	"github.com/charmbracelet/log"
	"magpie/checker"
	"magpie/checker/statistics"
	"magpie/database"
	"magpie/helper"
	"magpie/models"
	"magpie/settings"
	"time"
)

func Setup() {
	settings.ReadSettings()

	database.SetupDB()
	statisticsSetup()
	settings.SetBetweenTime()

	judgeSetup()

	go func() {
		cfg := settings.GetConfig()

		if settings.GetCurrentIp() == "" && cfg.Checker.IpLookup == "" {
			return
		}

		for settings.GetCurrentIp() == "" {
			html, err := checker.DefaultRequest(cfg.Checker.IpLookup)
			if err != nil {
				log.Error("Error checking IP address:", err)
				continue
			}

			currentIp := helper.FindIP(html)
			settings.SetCurrentIp(currentIp)
			log.Infof("Found IP! Current IP: %s", currentIp)

			time.Sleep(3 * time.Second)
		}

	}()

	proxies := database.GetAllProxies()
	checker.PublicProxyQueue.AddToQueue(proxies)
	proxyLen := len(proxies)
	statistics.IncreaseProxyCount(int64(proxyLen))
	log.Infof("Added %d proxies to queue", proxyLen)

	// Routines

	go checker.StartJudgeRoutine()
	go database.StartProxyStatisticsRoutine()
	go checker.Dispatcher()
}

func statisticsSetup() {
	statistics.SetProxyCount(database.GetAllProxyCount())
}

// judgeSetup gets empty judges list of users and adds the default judges (from config) to the db
func judgeSetup() {
	cfg := settings.GetConfig()
	users := database.GetUsersThatDontHaveJudges()

	judgesWithRegex := make([]*models.JudgeWithRegex, len(cfg.Checker.Judges))
	judges := make([]*models.Judge, len(cfg.Checker.Judges))

	for i, judge := range cfg.Checker.Judges {
		judgesWithRegex[i] = &models.JudgeWithRegex{
			Judge: &models.Judge{
				FullString: judge.URL,
			},
			Regex: judge.Regex,
		}

		judges[i] = judgesWithRegex[i].Judge
		err := judgesWithRegex[i].Judge.SetUp()
		if err != nil {
			log.Error("Error setting up judge", "error", err)
		}
		judgesWithRegex[i].Judge.UpdateIp()
	}

	jwr := database.GetJudgesRegexFromString(judges) // Get ids
	if len(jwr) == 0 {
		err := database.AddJudges(judges) // Sets id too
		if err != nil {
			database.GetJudgesFromString(judges) // Sets id if not added judges
		}
	} else {
		judgesWithRegex = jwr
	}

	for i, judge := range judges {
		judgesWithRegex[i].Judge = judge
	}

	err := database.AddUserJudgesRelation(users, judgesWithRegex)
	if err != nil {
		log.Error("Error adding user judges to database", "error", err)
	} else {
		judgesNonPointer := make([]models.JudgeWithRegex, len(judgesWithRegex))
		for i, j := range judgesWithRegex {
			judgesNonPointer[i] = *j
		}

		checker.AddJudgesToUsers(helper.GetUserIdsFromList(users), judgesNonPointer)
	}

	// TODO add all existing user judges relation to the judge handler
}
