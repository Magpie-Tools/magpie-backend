package setup

import (
	"github.com/charmbracelet/log"
	"magpie/checker/judges"
	"magpie/database"
	"magpie/helper"
	"magpie/models"
	"magpie/settings"
	"sync"
)

var addDefaultJudgeMutex sync.Mutex

// AddDefaultJudgesToUsers gets empty judges list of users and adds the default judges (from config) to the db
// this is the ugliest function I have ever written
func AddDefaultJudgesToUsers() {
	addDefaultJudgeMutex.Lock()
	defer addDefaultJudgeMutex.Unlock()

	cfg := settings.GetConfig()
	users := database.GetUsersThatDontHaveJudges()

	judgesWithRegex := make([]*models.JudgeWithRegex, len(cfg.Checker.Judges))
	judgeList := make([]*models.Judge, len(cfg.Checker.Judges))

	for i, judge := range cfg.Checker.Judges {
		judgesWithRegex[i] = &models.JudgeWithRegex{
			Judge: &models.Judge{
				FullString: judge.URL,
			},
			Regex: judge.Regex,
		}

		judgeList[i] = judgesWithRegex[i].Judge
		err := judgesWithRegex[i].Judge.SetUp()
		if err != nil {
			log.Error("Error setting up judge", "error", err)
		}
		judgesWithRegex[i].Judge.UpdateIp()
	}

	jwr := database.GetJudgesRegexFromString(judgeList) // Get ids
	if len(jwr) == 0 {
		err := database.AddJudges(judgeList) // Sets id too
		if err != nil {
			database.GetJudgesFromString(judgeList) // Sets id if not added judgeList
		}

		for i, judge := range judgeList {
			setUpAndUpdateJudgeIp(judge)
			judgesWithRegex[i].Judge = judge
		}
	} else {
		judgesWithRegex = jwr

		for _, judge := range judgesWithRegex {
			setUpAndUpdateJudgeIp(judge.Judge)
		}
	}

	err := database.AddUserJudgesRelation(users, judgesWithRegex)
	if err != nil {
		log.Error("Error adding user judgeList to database", "error", err)
	} else {
		judgesNonPointer := make([]models.JudgeWithRegex, len(judgesWithRegex))
		for i, j := range judgesWithRegex {
			judgesNonPointer[i] = *j
		}

		judges.AddJudgesToUsers(helper.GetUserIdsFromList(users), judgesNonPointer)
	}
}

func addJudgeRelationsToCache() {
	userJudges, jwr := database.GetAllUserJudgeRelations()

	for _, userJudge := range userJudges {
		for _, judge := range jwr {
			if userJudge.JudgeID == judge.Judge.ID {
				judges.AddUserJudge(userJudge.UserID, judge.Judge, judge.Regex)
			}
		}
	}
}

func setUpAndUpdateJudgeIp(judge *models.Judge) {
	judge.SetUp()
	judge.UpdateIp()
}
