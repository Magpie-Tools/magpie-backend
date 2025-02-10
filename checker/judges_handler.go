package checker

import (
	"magpie/models"
	"sort"
	"sync"
	"sync/atomic"
	"unsafe"
)

type judgeEntry struct {
	list    []models.JudgeWithRegex
	length  uint32
	counter uint32
	_       [64 - unsafe.Sizeof([]models.JudgeWithRegex{}) - 4 - 4]byte // Padding for cache line alignment
}

var (
	judgesMutex sync.Mutex
	judges      atomic.Value // map[uint]map[string]*judgeEntry (userID -> protocol -> entry)
)

func init() {
	updateJudges(make(map[uint]map[string]*judgeEntry))
}

// getNextJudge returns the next Judge and Regex for a user/protocol combination
func getNextJudge(userID uint, protocol string) (*models.Judge, string) {
	currentMap, _ := judges.Load().(map[uint]map[string]*judgeEntry)
	userMap, ok := currentMap[userID]
	if !ok {
		return nil, ""
	}

	je := userMap[protocol]
	if je == nil || je.length == 0 {
		return nil, ""
	}

	idx := atomic.AddUint32(&je.counter, 1) - 1
	idx %= je.length // Use bitwise AND if length is power-of-two

	entry := je.list[idx]
	return entry.Judge, entry.Regex
}

func updateJudges(newMap map[uint]map[string]*judgeEntry) {
	judges.Store(newMap)
}

// AddUserJudge atomically adds a Judge with Regex to a user's protocol list
func AddUserJudge(userID uint, judge *models.Judge, regex string) {
	judgesMutex.Lock()
	defer judgesMutex.Unlock()

	currentMap := judges.Load().(map[uint]map[string]*judgeEntry)
	newMap := copyMap(currentMap)

	protoMap := newMap[userID]
	if protoMap == nil {
		protoMap = make(map[string]*judgeEntry)
		newMap[userID] = protoMap
	}

	entry := protoMap[judge.GetScheme()]
	if entry == nil {
		protoMap[judge.GetScheme()] = &judgeEntry{
			list:    []models.JudgeWithRegex{{Judge: judge, Regex: regex}},
			length:  1,
			counter: 0,
		}
	} else {
		newEntry := &judgeEntry{
			list:    append(entry.list, models.JudgeWithRegex{Judge: judge, Regex: regex}),
			length:  entry.length + 1,
			counter: atomic.LoadUint32(&entry.counter),
		}
		protoMap[judge.GetScheme()] = newEntry
	}

	updateJudges(newMap)
}

// BulkUpdateJudges optimizes mass updates (use for initial load)
func BulkUpdateJudges(newMap map[uint]map[string]*judgeEntry) {
	judgesMutex.Lock()
	defer judgesMutex.Unlock()
	updateJudges(newMap)
}

func copyMap(src map[uint]map[string]*judgeEntry) map[uint]map[string]*judgeEntry {
	dst := make(map[uint]map[string]*judgeEntry, len(src))
	for userID, protoMap := range src {
		dstProto := make(map[string]*judgeEntry, len(protoMap))
		for proto, entry := range protoMap {
			dstProto[proto] = entry
		}
		dst[userID] = dstProto
	}
	return dst
}

// GetSortedJudgesByID returns a sorted list of all judges, sorted by their ID.
func GetSortedJudgesByID() []*models.Judge {
	currentMap, _ := judges.Load().(map[uint]map[string]*judgeEntry)
	judgeSet := make(map[uint]*models.Judge) // Judge ID as key for deduplication

	// Iterate through all user and protocol entries to collect judges
	for _, userMap := range currentMap {
		for _, entry := range userMap {
			for _, jwr := range entry.list {
				judge := jwr.Judge
				judgeSet[judge.ID] = judge
			}
		}
	}

	// Convert the map to a slice
	sortedJudges := make([]*models.Judge, 0, len(judgeSet))
	for _, judge := range judgeSet {
		sortedJudges = append(sortedJudges, judge)
	}

	// Sort the slice by Judge ID
	sort.Slice(sortedJudges, func(i, j int) bool {
		return sortedJudges[i].ID < sortedJudges[j].ID
	})

	return sortedJudges
}

// AddJudgesToUsers adds a list of judges with regex to multiple users atomically.
// Each user in the userIDs list receives all the provided judges.
func AddJudgesToUsers(userIDs []uint, judgesWithRegex []models.JudgeWithRegex) {
	judgesMutex.Lock()
	defer judgesMutex.Unlock()

	currentMap := judges.Load().(map[uint]map[string]*judgeEntry)
	newMap := copyMap(currentMap)

	for _, userID := range userIDs {
		protoMap := newMap[userID]
		if protoMap == nil {
			protoMap = make(map[string]*judgeEntry)
			newMap[userID] = protoMap
		}

		for _, jwr := range judgesWithRegex {
			scheme := jwr.Judge.GetScheme()
			entry := protoMap[scheme]
			if entry == nil {
				protoMap[scheme] = &judgeEntry{
					list:    []models.JudgeWithRegex{jwr},
					length:  1,
					counter: 0,
				}
			} else {
				newList := append(entry.list, jwr)
				protoMap[scheme] = &judgeEntry{
					list:    newList,
					length:  entry.length + 1,
					counter: atomic.LoadUint32(&entry.counter),
				}
			}
		}
	}

	updateJudges(newMap)
}
