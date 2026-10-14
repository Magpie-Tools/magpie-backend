package judges

import (
	"sort"
	"sync"
	"sync/atomic"
	"unsafe"

	"magpie/internal/domain"
)

type judgeEntry struct {
	list    []domain.JudgeWithRegex
	length  uint32
	counter uint32
	_       [64 - unsafe.Sizeof([]domain.JudgeWithRegex{}) - 4 - 4]byte // Padding for cache line alignment
}

var (
	judgesMutex sync.Mutex
	judges      atomic.Value // map[uint]map[string]*judgeEntry (workspaceID -> protocol -> entry)
)

func init() {
	updateJudges(make(map[uint]map[string]*judgeEntry))
}

// GetNextJudge returns the next judge and regex for a workspace and protocol.
func GetNextJudge(workspaceID uint, protocol string) (*domain.Judge, string) {
	currentMap, _ := judges.Load().(map[uint]map[string]*judgeEntry)
	workspaceMap, ok := currentMap[workspaceID]
	if !ok {
		return nil, ""
	}

	je := workspaceMap[protocol]
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

// AddWorkspaceJudge atomically adds a judge and regex to a workspace's protocol list.
func AddWorkspaceJudge(workspaceID uint, judge *domain.Judge, regex string) {
	judgesMutex.Lock()
	defer judgesMutex.Unlock()

	currentMap := judges.Load().(map[uint]map[string]*judgeEntry)
	newMap := copyMap(currentMap)

	protoMap := newMap[workspaceID]
	if protoMap == nil {
		protoMap = make(map[string]*judgeEntry)
		newMap[workspaceID] = protoMap
	}

	entry := protoMap[judge.GetScheme()]
	if entry == nil {
		protoMap[judge.GetScheme()] = &judgeEntry{
			list:    []domain.JudgeWithRegex{{Judge: judge, Regex: regex}},
			length:  1,
			counter: 0,
		}
	} else {
		newEntry := &judgeEntry{
			list:    append(entry.list, domain.JudgeWithRegex{Judge: judge, Regex: regex}),
			length:  entry.length + 1,
			counter: atomic.LoadUint32(&entry.counter),
		}
		protoMap[judge.GetScheme()] = newEntry
	}

	updateJudges(newMap)
}

func copyMap(src map[uint]map[string]*judgeEntry) map[uint]map[string]*judgeEntry {
	dst := make(map[uint]map[string]*judgeEntry, len(src))
	for workspaceID, protoMap := range src {
		dstProto := make(map[string]*judgeEntry, len(protoMap))
		for proto, entry := range protoMap {
			dstProto[proto] = entry
		}
		dst[workspaceID] = dstProto
	}
	return dst
}

// GetSortedJudgesByID returns a sorted list of all judges, sorted by their ID.
func GetSortedJudgesByID() []*domain.Judge {
	currentMap, _ := judges.Load().(map[uint]map[string]*judgeEntry)
	judgeSet := make(map[uint]*domain.Judge) // Judge ID as key for deduplication

	// Collect judges from every workspace and protocol entry.
	for _, workspaceMap := range currentMap {
		for _, entry := range workspaceMap {
			for _, jwr := range entry.list {
				judge := jwr.Judge
				judgeSet[judge.ID] = judge
			}
		}
	}

	// Convert the map to a slice
	sortedJudges := make([]*domain.Judge, 0, len(judgeSet))
	for _, judge := range judgeSet {
		sortedJudges = append(sortedJudges, judge)
	}

	// Sort the slice by Judge ID
	sort.Slice(sortedJudges, func(i, j int) bool {
		return sortedJudges[i].ID < sortedJudges[j].ID
	})

	return sortedJudges
}

// AddJudgesToWorkspaces atomically adds the judges and their regexes to each
// workspace's local cache, then broadcasts the change to other instances.
func AddJudgesToWorkspaces(workspaceIDs []uint, judgesWithRegex []domain.JudgeWithRegex) {
	if len(workspaceIDs) == 0 || len(judgesWithRegex) == 0 {
		return
	}

	addJudgesToWorkspacesLocal(workspaceIDs, judgesWithRegex)
	broadcastAddJudgesToWorkspaces(workspaceIDs, judgesWithRegex)
}

// SetWorkspaceJudges replaces a workspace's local judge cache in one atomic
// swap, then broadcasts the change to other instances.
func SetWorkspaceJudges(workspaceID uint, judgesWithRegex []domain.JudgeWithRegex) {
	setWorkspaceJudgesLocal(workspaceID, judgesWithRegex)
	broadcastSetWorkspaceJudges(workspaceID, judgesWithRegex)
}

func addJudgesToWorkspacesLocal(workspaceIDs []uint, judgesWithRegex []domain.JudgeWithRegex) {
	judgesMutex.Lock()
	defer judgesMutex.Unlock()

	currentMap := judges.Load().(map[uint]map[string]*judgeEntry)
	newMap := copyMap(currentMap)

	for _, workspaceID := range workspaceIDs {
		protoMap := newMap[workspaceID]
		if protoMap == nil {
			protoMap = make(map[string]*judgeEntry)
			newMap[workspaceID] = protoMap
		}

		for _, jwr := range judgesWithRegex {
			if jwr.Judge == nil {
				continue
			}
			scheme := jwr.Judge.GetScheme()
			entry := protoMap[scheme]
			if entry == nil {
				protoMap[scheme] = &judgeEntry{
					list:    []domain.JudgeWithRegex{jwr},
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

func setWorkspaceJudgesLocal(workspaceID uint, judgesWithRegex []domain.JudgeWithRegex) {
	judgesMutex.Lock()
	defer judgesMutex.Unlock()

	currentMap := judges.Load().(map[uint]map[string]*judgeEntry)
	newMap := copyMap(currentMap)

	protoMap := make(map[string]*judgeEntry, len(judgesWithRegex))
	for _, jwr := range judgesWithRegex {
		if jwr.Judge == nil {
			continue
		}

		scheme := jwr.Judge.GetScheme()
		if entry := protoMap[scheme]; entry != nil {
			newList := append(entry.list, jwr)
			protoMap[scheme] = &judgeEntry{
				list:    newList,
				length:  entry.length + 1,
				counter: atomic.LoadUint32(&entry.counter),
			}
		} else {
			protoMap[scheme] = &judgeEntry{
				list:    []domain.JudgeWithRegex{jwr},
				length:  1,
				counter: 0,
			}
		}
	}

	newMap[workspaceID] = protoMap
	updateJudges(newMap)
}
