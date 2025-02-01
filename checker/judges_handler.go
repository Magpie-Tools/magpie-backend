package checker

import (
	"magpie/models"
	"sort"
	"sync"
	"sync/atomic"
	"unsafe"
)

type judgeEntry struct {
	list    []*models.Judge
	length  uint32
	counter uint32
	_       [64 - unsafe.Sizeof([]models.Judge{}) - 4 - 4]byte // Pad to 64 bytes
}

var (
	judgesMutex sync.Mutex
	judges      atomic.Value // Holds map[string]*judgeEntry
)

//TODO
// no judges for socks4/5. Because they just will be redundant and will just be more work than without them.
// Use http/s for them.

func init() {
	updateJudges(make(map[string]*judgeEntry))
}

func getNextJudge(protocol string) *models.Judge {
	currentMap, _ := judges.Load().(map[string]*judgeEntry)
	je := currentMap[protocol]
	if je == nil || je.length == 0 {
		return &models.Judge{}
	}

	idx := atomic.AddUint32(&je.counter, 1) - 1
	idx %= je.length // Consider power-of-two optimization

	return je.list[idx]
}

func updateJudges(newMap map[string]*judgeEntry) {
	judges.Store(newMap)
}

func GetAllJudgeEntries() map[string]*judgeEntry {
	return judges.Load().(map[string]*judgeEntry)
}

func GetSortedJudgeEntries() []*judgeEntry {
	judgeMap := GetAllJudgeEntries()
	keys := make([]string, 0, len(judgeMap))

	// Collect keys
	for key := range judgeMap {
		keys = append(keys, key)
	}

	// Sort keys
	sort.Strings(keys)

	// Iterate in sorted order
	sortedEntries := make([]*judgeEntry, 0, len(keys))
	for _, key := range keys {
		sortedEntries = append(sortedEntries, judgeMap[key])
	}

	return sortedEntries
}

// AddJudge adds a judge to the list of available judges for the specified protocol and sorts the list by fullstring.
func AddJudge(protocol string, judge *models.Judge) {
	judgesMutex.Lock()
	defer judgesMutex.Unlock()

	currentMap := judges.Load().(map[string]*judgeEntry)
	newMap := make(map[string]*judgeEntry, len(currentMap)+1)

	// Copy existing entries
	for k, v := range currentMap {
		newMap[k] = v
	}

	entry, exists := newMap[protocol]
	if !exists {
		newMap[protocol] = &judgeEntry{
			list:    []*models.Judge{judge},
			length:  1,
			counter: 0,
		}
	} else {
		newList := make([]*models.Judge, len(entry.list)+1)
		copy(newList, entry.list)
		newList[len(entry.list)] = judge

		currentCounter := atomic.LoadUint32(&entry.counter)
		newMap[protocol] = &judgeEntry{
			list:    newList,
			length:  uint32(len(newList)),
			counter: currentCounter,
		}
	}

	updateJudges(newMap)
}
func CreateAndAddJudgeToHandler(url, regex string) error {
	judge := models.Judge{}
	err := judge.SetUp(url, regex)
	if err != nil {
		return err
	}

	judge.UpdateIp()

	AddJudge(judge.GetScheme(), &judge)

	return nil
}
