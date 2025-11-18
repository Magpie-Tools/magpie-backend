package judges

import "testing"

func resetJudgesCache() {
	updateJudges(make(map[uint]map[string]*judgeEntry))
}

func TestHandleJudgeSyncEventSetUserJudges(t *testing.T) {
	resetJudgesCache()

	event := judgeSyncEvent{
		Type:   judgeEventTypeSetUserJudges,
		UserID: 99,
		Judges: []judgeSyncJudge{
			{
				ID:    123,
				URL:   "https://example.com/judge",
				Regex: "default",
			},
		},
	}

	handleJudgeSyncEvent(event)

	judge, regex := GetNextJudge(99, "https")
	if judge == nil {
		t.Fatalf("expected judge to be set for user 99")
	}
	if judge.ID != 123 {
		t.Fatalf("expected judge ID 123, got %d", judge.ID)
	}
	if regex != "default" {
		t.Fatalf("expected regex 'default', got %q", regex)
	}
}

func TestHandleJudgeSyncEventAddJudgesToUsers(t *testing.T) {
	resetJudgesCache()

	event := judgeSyncEvent{
		Type:    judgeEventTypeAddJudgesToUsers,
		UserIDs: []uint{1, 2},
		Judges: []judgeSyncJudge{
			{
				ID:    55,
				URL:   "http://azenv.net",
				Regex: "azenv",
			},
		},
	}

	handleJudgeSyncEvent(event)

	for _, userID := range []uint{1, 2} {
		judge, regex := GetNextJudge(userID, "http")
		if judge == nil {
			t.Fatalf("expected judge for user %d", userID)
		}
		if judge.ID != 55 {
			t.Fatalf("expected judge ID 55 for user %d, got %d", userID, judge.ID)
		}
		if regex != "azenv" {
			t.Fatalf("expected regex 'azenv' for user %d, got %q", userID, regex)
		}
	}
}
