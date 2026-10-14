package judges

import (
	"encoding/json"
	"testing"
)

func resetJudgesCache() {
	updateJudges(make(map[uint]map[string]*judgeEntry))
}

func decodeLegacyJudgeSyncEvent(t *testing.T, payload string) judgeSyncEvent {
	t.Helper()
	var event judgeSyncEvent
	if err := json.Unmarshal([]byte(payload), &event); err != nil {
		t.Fatalf("decode historical judge event: %v", err)
	}
	encoded, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("encode judge event: %v", err)
	}
	if string(encoded) != payload {
		t.Fatalf("judge event changed wire format: got %s, want %s", encoded, payload)
	}
	return event
}

func TestHandleJudgeSyncEventSetWorkspaceJudges(t *testing.T) {
	resetJudgesCache()

	event := decodeLegacyJudgeSyncEvent(t, `{"type":"set_user_judges","origin":"","user_id":99,"judges":[{"id":123,"url":"https://example.com/judge","regex":"default"}]}`)

	handleJudgeSyncEvent(event)

	judge, regex := GetNextJudge(99, "https")
	if judge == nil {
		t.Fatalf("expected judge to be set for workspace 99")
	}
	if judge.ID != 123 {
		t.Fatalf("expected judge ID 123, got %d", judge.ID)
	}
	if regex != "default" {
		t.Fatalf("expected regex 'default', got %q", regex)
	}
}

func TestHandleJudgeSyncEventAddJudgesToWorkspaces(t *testing.T) {
	resetJudgesCache()

	event := decodeLegacyJudgeSyncEvent(t, `{"type":"add_judges_to_users","origin":"","user_ids":[1,2],"judges":[{"id":55,"url":"http://azenv.net","regex":"azenv"}]}`)

	handleJudgeSyncEvent(event)

	for _, workspaceID := range []uint{1, 2} {
		judge, regex := GetNextJudge(workspaceID, "http")
		if judge == nil {
			t.Fatalf("expected judge for workspace %d", workspaceID)
		}
		if judge.ID != 55 {
			t.Fatalf("expected judge ID 55 for workspace %d, got %d", workspaceID, judge.ID)
		}
		if regex != "azenv" {
			t.Fatalf("expected regex 'azenv' for workspace %d, got %q", workspaceID, regex)
		}
	}
}
