package testdb

import (
	"time"

	"gorm.io/gorm"
	"testing"

	"magpie/internal/api/dto"
	"magpie/internal/domain"
)

// CheckerProfiles returns the checker settings of the evidence scenario: HTTP
// only, a one-second timeout, no retries, over TCP.
func CheckerProfiles() *dto.CheckerSettings {
	w := domain.Workspace{HTTPProtocol: true, Timeout: 1000, Retries: 0, TransportProtocol: "tcp"}
	s := w.DefaultCheckerSettings()
	return s
}

// SeedCheckerEvidence is the shared checker-evidence scenario. It creates the
// four checker protocols, one judge, and two workspaces that manage the same
// route with different judge validation: the first accepts the response
// "accepted", the second expects "different". It returns the accepting
// workspace, the rejecting workspace, the route and the judge.
func SeedCheckerEvidence(t testing.TB, db *gorm.DB) (domain.Workspace, domain.Workspace, domain.Proxy, domain.Judge) {
	t.Helper()
	for i, name := range domain.CheckerProtocols {
		if err := db.Create(&domain.Protocol{ID: i + 1, Name: name}).Error; err != nil {
			t.Fatal(err)
		}
	}
	judge := domain.Judge{FullString: "http://127.0.0.1:8080/"}
	if err := db.Create(&judge).Error; err != nil {
		t.Fatal(err)
	}
	a, b := domain.Workspace{Name: "Accept", CheckerConfig: CheckerProfiles()}, domain.Workspace{Name: "Reject", CheckerConfig: CheckerProfiles()}
	if err := db.Create(&a).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&b).Error; err != nil {
		t.Fatal(err)
	}
	for id, regex := range map[uint]string{a.ID: "accepted", b.ID: "different"} {
		if err := db.Create(&domain.WorkspaceJudge{WorkspaceID: id, JudgeID: judge.ID, Regex: regex}).Error; err != nil {
			t.Fatal(err)
		}
	}
	proxy := domain.Proxy{IP: "192.0.2.1", Port: 8080}
	if err := db.Create(&proxy).Error; err != nil {
		t.Fatal(err)
	}
	for _, id := range []uint{a.ID, b.ID} {
		if err := db.Create(&domain.ManagedProxy{WorkspaceID: id, ProxyID: proxy.ID}).Error; err != nil {
			t.Fatal(err)
		}
	}
	return a, b, proxy, judge
}

// SeedAlertCheckRoute is the route of the alert measurement scenario: the HTTP
// and HTTPS protocols, judge 1, and one route without a managing workspace.
func SeedAlertCheckRoute(t *testing.T, db *gorm.DB) domain.Proxy {
	t.Helper()
	alertMust(t, db.Create(&domain.Protocol{ID: 1, Name: "http"}).Error)
	alertMust(t, db.Create(&domain.Protocol{ID: 2, Name: "https"}).Error)
	alertMust(t, db.Create(&domain.Judge{ID: 1, FullString: "https://judge.example.com"}).Error)
	proxy := domain.Proxy{IP: "1.2.3.4", Port: 8080}
	alertMust(t, db.Create(&proxy).Error)
	return proxy
}

// SeedAlertChecks records count checks of the route at the given time. Each
// carries one workspace verdict (alive, under the configuration key) and the
// opposite aggregate result, so a reader that ignores workspace attribution
// sees the wrong answer.
func SeedAlertChecks(t *testing.T, db *gorm.DB, proxy domain.Proxy, workspaceID uint, key string, alive bool, count int, now time.Time) {
	t.Helper()
	rows := make([]domain.ProxyStatistic, count)
	for i := range rows {
		rows[i] = domain.ProxyStatistic{ProxyID: proxy.ID, ProtocolID: 1, JudgeID: 1, Alive: !alive, ResponseTime: 120, TransportProtocol: "tcp", CreatedAt: now, CheckEvidence: []domain.WorkspaceCheckEvidence{{WorkspaceID: workspaceID, ConfigKey: key, Alive: alive}}}
	}
	alertMust(t, db.Create(&rows).Error)
}

func alertMust(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
