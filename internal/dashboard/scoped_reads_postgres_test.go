package dashboard

import (
	"context"
	"encoding/json"
	"magpie/internal/api/dto"
	"magpie/internal/evidence"
	"reflect"
	"testing"
)

func TestDashboardScopedReadsPreserveFastestAndJudgeResultsPostgres(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	workspaces := seedCurrentLatestReadTest(t, db)
	t.Cleanup(func() {
		for _, workspace := range workspaces {
			dashboardInfoCache.Delete(workspace.ID)
			for _, limit := range []int{1, 3, 100} {
				dashboardFastestAliveCache.Delete(dashboardProxyListCacheKey{WorkspaceID: workspace.ID, Limit: limit})
			}
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	db = db.WithContext(ctx)
	for _, workspace := range workspaces {
		for _, limit := range []int{1, 3, 100} {
			var want []dto.ProxyFastestAlive
			legacy := db.Table("("+evidence.CurrentLatestStatisticsSQL+") pls").Where("pls.checking_workspace_id=? AND pls.alive=TRUE", workspace.ID).
				Select("DISTINCT ON (pls.proxy_id) pls.proxy_id,pls.response_time,pls.checked_at").
				Order("pls.proxy_id,pls.response_time ASC,pls.checked_at DESC,pls.statistic_id DESC")
			if err := db.Table("proxies p").Select(`p.id,p.host AS ip,p.port,COALESCE(latest.response_time,0) AS response_time,
COALESCE(NULLIF(p.country,''),'N/A') AS country,LOWER(COALESCE(NULLIF(pr.label,''),'unknown')) AS reputation_label,
COALESCE(pr.score,0) AS reputation_score,latest.checked_at AS latest_check`).
				Joins("JOIN user_proxies up ON up.proxy_id=p.id AND up.workspace_id=? AND up.state='active'", workspace.ID).
				Joins("JOIN (?) latest ON latest.proxy_id=p.id", legacy).
				Joins("LEFT JOIN proxy_reputations pr ON pr.proxy_id=p.id AND pr.kind='overall'").
				Order("latest.response_time ASC,latest.checked_at DESC,p.id ASC").Limit(limit).Scan(&want).Error; err != nil {
				t.Fatal(err)
			}
			got, err := refreshFastestAliveProxiesCache(db, workspace.ID, limit)
			if err != nil || len(got) != len(want) || len(got) > 0 && !reflect.DeepEqual(got, want) {
				t.Fatalf("workspace=%d limit=%d fastest=%+v want=%+v error=%v", workspace.ID, limit, got, want, err)
			}
		}
		type judgeBuckets struct {
			JudgeUrl           string `json:"judge_url"`
			EliteProxies       uint   `json:"elite_proxies"`
			AnonymousProxies   uint   `json:"anonymous_proxies"`
			TransparentProxies uint   `json:"transparent_proxies"`
		}
		want := make([]judgeBuckets, 0)
		if err := db.Table("("+evidence.CurrentLatestStatisticsSQL+") pls").
			Select(`j.full_string AS judge_url,SUM(CASE WHEN al.name='elite' THEN 1 ELSE 0 END) AS elite_proxies,
SUM(CASE WHEN al.name='anonymous' THEN 1 ELSE 0 END) AS anonymous_proxies,
SUM(CASE WHEN al.name='transparent' THEN 1 ELSE 0 END) AS transparent_proxies`).
			Joins("JOIN user_proxies up ON up.proxy_id=pls.proxy_id AND up.workspace_id=? AND up.state='active'", workspace.ID).
			Joins("JOIN user_judges uj ON uj.judge_id=pls.judge_id AND uj.workspace_id=?", workspace.ID).
			Joins("JOIN judges j ON j.id=pls.judge_id JOIN anonymity_levels al ON al.id=pls.level_id").
			Where("pls.checking_workspace_id=? AND pls.alive=TRUE", workspace.ID).Group("j.id,j.full_string").Scan(&want).Error; err != nil {
			t.Fatal(err)
		}
		info, err := refreshDashboardInfoCache(db, workspace.ID)
		if err != nil {
			t.Fatal(err)
		}
		gotJSON, _ := json.Marshal(info.JudgeValidProxies)
		wantJSON, _ := json.Marshal(want)
		if len(info.JudgeValidProxies) != len(want) || len(want) > 0 && string(gotJSON) != string(wantJSON) {
			t.Fatalf("workspace=%d judge buckets=%s want=%s", workspace.ID, gotJSON, wantJSON)
		}
	}
}
