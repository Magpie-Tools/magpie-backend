package database

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"magpie/internal/api/dto"
	"magpie/internal/checkerconfig"
	"magpie/internal/config"
	"magpie/internal/domain"
	"magpie/internal/security"
	"magpie/internal/support/reputation"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func createDevelopmentFixtures(tx *gorm.DB, users []domain.User, options DevelopmentSeedOptions, result *DevelopmentSeedResult) error {
	namespaceHash := sha256.Sum256([]byte(strings.Join(options.Emails, ",")))
	namespace := "magpie-fixtures-" + hex.EncodeToString(namespaceHash[:6])
	judges := make([]domain.JudgeWithRegex, 0, 4)
	for i, scheme := range []string{"http", "https", "http", "https"} {
		judge := domain.Judge{FullString: fmt.Sprintf("%s://judge-%d.%s.invalid/headers", scheme, i+1, namespace), CreatedAt: options.Now.AddDate(0, 0, -options.HistoryDays)}
		if err := tx.Where("full_string = ?", judge.FullString).FirstOrCreate(&judge).Error; err != nil {
			return err
		}
		judges = append(judges, domain.JudgeWithRegex{Judge: &judge, Regex: "default"})
	}
	var levels []domain.AnonymityLevel
	if err := tx.Order("id").Find(&levels).Error; err != nil {
		return err
	}
	if len(levels) == 0 {
		return fmt.Errorf("run database migrations before seeding development data")
	}
	workspaces := make([]domain.Workspace, 0, 5)
	for index := 0; index < 5; index++ {
		name := []string{"Personal lab", "Provider testing", "Scraping research", "QA team", "Read-only review"}[index]
		workspace := domain.Workspace{
			Name: name, Personal: index < 3, HTTPProtocol: true, HTTPSProtocol: true,
			SOCKS4Protocol: true, SOCKS5Protocol: true, Timeout: 7500, Retries: 2,
			UseHttpsForSocks: true, TransportProtocol: "tcp", FailureAction: domain.FailureActionPause,
			AutoRemoveFailureThreshold: 3, CreatedAt: options.Now.AddDate(0, 0, -options.HistoryDays), UpdatedAt: options.Now,
		}
		if err := tx.Create(&workspace).Error; err != nil {
			return err
		}
		for userIndex, user := range users {
			if index < 3 && index != userIndex {
				continue
			}
			role := domain.WorkspaceRoleOwner
			if index == 3 {
				role = []string{domain.WorkspaceRoleOwner, domain.WorkspaceRoleAdmin, domain.WorkspaceRoleOperator}[userIndex]
			}
			if index == 4 {
				role = []string{domain.WorkspaceRoleOwner, domain.WorkspaceRoleViewer, domain.WorkspaceRoleViewer}[userIndex]
			}
			membership := domain.WorkspaceMembership{WorkspaceID: workspace.ID, UserID: user.ID, Role: role, BillingAdmin: role == domain.WorkspaceRoleOwner || role == domain.WorkspaceRoleAdmin, IsDefault: index == userIndex, CreatedAt: workspace.CreatedAt, UpdatedAt: options.Now}
			if err := tx.Create(&membership).Error; err != nil {
				return err
			}
			preference := domain.WorkspaceMemberPreference{WorkspaceID: workspace.ID, UserID: user.ID,
				ProxyListColumns: domain.NormalizeProxyListColumns(nil), ScrapeSourceProxyColumns: domain.NormalizeScrapeSourceProxyColumns(nil),
				ScrapeSourceListColumns: domain.StringList{"url", "proxy_count", "alive_count", "health", "actions"}, CreatedAt: workspace.CreatedAt, UpdatedAt: options.Now}
			if err := tx.Create(&preference).Error; err != nil {
				return err
			}
		}
		subscription := domain.WorkspaceSubscription{WorkspaceID: workspace.ID, PlanCode: "self-hosted", Status: domain.WorkspaceSubscriptionStatusActive,
			OverageMode: domain.WorkspaceOverageUnlimited, StatisticsRetentionDays: uint32(options.HistoryDays),
			MinimumCheckIntervalSeconds: 300, CurrentPeriodStart: new(options.Now.AddDate(0, 0, -15)), CurrentPeriodEnd: new(options.Now.AddDate(0, 0, 15)),
			CreatedAt: workspace.CreatedAt, UpdatedAt: options.Now}
		if index == 3 {
			subscription.IncludedActiveRoutes = uint64(options.ProxiesPerWorkspace * 7 / 10)
			subscription.AdditionalActiveRoutes = uint64(options.ProxiesPerWorkspace / 10)
			subscription.IncludedOperators = 3
			subscription.OverageMode = domain.WorkspaceOverageAllowed
			subscription.OverageActiveRoutes = uint64(options.ProxiesPerWorkspace / 5)
		}
		if err := tx.Create(&subscription).Error; err != nil {
			return err
		}
		for _, judge := range judges {
			if err := tx.Create(&domain.WorkspaceJudge{WorkspaceID: workspace.ID, JudgeID: judge.Judge.ID, Regex: judge.Regex, CreatedAt: workspace.CreatedAt}).Error; err != nil {
				return err
			}
		}
		if err := seedDevelopmentWorkspace(tx, &workspace, index, namespace, judges, levels, options, result); err != nil {
			return fmt.Errorf("seed workspace %s: %w", name, err)
		}
		workspaces = append(workspaces, workspace)
		result.Workspaces++
	}
	// Pending invitations exercise the inbox without queuing notification mail.
	for i := 0; i < 3; i++ {
		inviter := users[i]
		invitation := domain.WorkspaceInvitation{WorkspaceID: workspaces[i].ID, InviteeUserID: users[(i+1)%3].ID, InviterUserID: &inviter.ID,
			InviterEmail: inviter.Email, Role: []string{domain.WorkspaceRoleViewer, domain.WorkspaceRoleOperator, domain.WorkspaceRoleAdmin}[i],
			Notification: domain.WorkspaceInvitationNotificationNotConfigured, ExpiresAt: options.Now.Add(7 * 24 * time.Hour), CreatedAt: options.Now, UpdatedAt: options.Now}
		if err := tx.Create(&invitation).Error; err != nil {
			return err
		}
	}
	return nil
}

func seedDevelopmentWorkspace(tx *gorm.DB, workspace *domain.Workspace, index int, namespace string, judges []domain.JudgeWithRegex, levels []domain.AnonymityLevel, options DevelopmentSeedOptions, result *DevelopmentSeedResult) error {
	tags := make([]domain.ProxyTag, 0, 8)
	names := []string{"Fast routes", "HTTP only", "Residential", "Datacenter", "Provider gateway", "Needs review", "Imported", "Research"}
	colors := []string{"#10B981", "#3B82F6", "#8B5CF6", "#64748B", "#F59E0B", "#EF4444", "#06B6D4", "#EC4899"}
	for i, name := range names {
		tag := domain.ProxyTag{WorkspaceID: workspace.ID, Name: name, Color: colors[i], CreatedAt: workspace.CreatedAt, UpdatedAt: options.Now}
		if err := tx.Create(&tag).Error; err != nil {
			return err
		}
		tags = append(tags, tag)
	}
	result.Tags += len(tags)
	sites := make([]domain.ScrapeSite, 12)
	for i := range sites {
		sites[i] = domain.ScrapeSite{URL: fmt.Sprintf("https://source-%02d-%02d.%s.invalid/proxies.txt", index+1, i+1, namespace), CreatedAt: workspace.CreatedAt.Add(time.Duration(i) * time.Hour)}
		if err := tx.Create(&sites[i]).Error; err != nil {
			return err
		}
		status := []string{"success", "success", "success", "success", "success", "success", "success", "success", "success", "error", "blocked", "empty"}[i]
		source := domain.WorkspaceScrapeSite{WorkspaceID: workspace.ID, ScrapeSiteID: sites[i].ID, FetchMode: domain.ScrapeFetchHTTP,
			LastScrapedAt: new(options.Now.Add(-time.Duration(i+1) * time.Hour)), LastScrapeStatus: status, CreatedAt: sites[i].CreatedAt}
		if i%3 == 0 {
			source.FetchMode = domain.ScrapeFetchBrowser
		}
		if status == "error" {
			source.LastScrapeError = "Simulated upstream HTTP 503"
		}
		if status == "blocked" {
			source.LastScrapeError = "Simulated robots.txt disallow"
		}
		if i < 9 {
			source.LastScrapeProxyCount = (options.ProxiesPerWorkspace + 8 - i) / 9
		}
		if err := tx.Create(&source).Error; err != nil {
			return err
		}
		if err := tx.Create(&domain.ScrapeSourceTag{WorkspaceID: workspace.ID, ScrapeSiteID: sites[i].ID, ProxyTagID: tags[6].ID}).Error; err != nil {
			return err
		}
	}
	result.Sources += len(sites)
	proxies := make([]domain.Proxy, options.ProxiesPerWorkspace)
	access := make([]domain.ManagedProxy, options.ProxiesPerWorkspace)
	countries := []string{"Germany", "United States", "Austria", "Netherlands", "United Kingdom", "France", "Japan", "Canada", "Singapore", "Australia", "Brazil", "N/A"}
	types := []string{"Datacenter", "Residential", "ISP"}
	for i := range proxies {
		ordinal := index*options.ProxiesPerWorkspace + i
		host := fmt.Sprintf("%s.%d", []string{"192.0.2", "198.51.100", "203.0.113"}[(ordinal/254)%3], ordinal%254+1)
		if i%5 == 0 {
			host = fmt.Sprintf("gateway-%02d-%04d.%s.invalid", index+1, i, namespace)
		}
		if i%5 == 1 {
			host = fmt.Sprintf("2001:db8:%x::%x", index+1, i+1)
		}
		username, password := "", ""
		if i%3 == 0 || i%5 == 0 {
			username, password = fmt.Sprintf("demo-session-%04d", ordinal), "FixtureProxy12345"
		}
		proxies[i] = domain.Proxy{IP: host, Port: uint16(8000 + ordinal/762*10 + i%10), Username: username, Password: password,
			Country: countries[(i+index)%len(countries)], EstimatedType: types[i%len(types)], CreatedAt: workspace.CreatedAt.Add(12*time.Hour + time.Duration(i%60)*time.Minute)}
		state, reason := domain.ManagedProxyStateActive, ""
		if i%10 == 7 {
			state, reason = domain.ManagedProxyStatePaused, domain.ManagedProxyPauseReasonManual
			if i%20 == 17 {
				reason = domain.ManagedProxyPauseReasonCapacity
			}
		}
		if i%20 == 18 {
			state, reason = domain.ManagedProxyStatePaused, domain.ManagedProxyPauseReasonFailure
		}
		if i%20 == 9 {
			state = domain.ManagedProxyStateArchived
		}
		access[i] = domain.ManagedProxy{WorkspaceID: workspace.ID, Username: username, Password: password, State: state, PauseReason: reason, CreatedAt: proxies[i].CreatedAt, UpdatedAt: options.Now}
		if state == domain.ManagedProxyStateActive {
			access[i].ActivatedAt = new(proxies[i].CreatedAt)
		}
		if state == domain.ManagedProxyStatePaused {
			access[i].PausedAt = new(options.Now.Add(-2 * time.Hour))
		}
		if state == domain.ManagedProxyStateArchived {
			access[i].ArchivedAt = new(options.Now.Add(-24 * time.Hour))
		}
		if reason == domain.ManagedProxyPauseReasonFailure {
			access[i].ConsecutiveFailures = 5
		}
	}
	// An address already attached to an unrelated workspace is never overwritten.
	// A collision aborts and rolls back the entire reset.
	if err := tx.Omit(clause.Associations).CreateInBatches(&proxies, 500).Error; err != nil {
		return err
	}
	assignments := make([]domain.ProxyTagAssignment, 0, len(proxies)*3)
	links := make([]domain.ProxyScrapeSite, 0, len(proxies))
	for i, proxy := range proxies {
		access[i].ProxyID = proxy.ID
		selected := []int{6}
		if proxy.EstimatedType == "Residential" {
			selected = append(selected, 2)
		}
		if proxy.EstimatedType == "Datacenter" {
			selected = append(selected, 3)
		}
		if i%4 == 0 {
			selected = append(selected, 0)
		}
		if i%4 == 1 {
			selected = append(selected, 1)
		}
		if proxy.IsHostname() {
			selected = append(selected, 4)
		}
		if i%10 >= 6 {
			selected = append(selected, 5)
		}
		for _, tagIndex := range selected {
			assignments = append(assignments, domain.ProxyTagAssignment{WorkspaceID: workspace.ID, ProxyID: proxy.ID, ProxyTagID: tags[tagIndex].ID, CreatedAt: proxy.CreatedAt})
		}
		links = append(links, domain.ProxyScrapeSite{ProxyID: proxy.ID, ScrapeSiteID: sites[i%9].ID, CreatedAt: proxy.CreatedAt})
	}
	if err := tx.Omit(clause.Associations).CreateInBatches(&access, 500).Error; err != nil {
		return err
	}
	if err := tx.CreateInBatches(&assignments, 1000).Error; err != nil {
		return err
	}
	if err := tx.CreateInBatches(&links, 1000).Error; err != nil {
		return err
	}
	workspace.CheckerConfig = &dto.CheckerSettings{Defaults: dto.CheckerProfileSettings{Protocols: []string{"http", "https", "socks4", "socks5"}, Transport: "tcp", Timeout: 7500, Retries: 2},
		Rules: []dto.TagCheckerRule{{TagID: tags[0].ID, Mode: "add", Protocols: []string{}, Timeout: new(uint16(3500)), Retries: new(uint8(1))}, {TagID: tags[1].ID, Mode: "replace", Protocols: []string{"http", "https"}}}}
	snapshot := checkerconfig.Build(*workspace, assignments, judges, config.GetConfig().Checker.StandardHeader)
	workspace.CheckerHTTPKey, workspace.CheckerHTTPSKey = snapshot.Default.Keys[0], snapshot.Default.Keys[1]
	workspace.CheckerSOCKS4Key, workspace.CheckerSOCKS5Key = snapshot.Default.Keys[2], snapshot.Default.Keys[3]
	workspace.CheckerGeneration, workspace.CheckerRevision, workspace.CheckerFullRevision, workspace.CheckerProjectedRevision = 1, 1, 1, 1
	if err := tx.Omit(clause.Associations).Save(workspace).Error; err != nil {
		return err
	}
	plans := make([]domain.ProxyCheckerPlan, 0)
	for _, proxy := range proxies {
		for p, key := range snapshot.Plan(proxy.ID).Keys {
			if key != snapshot.Default.Keys[p] {
				plans = append(plans, domain.ProxyCheckerPlan{WorkspaceID: workspace.ID, ProxyID: proxy.ID, ProtocolID: p + 1, ConfigKey: key, Transport: "tcp"})
			}
		}
	}
	if len(plans) > 0 {
		if err := tx.CreateInBatches(&plans, 1000).Error; err != nil {
			return err
		}
	}
	if err := seedDevelopmentMeasurements(tx, *workspace, proxies, snapshot, levels, options, result); err != nil {
		return err
	}
	if err := seedDevelopmentTimeline(tx, *workspace, access, options); err != nil {
		return err
	}
	if err := seedDevelopmentAlerts(tx, *workspace, index, options, result); err != nil {
		return err
	}
	if err := refreshWorkspaceProxyFilterIndexes(tx, "WHERE up.workspace_id = ?", workspace.ID); err != nil {
		return err
	}
	if err := refreshWorkspaceScrapeSourceStats(tx, "WHERE uss.workspace_id = ?", workspace.ID); err != nil {
		return err
	}
	result.Proxies += len(proxies)
	return nil
}

func seedDevelopmentMeasurements(tx *gorm.DB, workspace domain.Workspace, proxies []domain.Proxy, snapshot *checkerconfig.Workspace, levels []domain.AnonymityLevel, options DevelopmentSeedOptions, result *DevelopmentSeedResult) error {
	statistics := make([]domain.ProxyStatistic, 0, 1000)
	latest := make([]domain.ProxyLatestStatistic, 0, len(proxies)*4)
	overall := make([]domain.ProxyOverallStatus, 0, len(proxies))
	daily := make([]domain.ProxyDailyCheck, 0, len(proxies)*options.HistoryDays)
	reputations := make([]domain.ProxyReputation, 0, len(proxies)*5)
	flush := func() error {
		if len(statistics) == 0 {
			return nil
		}
		if err := tx.Omit(clause.Associations).CreateInBatches(&statistics, 1000).Error; err != nil {
			return err
		}
		for _, stat := range statistics {
			if stat.CreatedAt.After(options.Now.Add(-24 * time.Hour)) {
				latest = append(latest, domain.ProxyLatestStatistic{WorkspaceID: workspace.ID, ConfigKey: stat.CheckEvidence[0].ConfigKey,
					TransportProtocol: stat.TransportProtocol, ProxyID: stat.ProxyID, ProtocolID: stat.ProtocolID, Alive: stat.Alive,
					StatisticID: stat.ID, ResponseTime: stat.ResponseTime, Attempt: stat.Attempt, LevelID: stat.LevelID, JudgeID: stat.JudgeID,
					CheckedAt: stat.CreatedAt, CreatedAt: stat.CreatedAt, UpdatedAt: options.Now})
			}
		}
		result.Statistics += len(statistics)
		statistics = statistics[:0]
		return nil
	}
	for i, proxy := range proxies {
		if i%10 == 9 { // Unknown routes have no invented check evidence.
			continue
		}
		plan := snapshot.Plan(proxy.ID)
		metrics := reputation.Metrics{EstimatedType: proxy.EstimatedType, SampleWindowHours: float64(options.HistoryDays * 24)}
		isAlive := false
		checkedAt := options.Now.Add(-time.Duration(i%180+1) * time.Second)
		for day := options.HistoryDays - 1; day >= 0; day-- {
			dayChecks := int64(0)
			for p, setting := range plan.Settings {
				if !setting.Enabled {
					continue
				}
				alive := i%10 < 6 || i%10 == 6 && (day+p)%2 == 0 || i%10 == 7 && day%3 != 1
				responseTime := uint16(45 + (uint64(i*37+p*71+day*11)+uint64(options.Seed))%1800)
				attempt := uint8(1)
				if i%4 == 0 {
					responseTime = uint16(50 + i%100)
				}
				if i%10 == 7 {
					responseTime = 3200
				}
				if !alive {
					responseTime, attempt = setting.Timeout, setting.Retries+1
				}
				timestamp := checkedAt.Add(-time.Duration(day) * 24 * time.Hour)
				judge := plan.Judges[p][i%len(plan.Judges[p])].Judge
				level := levels[i%len(levels)]
				stat := domain.ProxyStatistic{ProxyID: proxy.ID, ProtocolID: p + 1, JudgeID: judge.ID, Alive: alive, Attempt: attempt, ResponseTime: responseTime,
					TransportProtocol: setting.Transport, CheckTimeout: setting.Timeout, CheckRetries: setting.Retries,
					CheckEvidence: []domain.WorkspaceCheckEvidence{{WorkspaceID: workspace.ID, ConfigKey: plan.Keys[p], Alive: alive}}, CreatedAt: timestamp}
				if alive {
					stat.LevelID = new(level.ID)
					metrics.SuccessfulChecks++
					metrics.ResponseTimesMS = append(metrics.ResponseTimesMS, responseTime)
					metrics.LatestSuccess = new(timestamp)
					metrics.BestAnonymity = level.Name
				}
				if day == 0 && i < 25 {
					stat.ResponseBody = fmt.Sprintf("MAGPIE_DEVELOPMENT_FIXTURE = true\nREMOTE_ADDR = %s\nHTTP_USER_AGENT = Magpie development data\nCHECK_RESULT = %t\n", proxy.GetHost(), alive)
				}
				statistics = append(statistics, stat)
				metrics.TotalChecks++
				metrics.LatestCheck = new(timestamp)
				dayChecks++
				if day == 0 && alive {
					isAlive = true
				}
				if len(statistics) == cap(statistics) {
					if err := flush(); err != nil {
						return err
					}
				}
			}
			daily = append(daily, domain.ProxyDailyCheck{ProxyID: proxy.ID, Day: startOfUTCDay(checkedAt.Add(-time.Duration(day) * 24 * time.Hour)), ChecksCount: dayChecks, CreatedAt: options.Now, UpdatedAt: options.Now})
		}
		if !isAlive {
			metrics.FailureStreak = 5
		}
		score := reputation.Score(metrics, options.Now, nil)
		signals, err := json.Marshal(score.Signals)
		if err != nil {
			return err
		}
		reputations = append(reputations, domain.ProxyReputation{ProxyID: proxy.ID, Kind: domain.ProxyReputationKindOverall, Score: float32(score.Score), Label: score.Label, Signals: signals, CalculatedAt: options.Now, UpdatedAt: options.Now})
		overall = append(overall, domain.ProxyOverallStatus{ProxyID: proxy.ID, OverallAlive: isAlive, LastCheckedAt: checkedAt, CreatedAt: checkedAt, UpdatedAt: options.Now})
	}
	if err := flush(); err != nil {
		return err
	}
	for _, batch := range []struct {
		rows any
		size int
	}{{&latest, 500}, {&overall, 1000}, {&daily, 1000}, {&reputations, 500}} {
		if err := tx.Omit(clause.Associations).CreateInBatches(batch.rows, batch.size).Error; err != nil {
			return err
		}
	}
	ids := make([]uint64, 0, len(proxies))
	for _, proxy := range proxies {
		ids = append(ids, proxy.ID)
	}
	return markProxyDailyChecksBackfilled(tx, ids)
}

func seedDevelopmentTimeline(tx *gorm.DB, workspace domain.Workspace, access []domain.ManagedProxy, options DevelopmentSeedOptions) error {
	var active uint64
	for _, route := range access {
		if route.State == domain.ManagedProxyStateActive {
			active++
		}
	}
	history := make([]domain.ProxyHistory, 0, options.HistoryDays*4)
	snapshots := make([]domain.ProxySnapshot, 0, options.HistoryDays*8)
	for day := options.HistoryDays - 1; day >= 0; day-- {
		for slot := 0; slot < 4; slot++ {
			timestamp := options.Now.Add(-time.Duration(day*24+slot*6) * time.Hour)
			count := int64(len(access)*(options.HistoryDays-day)/(options.HistoryDays+1)) + int64(slot*3)
			count = min(count, int64(len(access)))
			if day == 0 && slot == 0 {
				count = int64(len(access))
			}
			history = append(history, domain.ProxyHistory{WorkspaceID: workspace.ID, ProxyCount: count, CreatedAt: timestamp})
			snapshots = append(snapshots, domain.ProxySnapshot{WorkspaceID: workspace.ID, Metric: domain.ProxySnapshotMetricAlive, Count: count * int64(65+day%15) / 100, CreatedAt: timestamp},
				domain.ProxySnapshot{WorkspaceID: workspace.ID, Metric: domain.ProxySnapshotMetricScraped, Count: int64(15 + (day*19+slot*7)%80), CreatedAt: timestamp})
		}
	}
	if err := tx.CreateInBatches(&history, 1000).Error; err != nil {
		return err
	}
	if err := tx.CreateInBatches(&snapshots, 1000).Error; err != nil {
		return err
	}
	periodStart := time.Date(options.Now.Year(), options.Now.Month(), 1, 0, 0, 0, 0, time.UTC)
	var checkAttempts int64
	if err := tx.Model(&domain.ProxyStatistic{}).Joins("JOIN user_proxies up ON up.proxy_id = proxy_statistics.proxy_id").Where("up.workspace_id = ? AND proxy_statistics.created_at >= ?", workspace.ID, periodStart).Count(&checkAttempts).Error; err != nil {
		return err
	}
	usage := domain.WorkspaceUsagePeriod{WorkspaceID: workspace.ID, PeriodStart: periodStart, PeriodEnd: periodStart.AddDate(0, 1, 0),
		ActiveRoutes: active, PeakActiveRoutes: active, CheckAttempts: uint64(checkAttempts),
		ManagedRequests: uint64(len(access) * 240), ManagedBytes: uint64(len(access)) * 1250000, CreatedAt: periodStart, UpdatedAt: options.Now}
	return tx.Create(&usage).Error
}

func seedDevelopmentAlerts(tx *gorm.DB, workspace domain.Workspace, index int, options DevelopmentSeedOptions, result *DevelopmentSeedResult) error {
	rotators := make([]domain.RotatingProxy, 4)
	for i := range rotators {
		rotators[i] = domain.RotatingProxy{WorkspaceID: workspace.ID, Name: []string{"HTTP general pool", "HTTPS reliable routes", "SOCKS5 authenticated", "SOCKS4 research"}[i],
			InstanceID: "magpie-development-fixtures", InstanceName: "Development fixtures", InstanceRegion: "Local", ProtocolID: []int{1, 2, 4, 3}[i],
			ListenProtocolID: []int{1, 1, 4, 3}[i], ListenPort: uint16(20000 + index*4 + i), TransportProtocol: "tcp", ListenTransportProtocol: "tcp",
			AuthRequired: i == 2, AuthUsername: "demo", AuthPassword: "FixtureRotator12345", ReputationLabels: domain.StringList{}, CreatedAt: workspace.CreatedAt, UpdatedAt: options.Now}
		if i == 1 {
			rotators[i].ReputationLabels = domain.StringList{"good"}
			rotators[i].UptimeFilterType, rotators[i].UptimePercentage = "min", new(float64(90))
		}
		if err := tx.Omit(clause.Associations).Create(&rotators[i]).Error; err != nil {
			return err
		}
	}
	result.Rotators += len(rotators)
	destinations := make([]domain.AlertDestination, 0, 2)
	for i, target := range []string{"alerts@magpie-fixtures.invalid", "https://notifications.magpie-fixtures.invalid/webhook"} {
		encrypted, err := security.EncryptProxySecret(target)
		if err != nil {
			return err
		}
		destination := domain.AlertDestination{WorkspaceID: workspace.ID, Name: []string{"Demo email", "Demo webhook"}[i], Kind: []string{"email", "webhook"}[i],
			Enabled: false, MentionMode: "none", TargetEncrypted: encrypted, CreatedAt: workspace.CreatedAt, UpdatedAt: options.Now}
		if err := tx.Omit(clause.Associations).Create(&destination).Error; err != nil {
			return err
		}
		destinations = append(destinations, destination)
	}
	for i := 0; i < 6; i++ {
		rule := domain.AlertRule{WorkspaceID: workspace.ID, Name: []string{"Pool capacity", "Check success rate", "Latency budget", "Reliable HTTPS capacity", "Paused maintenance rule", "Waiting for samples"}[i],
			Metric:    []string{domain.AlertMetricUsableRoutes, domain.AlertMetricSuccessRate, domain.AlertMetricLatency, domain.AlertMetricUsableRoutes, domain.AlertMetricSuccessRate, domain.AlertMetricLatency}[i],
			Threshold: []float64{250, 90, 1200, 50, 75, 2000}[i], Enabled: i != 4, Revision: 1,
			DestinationIDs: []uint64{destinations[0].ID, destinations[1].ID}, Status: []string{"healthy", "breaching", "healthy", "breaching", "disabled", "unknown"}[i],
			LastValue: new([]float64{600, 76.3, 420, 35, 98, 0}[i]), SampleCount: 300, LastEvaluatedAt: new(options.Now.Add(-time.Minute)), CreatedAt: workspace.CreatedAt, UpdatedAt: options.Now}
		if i == 3 {
			rule.RotatorID = &rotators[1].ID
		}
		if i == 5 {
			rule.UnknownReason, rule.LastValue, rule.SampleCount = "Insufficient samples", nil, 0
		}
		if i == 1 || i == 3 {
			rule.BreachSince = new(options.Now.Add(-time.Hour))
		}
		if err := tx.Omit(clause.Associations).Create(&rule).Error; err != nil {
			return err
		}
		result.AlertRules++
		for n := 0; n < 3; n++ {
			opened := options.Now.Add(-time.Duration(n*48+3) * time.Hour)
			incident := domain.AlertIncident{WorkspaceID: workspace.ID, RuleID: rule.ID, RuleRevision: rule.Revision, RuleName: rule.Name, ScopeName: workspace.Name,
				Metric: rule.Metric, Threshold: rule.Threshold, OpeningValue: []float64{180, 72, 1800, 20, 60, 2500}[i], OpenedAt: opened}
			if rule.RotatorID != nil {
				incident.ScopeName = rotators[1].Name
			}
			if n != 0 || rule.Status != "breaching" {
				incident.ClosedAt, incident.ClosingValue, incident.CloseReason = new(opened.Add(time.Hour)), rule.LastValue, "recovered"
			}
			if err := tx.Omit(clause.Associations).Create(&incident).Error; err != nil {
				return err
			}
			if incident.ClosedAt == nil {
				if err := tx.Model(&domain.AlertRule{}).Where("id = ?", rule.ID).UpdateColumn("active_incident_id", incident.ID).Error; err != nil {
					return err
				}
			}
			delivery := domain.AlertDelivery{WorkspaceID: workspace.ID, IncidentID: incident.ID, DestinationID: destinations[n%2].ID, DestinationName: destinations[n%2].Name,
				Kind: destinations[n%2].Kind, Event: "opened", Payload: `{"development_fixture":true}`, Status: "sent", Attempts: 1,
				NextAttemptAt: opened, LastAttemptAt: new(opened), SentAt: new(opened), CreatedAt: opened}
			if n == 1 {
				delivery.Status, delivery.SentAt, delivery.Attempts, delivery.LastError = "failed", nil, 4, "Simulated delivery failure"
			}
			if err := tx.Omit(clause.Associations).Create(&delivery).Error; err != nil {
				return err
			}
			result.Incidents++
		}
	}
	return nil
}
