package database

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"magpie/internal/checkerconfig"
	"magpie/internal/domain"

	"github.com/charmbracelet/log"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const maxParamsPerBatch = 65534 // Conservative default (PostgreSQL's limit) - 1

const minBatchSize = 100 // Minimum batch size to maintain efficiency

// InsertAndGetProxiesWithWorkspace deduplicates exact routes and commits their
// workspace management in one transaction. Existing lifecycle state survives
// reimport; new routes beyond capacity are retained as paused managed proxies.
// Results include all active workspaces for queue publication, but this function
// does not enqueue them. Loading those results happens after commit, so a load
// error does not imply the import rolled back.
func InsertAndGetProxiesWithWorkspace(proxies []domain.Proxy, workspaceIDs ...uint) ([]domain.Proxy, error) {
	inserted, err := insertAndAssociateProxies(proxies, workspaceIDs)
	if err != nil || len(inserted) == 0 {
		return inserted, err
	}

	proxiesWithWorkspaces, err := fetchProxiesWithWorkspaces(DB, inserted)
	if err != nil {
		return nil, fmt.Errorf("load imported proxies with workspaces: %w", err)
	}

	for _, workspaceID := range workspaceIDs {
		checkerconfig.TagAssignmentsChanged(workspaceID)
	}
	return proxiesWithWorkspaces, nil
}

func insertAndAssociateProxies(proxies []domain.Proxy, workspaceIDs []uint) ([]domain.Proxy, error) {
	if len(proxies) == 0 || len(workspaceIDs) == 0 {
		return nil, nil
	}

	workspaceIDs = normalizeWorkspaceIDs(workspaceIDs)
	if len(workspaceIDs) == 0 {
		return nil, nil
	}

	uniqueProxies, err := deduplicateProxies(proxies)
	if err != nil {
		return nil, err
	}
	if len(uniqueProxies) == 0 {
		return nil, nil
	}

	tx := DB.Begin()
	if tx.Error != nil {
		return nil, tx.Error
	}
	defer transactionRollbackHandler(tx)

	associationPlans := make(map[uint]map[string]managedProxyStatePlan, len(workspaceIDs))
	for _, workspaceID := range workspaceIDs {
		plan, err := planWorkspaceManagedProxyStates(tx, workspaceID, uniqueProxies)
		if err != nil {
			tx.Rollback()
			return nil, fmt.Errorf("plan workspace %d proxy states: %w", workspaceID, err)
		}
		associationPlans[workspaceID] = plan
	}

	if err := insertProxies(tx, uniqueProxies); err != nil {
		tx.Rollback()
		return nil, fmt.Errorf("insert %d proxy routes: %w", len(uniqueProxies), err)
	}

	if err := ensureProxyIDs(tx, uniqueProxies); err != nil {
		tx.Rollback()
		return nil, fmt.Errorf("resolve proxy IDs: %w", err)
	}

	hashToID := make(map[string]uint64, len(uniqueProxies))
	hashToProxy := make(map[string]domain.Proxy, len(uniqueProxies))
	for i := range uniqueProxies {
		key := string(uniqueProxies[i].Hash)
		hashToID[key] = uniqueProxies[i].ID
		hashToProxy[key] = uniqueProxies[i]
	}

	for _, workspaceID := range workspaceIDs {
		plan := associationPlans[workspaceID]
		workspaceProxies := make([]domain.Proxy, 0, len(plan))
		for hash := range plan {
			if id, ok := hashToID[hash]; ok {
				proxy := hashToProxy[hash]
				proxy.ID = id
				workspaceProxies = append(workspaceProxies, proxy)
			}
		}
		if len(workspaceProxies) == 0 {
			continue
		}

		if err := createWorkspaceAssociations(tx, workspaceProxies, workspaceID, plan); err != nil {
			tx.Rollback()
			return nil, fmt.Errorf("associate %d proxies with workspace %d: %w", len(workspaceProxies), workspaceID, err)
		}
	}

	if err := tx.Commit().Error; err != nil {
		return nil, err
	}

	return uniqueProxies, nil
}

type managedProxyStatePlan struct {
	State       string
	PauseReason string
}

func planWorkspaceManagedProxyStates(tx *gorm.DB, workspaceID uint, proxies []domain.Proxy) (map[string]managedProxyStatePlan, error) {
	if tx == nil || workspaceID == 0 {
		return nil, errors.New("workspace is required")
	}

	var subscription domain.WorkspaceSubscription
	subscriptionErr := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("workspace_id = ?", workspaceID).First(&subscription).Error
	if subscriptionErr != nil && !errors.Is(subscriptionErr, gorm.ErrRecordNotFound) {
		return nil, subscriptionErr
	}
	if errors.Is(subscriptionErr, gorm.ErrRecordNotFound) {
		subscription = domain.WorkspaceSubscription{WorkspaceID: workspaceID, OverageMode: domain.WorkspaceOverageUnlimited}
	}

	type existingRow struct {
		Hash        []byte
		State       string
		PauseReason string
	}
	hashes := make([][]byte, 0, len(proxies))
	for _, proxy := range proxies {
		hashes = append(hashes, proxy.Hash)
	}
	existing := make(map[string]managedProxyStatePlan)
	// Reserve one parameter for workspace_id in addition to the route hashes.
	const lookupBatchSize = maxParamsPerBatch - 1
	for start := 0; start < len(hashes); start += lookupBatchSize {
		end := min(start+lookupBatchSize, len(hashes))
		var existingRows []existingRow
		if err := tx.Table("user_proxies up").
			Select("p.hash, up.state, up.pause_reason").
			Joins("JOIN proxies p ON p.id = up.proxy_id").
			Where("up.workspace_id = ? AND p.hash IN ?", workspaceID, hashes[start:end]).
			Scan(&existingRows).Error; err != nil {
			return nil, err
		}
		for _, row := range existingRows {
			existing[string(row.Hash)] = managedProxyStatePlan{State: row.State, PauseReason: row.PauseReason}
		}
	}
	// Reimporting only existing management does not allocate capacity. Keep the
	// subscription lock and lifecycle choices, but avoid scanning the inventory.
	if len(existing) == len(proxies) {
		return existing, nil
	}

	limit, unlimited := subscription.ActivationLimit()
	var available uint64
	if !unlimited {
		var activeCount int64
		if err := tx.Model(&domain.ManagedProxy{}).
			Where("workspace_id = ? AND state = ?", workspaceID, domain.ManagedProxyStateActive).
			Count(&activeCount).Error; err != nil {
			return nil, err
		}
		if activeCount < int64(limit) {
			available = limit - uint64(activeCount)
		}
	}

	plan := make(map[string]managedProxyStatePlan, len(proxies))
	for _, proxy := range proxies {
		key := string(proxy.Hash)
		if current, ok := existing[key]; ok {
			plan[key] = current
			continue
		}
		if unlimited || available > 0 {
			plan[key] = managedProxyStatePlan{State: domain.ManagedProxyStateActive}
			if !unlimited {
				available--
			}
			continue
		}
		plan[key] = managedProxyStatePlan{State: domain.ManagedProxyStatePaused, PauseReason: domain.ManagedProxyPauseReasonCapacity}
	}
	return plan, nil
}

func deduplicateProxies(proxies []domain.Proxy) ([]domain.Proxy, error) {
	seen := make(map[string]struct{}, len(proxies))
	unique := make([]domain.Proxy, 0, len(proxies))
	for _, p := range proxies {
		if err := p.GenerateHash(); err != nil {
			return nil, err
		}
		key := string(p.Hash)
		if _, exists := seen[key]; !exists {
			seen[key] = struct{}{}
			unique = append(unique, p)
		}
	}
	return unique, nil
}

// Size inserts for their own table, including small imports. A row count that
// fits proxies can exceed the parameter budget for wider managed-proxy rows.
// Counting every mapped column also reserves room for explicitly supplied IDs.
func calculateCreateBatchSize(tx *gorm.DB, model any, rowCount int) (int, error) {
	if rowCount <= 0 {
		return 0, nil
	}
	numFields, err := getNumDatabaseFields(model, tx)
	if err != nil {
		return 0, err
	}
	if numFields <= 0 || numFields > maxParamsPerBatch {
		return 0, fmt.Errorf("cannot batch insert with %d database fields", numFields)
	}
	return min(maxParamsPerBatch/numFields, rowCount), nil
}

func insertProxies(tx *gorm.DB, proxies []domain.Proxy) error {
	if len(proxies) == 0 {
		return nil
	}
	batchSize, err := calculateCreateBatchSize(tx, domain.Proxy{}, len(proxies))
	if err != nil {
		return err
	}
	return tx.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "hash"}},
		DoUpdates: clause.AssignmentColumns([]string{"hash"}), // Return IDs for existing routes.
	}).CreateInBatches(proxies, batchSize).Error
}

func ensureProxyIDs(tx *gorm.DB, proxies []domain.Proxy) error {
	var missing [][]byte
	for _, proxy := range proxies {
		if proxy.ID == 0 {
			missing = append(missing, proxy.Hash)
		}
	}

	if len(missing) == 0 {
		return nil
	}

	lookup := make(map[string]uint64, len(missing))
	for start := 0; start < len(missing); start += maxParamsPerBatch {
		end := min(start+maxParamsPerBatch, len(missing))
		var results []struct {
			ID   uint64
			Hash []byte
		}
		if err := tx.Model(&domain.Proxy{}).
			Select("id, hash").
			Where("hash IN ?", missing[start:end]).
			Find(&results).Error; err != nil {
			return err
		}
		for _, r := range results {
			lookup[string(r.Hash)] = r.ID
		}
	}

	for i, proxy := range proxies {
		if proxy.ID != 0 {
			continue
		}
		if id, ok := lookup[string(proxy.Hash)]; ok {
			proxies[i].ID = id
		}
	}

	return nil
}

func createWorkspaceAssociations(tx *gorm.DB, proxies []domain.Proxy, workspaceID uint, plans map[string]managedProxyStatePlan) error {
	if len(proxies) == 0 {
		return nil
	}

	batchSize, err := calculateCreateBatchSize(tx, domain.ManagedProxy{}, len(proxies))
	if err != nil {
		return err
	}

	associations := make([]domain.ManagedProxy, len(proxies))
	proxyIDs := make([]uint64, len(proxies))
	for i, proxy := range proxies {
		proxyIDs[i] = proxy.ID
		statePlan := plans[string(proxy.Hash)]
		now := time.Now().UTC()
		var activatedAt, pausedAt *time.Time
		if statePlan.State == domain.ManagedProxyStateActive {
			activatedAt = &now
		} else if statePlan.State == domain.ManagedProxyStatePaused {
			pausedAt = &now
		}
		associations[i] = domain.ManagedProxy{
			WorkspaceID: workspaceID,
			ProxyID:     proxy.ID,
			Username:    proxy.Username,
			Password:    proxy.Password,
			State:       statePlan.State,
			PauseReason: statePlan.PauseReason,
			ActivatedAt: activatedAt,
			PausedAt:    pausedAt,
		}
	}

	// Prepare credentials once. The conflict probe uses a copy because RETURNING
	// can populate only the newly inserted rows; credential updates need the
	// original complete input. Neither statement reruns the encryption hook.
	for i := range associations {
		if err := associations[i].BeforeSave(tx); err != nil {
			return err
		}
	}
	newAssociations := append([]domain.ManagedProxy(nil), associations...)
	inserted := tx.Session(&gorm.Session{SkipHooks: true}).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "workspace_id"}, {Name: "proxy_id"}},
		DoNothing: true,
	}).CreateInBatches(newAssociations, batchSize)
	if inserted.Error != nil {
		return inserted.Error
	}
	if inserted.RowsAffected < int64(len(associations)) {
		if err := updateWorkspaceAssociationCredentials(tx, associations, workspaceID, batchSize); err != nil {
			return err
		}
	}
	if err := refreshWorkspaceProxyFilterIndexesForWorkspaceProxyIDs(tx, workspaceID, proxyIDs); err != nil {
		return err
	}
	if err := refreshWorkspaceScrapeSourceStatsForWorkspaceProxyIDs(tx, workspaceID, proxyIDs); err != nil {
		return err
	}
	if inserted.RowsAffected == 0 {
		return nil
	}
	return refreshWorkspaceUsageActiveRoutes(tx, workspaceID)
}

// Only the insert above creates management. An UPDATE cannot resurrect a row
// deleted concurrently after its conflict check, so RowsAffected remains an
// accurate reason to refresh capacity. Lifecycle columns are never changed.
func updateWorkspaceAssociationCredentials(tx *gorm.DB, associations []domain.ManagedProxy, workspaceID uint, batchSize int) error {
	for start := 0; start < len(associations); start += batchSize {
		end := min(start+batchSize, len(associations))
		var query strings.Builder
		query.WriteString("WITH credentials(proxy_id, username, password) AS (VALUES ")
		args := make([]any, 0, (end-start)*3+1)
		for i := start; i < end; i++ {
			if i > start {
				query.WriteByte(',')
			}
			query.WriteString("(CAST(? AS bigint), CAST(? AS text), CAST(? AS text))")
			args = append(args, associations[i].ProxyID, associations[i].UsernameEncrypted, associations[i].PasswordEncrypted)
		}
		query.WriteString(`) UPDATE user_proxies AS managed
			SET username = credentials.username, password = credentials.password
			FROM credentials
			WHERE managed.workspace_id = ? AND managed.proxy_id = credentials.proxy_id
			AND (managed.username IS DISTINCT FROM credentials.username OR managed.password IS DISTINCT FROM credentials.password)`)
		args = append(args, workspaceID)
		if err := tx.Exec(query.String(), args...).Error; err != nil {
			return err
		}
	}
	return nil
}

func fetchProxiesWithWorkspaces(tx *gorm.DB, proxies []domain.Proxy) ([]domain.Proxy, error) {
	ids := make([]uint64, len(proxies))
	for i, p := range proxies {
		ids[i] = p.ID
	}

	var results []domain.Proxy
	credentialsByID := make(map[uint64]domain.Proxy, len(proxies))
	for _, proxy := range proxies {
		credentialsByID[proxy.ID] = proxy
	}
	for i := 0; i < len(ids); i += maxParamsPerBatch {
		end := i + maxParamsPerBatch
		if end > len(ids) {
			end = len(ids)
		}

		var batch []domain.Proxy
		err := tx.
			Where("id IN ?", ids[i:end]).
			Find(&batch).Error
		if err != nil {
			return nil, err
		}
		if err := hydrateActiveProxyWorkspaces(tx, batch); err != nil {
			return nil, err
		}
		for idx := range batch {
			if original, ok := credentialsByID[batch[idx].ID]; ok {
				batch[idx].Username = original.Username
				batch[idx].Password = original.Password
			}
		}
		results = append(results, batch...)
	}
	return results, nil
}

func transactionRollbackHandler(tx *gorm.DB) {
	if r := recover(); r != nil {
		tx.Rollback()
		log.Errorf("Transaction rolled back due to panic: %v", r)
	}
}

func getNumDatabaseFields(model interface{}, db *gorm.DB) (int, error) {
	stmt := &gorm.Statement{DB: db}
	if err := stmt.Parse(model); err != nil {
		return 0, err
	}
	return len(stmt.Schema.DBNames), nil
}
