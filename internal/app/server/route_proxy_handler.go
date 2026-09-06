package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"magpie/internal/api/dto"
	"magpie/internal/blacklist"
	"magpie/internal/database"
	"magpie/internal/domain"
	proxyqueue "magpie/internal/jobs/queue/proxy"
	"magpie/internal/support"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/charmbracelet/log"

	"gorm.io/gorm"
)

const (
	proxyUploadParseChunkBytes = 1 << 20
	proxyUploadBatchSize       = 5_000
	proxyUploadMaxLineBytes    = 1 << 20
	proxyExportBatchSize       = 2_000
)

var errMissingProxyUploadContent = errors.New("missing proxy upload content")
var getQueuedProxyForUser = database.GetQueuedProxyForUser
var insertProxiesForWorkspace = database.InsertAndGetProxiesWithWorkspace
var removeQueuedProxies = func(proxies []domain.Proxy) error {
	return proxyqueue.PublicProxyQueue.RemoveFromQueue(proxies)
}
var enqueueProxiesNow = func(proxies []domain.Proxy) error {
	return proxyqueue.PublicProxyQueue.AddToQueue(proxies)
}

func addProxies(w http.ResponseWriter, r *http.Request) {
	userID, userErr := workspaceIDFromRequest(r)
	if userErr != nil {
		writeError(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	tagIDs, tagParseErr := parseStrictProxyTagIDs(r.URL.Query()["tagId"])
	if tagParseErr != nil {
		writeError(w, "Invalid proxy tag id", http.StatusBadRequest)
		return
	}
	if tagErr := database.ValidateProxyTags(userID, tagIDs); tagErr != nil {
		writeProxyTagError(w, tagErr)
		return
	}
	startedAt := time.Now()
	maxBodyBytes := resolveUploadMaxBodyBytes()
	insertedCount, parseStats, blacklistedCount, err := ingestProxyUploadMultipartWithTags(w, r, userID, tagIDs, maxBodyBytes)
	if err != nil {
		if errors.Is(err, bufio.ErrTooLong) {
			writeError(w, "Input line exceeds maximum supported length", http.StatusRequestEntityTooLarge)
			return
		}
		if isRequestBodyTooLarge(err) {
			writeError(w, requestBodyTooLargeMessage(maxBodyBytes), http.StatusRequestEntityTooLarge)
			return
		}
		if errors.Is(err, errMissingProxyUploadContent) {
			writeError(w, "Failed to retrieve file", http.StatusBadRequest)
			return
		}
		log.Error("Could not add proxies to database", "error", err)
		writeError(w, "Could not add proxies to database", http.StatusInternalServerError)
		return
	}

	processingMs := time.Since(startedAt).Milliseconds()
	response := dto.AddProxiesResponse{
		ProxyCount: insertedCount,
		Details: dto.AddProxiesDetails{
			SubmittedCount:      parseStats.SubmittedCount,
			ParsedCount:         parseStats.ParsedCount,
			InvalidFormatCount:  parseStats.InvalidFormatCount,
			InvalidAddressCount: parseStats.InvalidAddressCount,
			InvalidIPCount:      parseStats.InvalidIPCount,
			InvalidIPv4Count:    parseStats.InvalidIPv4Count,
			InvalidPortCount:    parseStats.InvalidPortCount,
			BlacklistedCount:    blacklistedCount,
			ProcessingMs:        processingMs,
		},
	}
	writeJSON(w, http.StatusOK, response)
}

func ingestProxyUploadMultipart(w http.ResponseWriter, r *http.Request, userID uint, maxBodyBytes int64) (int, support.ProxyParseStats, int, error) {
	return ingestProxyUploadMultipartWithTags(w, r, userID, nil, maxBodyBytes)
}

func ingestProxyUploadMultipartWithTags(w http.ResponseWriter, r *http.Request, userID uint, tagIDs []uint64, maxBodyBytes int64) (int, support.ProxyParseStats, int, error) {
	if r == nil || r.Body == nil {
		return 0, support.ProxyParseStats{}, 0, errMissingProxyUploadContent
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	multipartReader, err := r.MultipartReader()
	if err != nil {
		return 0, support.ProxyParseStats{}, 0, err
	}

	var (
		stats          support.ProxyParseStats
		insertedCount  int
		blacklistCount int
		batch          []domain.Proxy
		sawInput       bool
	)

	flushBatch := func() error {
		if len(batch) == 0 {
			return nil
		}

		filtered, blocked := blacklist.FilterProxies(batch)
		blacklistCount += len(blocked)
		if len(blocked) > 0 {
			log.Info("Dropped blacklisted proxies from upload", "count", len(blocked))
		}

		batch = batch[:0]
		if len(filtered) == 0 {
			return nil
		}

		inserted, err := insertProxiesForWorkspace(filtered, userID)
		if err != nil {
			return err
		}

		if len(inserted) > 0 {
			proxyIDs := make([]uint64, 0, len(inserted))
			for _, proxy := range inserted {
				proxyIDs = append(proxyIDs, proxy.ID)
			}
			if err := database.AddProxyTagsToProxies(userID, proxyIDs, tagIDs); err != nil {
				return err
			}
			insertedCount += len(inserted)
			database.AsyncEnrichProxyMetadata(inserted)
			if err := proxyqueue.PublicProxyQueue.AddToQueue(inserted); err != nil {
				log.Error("Could not add proxies to queue", "error", err)
			}
		}

		return nil
	}

	flushParseChunk := func(chunk *strings.Builder) error {
		if chunk.Len() == 0 {
			return nil
		}

		parsed, parseStats := support.ParseTextToProxiesWithStats(chunk.String())
		stats.SubmittedCount += parseStats.SubmittedCount
		stats.ParsedCount += parseStats.ParsedCount
		stats.InvalidFormatCount += parseStats.InvalidFormatCount
		stats.InvalidAddressCount += parseStats.InvalidAddressCount
		stats.InvalidIPCount += parseStats.InvalidIPCount
		stats.InvalidIPv4Count += parseStats.InvalidIPv4Count
		stats.InvalidPortCount += parseStats.InvalidPortCount

		batch = append(batch, parsed...)
		chunk.Reset()

		if len(batch) >= proxyUploadBatchSize {
			return flushBatch()
		}
		return nil
	}

	ingestPart := func(source io.Reader) error {
		scanner := bufio.NewScanner(source)
		scanner.Buffer(make([]byte, 64*1024), proxyUploadMaxLineBytes)
		var parseChunk strings.Builder

		for scanner.Scan() {
			parseChunk.WriteString(scanner.Text())
			parseChunk.WriteByte('\n')
			if parseChunk.Len() >= proxyUploadParseChunkBytes {
				if err := flushParseChunk(&parseChunk); err != nil {
					return err
				}
			}
		}
		if err := scanner.Err(); err != nil {
			return err
		}
		return flushParseChunk(&parseChunk)
	}

	for {
		part, err := multipartReader.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, support.ProxyParseStats{}, 0, err
		}

		formName := strings.TrimSpace(part.FormName())
		switch formName {
		case "file", "proxyTextarea", "clipboardProxies":
			sawInput = true
			if name := strings.TrimSpace(part.FileName()); name != "" {
				log.Debugf("Uploaded file: %s", name)
			}
			if err := ingestPart(part); err != nil {
				_ = part.Close()
				return 0, support.ProxyParseStats{}, 0, err
			}
		default:
			if _, err := io.Copy(io.Discard, part); err != nil {
				_ = part.Close()
				return 0, support.ProxyParseStats{}, 0, err
			}
		}

		if err := part.Close(); err != nil {
			return 0, support.ProxyParseStats{}, 0, err
		}
	}

	if !sawInput {
		return 0, support.ProxyParseStats{}, 0, errMissingProxyUploadContent
	}
	if err := flushBatch(); err != nil {
		return 0, support.ProxyParseStats{}, 0, err
	}

	return insertedCount, stats, blacklistCount, nil
}

func getProxyPage(w http.ResponseWriter, r *http.Request) {
	userID, userErr := workspaceIDFromRequest(r)
	if userErr != nil {
		writeError(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	page, err := strconv.Atoi(r.PathValue("page"))
	if err != nil {
		log.Error("error converting page to int", "error", err.Error())
		writeError(w, "Invalid page", http.StatusBadRequest)
		return
	}

	pageSize := 0
	if rawPageSize := r.URL.Query().Get("pageSize"); rawPageSize != "" {
		if parsedPageSize, parseErr := strconv.Atoi(rawPageSize); parseErr == nil && parsedPageSize > 0 {
			pageSize = parsedPageSize
		}
	}

	search := strings.TrimSpace(r.URL.Query().Get("search"))

	status := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("status")))
	if status != "alive" && status != "dead" {
		status = ""
	}

	filters := dto.ProxyListFilters{
		Status:           status,
		Protocols:        normalizeQueryList(r.URL.Query()["protocol"]),
		MinHealthOverall: parseHealthPercentParam(r.URL.Query().Get("minHealthOverall")),
		MinHealthHTTP:    parseHealthPercentParam(r.URL.Query().Get("minHealthHttp")),
		MinHealthHTTPS:   parseHealthPercentParam(r.URL.Query().Get("minHealthHttps")),
		MinHealthSOCKS4:  parseHealthPercentParam(r.URL.Query().Get("minHealthSocks4")),
		MinHealthSOCKS5:  parseHealthPercentParam(r.URL.Query().Get("minHealthSocks5")),
		Countries:        normalizeQueryList(r.URL.Query()["country"]),
		Types:            normalizeQueryList(r.URL.Query()["type"]),
		AnonymityLevels:  normalizeQueryList(r.URL.Query()["anonymity"]),
		MaxTimeout:       parsePositiveIntParam(r.URL.Query().Get("maxTimeout")),
		MaxRetries:       parsePositiveIntParam(r.URL.Query().Get("maxRetries")),
		ReputationLabels: normalizeQueryList(r.URL.Query()["reputation"]),
		TagIDs:           parseProxyTagFilterIDs(r.URL.Query()["tagId"]),
	}

	includeHealth := parseBoolQueryParam(r.URL.Query().Get("includeHealth"), true)
	includeReputation := parseBoolQueryParam(r.URL.Query().Get("includeReputation"), true)
	sortField := strings.TrimSpace(r.URL.Query().Get("sortField"))
	sortOrder := strings.TrimSpace(r.URL.Query().Get("sortOrder"))

	proxies, total := database.GetProxyInfoPageWithFiltersAndOptions(
		userID,
		page,
		pageSize,
		search,
		filters,
		database.ProxyPageQueryOptions{
			IncludeHealth:     includeHealth,
			IncludeReputation: includeReputation,
			SortField:         sortField,
			SortOrder:         sortOrder,
		},
	)

	response := dto.ProxyPage{
		Proxies: proxies,
		Total:   total,
	}

	writeJSON(w, http.StatusOK, response)
}

func getProxyFilters(w http.ResponseWriter, r *http.Request) {
	userID, userErr := workspaceIDFromRequest(r)
	if userErr != nil {
		writeError(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	options, err := database.GetProxyFilterOptions(userID)
	if err != nil {
		log.Error("error retrieving proxy filters", "error", err.Error())
		writeError(w, "Failed to retrieve proxy filters", http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusOK, options)
}

func normalizeQueryList(values []string) []string {
	if len(values) == 0 {
		return nil
	}

	normalized := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))

	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		lower := strings.ToLower(trimmed)
		if _, exists := seen[lower]; exists {
			continue
		}
		seen[lower] = struct{}{}
		normalized = append(normalized, lower)
	}

	return normalized
}

func parsePositiveIntParam(value string) int {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return 0
	}
	parsed, err := strconv.Atoi(trimmed)
	if err != nil || parsed <= 0 {
		return 0
	}
	return parsed
}

func parseHealthPercentParam(value string) int {
	parsed := parsePositiveIntParam(value)
	if parsed > 100 {
		return 100
	}
	return parsed
}

func parseBoolQueryParam(value string, defaultValue bool) bool {
	trimmed := strings.TrimSpace(strings.ToLower(value))
	if trimmed == "" {
		return defaultValue
	}

	switch trimmed {
	case "1", "true", "yes", "y", "on":
		return true
	case "0", "false", "no", "n", "off":
		return false
	default:
		return defaultValue
	}
}

func getProxyCount(w http.ResponseWriter, r *http.Request) {
	userID, userErr := workspaceIDFromRequest(r)
	if userErr != nil {
		writeError(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	writeJSON(w, http.StatusOK, database.GetAllProxyCountOfUser(userID))
}

func getProxyDetail(w http.ResponseWriter, r *http.Request) {
	userID, userErr := workspaceIDFromRequest(r)
	if userErr != nil {
		writeError(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	proxyID, err := strconv.ParseUint(r.PathValue("id"), 10, 64)
	if err != nil {
		log.Error("error converting proxy id", "error", err.Error())
		writeError(w, "Invalid proxy id", http.StatusBadRequest)
		return
	}

	detail, dbErr := database.GetProxyDetail(userID, proxyID)
	if dbErr != nil {
		log.Error("error retrieving proxy detail", "error", dbErr.Error(), "proxy_id", proxyID)
		writeError(w, "Failed to retrieve proxy", http.StatusInternalServerError)
		return
	}

	if detail == nil {
		writeError(w, "Proxy not found", http.StatusNotFound)
		return
	}

	writeJSON(w, http.StatusOK, detail)
}

func updateManagedProxyLifecycle(w http.ResponseWriter, r *http.Request) {
	workspaceID, workspaceErr := workspaceIDFromRequest(r)
	if workspaceErr != nil {
		writeWorkspaceAccessError(w, workspaceErr)
		return
	}
	proxyID, err := strconv.ParseUint(r.PathValue("id"), 10, 64)
	if err != nil || proxyID == 0 {
		writeError(w, "Invalid proxy id", http.StatusBadRequest)
		return
	}
	var payload dto.ManagedProxyLifecycleRequest
	if !decodeJSONBodyLimited(w, r, &payload, resolveJSONMaxBodyBytes()) {
		return
	}
	if err := database.SetManagedProxyState(workspaceID, proxyID, payload.State); err != nil {
		switch {
		case errors.Is(err, database.ErrWorkspaceCapacityReached):
			writeError(w, err.Error(), http.StatusConflict)
		case errors.Is(err, gorm.ErrRecordNotFound):
			writeError(w, "Proxy not found", http.StatusNotFound)
		default:
			writeError(w, err.Error(), http.StatusBadRequest)
		}
		return
	}

	proxy, active, err := database.GetProxyQueueState(proxyID)
	if err != nil {
		writeError(w, "Lifecycle changed, but queue synchronization failed", http.StatusServiceUnavailable)
		return
	}
	if proxy != nil {
		if active {
			err = enqueueProxiesNow([]domain.Proxy{*proxy})
		} else {
			err = removeQueuedProxies([]domain.Proxy{*proxy})
		}
	}
	if err != nil {
		writeError(w, "Lifecycle changed, but queue synchronization failed", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func requeueProxy(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	userID, userErr := workspaceIDFromRequest(r)
	if userErr != nil {
		writeError(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	proxyID, err := strconv.ParseUint(r.PathValue("id"), 10, 64)
	if err != nil {
		log.Error("error converting proxy id", "error", err.Error())
		writeError(w, "Invalid proxy id", http.StatusBadRequest)
		return
	}

	proxy, dbErr := getQueuedProxyForUser(userID, proxyID)
	if dbErr != nil {
		log.Error("error retrieving proxy for requeue", "error", dbErr.Error(), "proxy_id", proxyID, "user_id", userID)
		writeError(w, "Failed to retrieve proxy", http.StatusInternalServerError)
		return
	}

	if proxy == nil {
		writeError(w, "Proxy not found", http.StatusNotFound)
		return
	}

	if err := removeQueuedProxies([]domain.Proxy{*proxy}); err != nil {
		log.Error("failed to remove proxy from queue before requeue", "error", err, "proxy_id", proxyID, "user_id", userID)
		writeError(w, "Failed to queue proxy", http.StatusServiceUnavailable)
		return
	}

	if err := enqueueProxiesNow([]domain.Proxy{*proxy}); err != nil {
		log.Error("failed to queue proxy", "error", err, "proxy_id", proxyID, "user_id", userID)
		writeError(w, "Failed to queue proxy", http.StatusServiceUnavailable)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"message":  "Proxy queued successfully",
		"proxy_id": proxyID,
	})
}

func getProxyStatistics(w http.ResponseWriter, r *http.Request) {
	userID, userErr := workspaceIDFromRequest(r)
	if userErr != nil {
		writeError(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	proxyID, err := strconv.ParseUint(r.PathValue("id"), 10, 64)
	if err != nil {
		log.Error("error converting proxy id", "error", err.Error())
		writeError(w, "Invalid proxy id", http.StatusBadRequest)
		return
	}

	limit := 100
	if rawLimit := strings.TrimSpace(r.URL.Query().Get("limit")); rawLimit != "" {
		if parsed, parseErr := strconv.Atoi(rawLimit); parseErr == nil && parsed > 0 {
			limit = parsed
		}
	}

	statistics, dbErr := database.GetProxyStatistics(userID, proxyID, limit)
	if dbErr != nil {
		log.Error("error retrieving proxy statistics", "error", dbErr.Error(), "proxy_id", proxyID)
		writeError(w, "Failed to retrieve proxy statistics", http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"statistics": statistics})
}

func getProxyStatisticResponseBody(w http.ResponseWriter, r *http.Request) {
	userID, userErr := workspaceIDFromRequest(r)
	if userErr != nil {
		writeError(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	proxyID, err := strconv.ParseUint(r.PathValue("id"), 10, 64)
	if err != nil {
		log.Error("error converting proxy id", "error", err.Error())
		writeError(w, "Invalid proxy id", http.StatusBadRequest)
		return
	}

	statisticID, err := strconv.ParseUint(r.PathValue("statisticId"), 10, 64)
	if err != nil {
		log.Error("error converting statistic id", "error", err.Error())
		writeError(w, "Invalid statistic id", http.StatusBadRequest)
		return
	}

	responseDetail, dbErr := database.GetProxyStatisticResponseBody(userID, proxyID, statisticID)
	if dbErr != nil {
		if errors.Is(dbErr, gorm.ErrRecordNotFound) {
			writeError(w, "Proxy statistic not found", http.StatusNotFound)
			return
		}

		log.Error("error retrieving proxy statistic body", "error", dbErr.Error(), "proxy_id", proxyID, "statistic_id", statisticID)
		writeError(w, "Failed to retrieve proxy statistic body", http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusOK, responseDetail)
}

func deleteProxies(w http.ResponseWriter, r *http.Request) {
	userID, userErr := workspaceIDFromRequest(r)
	if userErr != nil {
		writeError(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	var body json.RawMessage
	if !decodeJSONBodyLimited(w, r, &body, resolveJSONMaxBodyBytes()) {
		return
	}
	body = bytes.TrimSpace(body)

	if len(body) == 0 {
		writeError(w, "Invalid request", http.StatusBadRequest)
		return
	}

	if body[0] == '[' {
		var proxies []int
		if err := json.Unmarshal(body, &proxies); err != nil {
			writeError(w, "Invalid request", http.StatusBadRequest)
			return
		}

		if len(proxies) == 0 {
			writeError(w, "No proxies selected for deletion", http.StatusBadRequest)
			return
		}

		deleted, orphaned, deleteErr := database.DeleteProxyRelation(userID, proxies)
		if deleteErr != nil {
			log.Error("could not delete proxies", "error", deleteErr.Error())
			writeError(w, "Could not delete proxies", http.StatusInternalServerError)
			return
		}

		if len(orphaned) > 0 {
			if err := proxyqueue.PublicProxyQueue.RemoveFromQueue(orphaned); err != nil {
				log.Error("failed to remove orphaned proxies from queue", "error", err)
			}
		}

		if deleted == 0 {
			writeJSON(w, http.StatusOK, "No proxies matched the delete criteria.")
			return
		}

		writeJSON(w, http.StatusOK, fmt.Sprintf("Deleted %d proxies.", deleted))
		return
	}

	var settings dto.DeleteSettings
	if err := json.Unmarshal(body, &settings); err != nil {
		writeError(w, "Invalid request", http.StatusBadRequest)
		return
	}

	if settings.Scope != "all" && settings.Scope != "selected" {
		settings.Scope = "all"
	}

	deleted, orphaned, deleteErr := database.DeleteProxiesWithSettings(userID, settings)
	if deleteErr != nil {
		if errors.Is(deleteErr, database.ErrNoProxiesSelected) {
			writeError(w, "No proxies selected for deletion", http.StatusBadRequest)
			return
		}

		log.Error("could not delete proxies with filters", "error", deleteErr.Error())
		writeError(w, "Could not delete proxies", http.StatusInternalServerError)
		return
	}

	if len(orphaned) > 0 {
		if err := proxyqueue.PublicProxyQueue.RemoveFromQueue(orphaned); err != nil {
			log.Error("failed to remove orphaned proxies from queue", "error", err)
		}
	}

	if deleted == 0 {
		writeJSON(w, http.StatusOK, "No proxies matched the delete criteria.")
		return
	}

	writeJSON(w, http.StatusOK, fmt.Sprintf("Deleted %d proxies.", deleted))
}

func exportProxies(w http.ResponseWriter, r *http.Request) {
	userID, userErr := workspaceIDFromRequest(r)
	if userErr != nil {
		writeError(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	var settings dto.ExportSettings
	if !decodeJSONBodyLimited(w, r, &settings, resolveJSONMaxBodyBytes()) {
		return
	}

	timeout := time.Duration(resolvePositiveEnvInt("PROXY_EXPORT_TIMEOUT_SECONDS", 300)) * time.Second
	writeProxyExport(w, r, timeout, settings.OutputFormat, func(ctx context.Context, consume func([]domain.Proxy) error) error {
		return database.StreamProxiesForExport(ctx, userID, settings, proxyExportBatchSize, consume)
	})
}

// The context bounds database work; the additional write grace allows a timeout
// error to reach the client when no export bytes have been sent yet.
func writeProxyExport(w http.ResponseWriter, r *http.Request, timeout time.Duration, outputFormat string, stream func(context.Context, func([]domain.Proxy) error) error) {
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()
	deadline, _ := ctx.Deadline()
	response := newStatusRecorder(w)
	controller := http.NewResponseController(response)
	if err := controller.SetWriteDeadline(deadline.Add(5 * time.Second)); err != nil {
		handleExportProxiesStreamError(response, err, false)
		return
	}

	response.Header().Set("Content-Type", "text/plain")
	response.Header().Set("Content-Disposition", "attachment; filename=proxies.txt")
	response.Header().Set("X-Accel-Buffering", "no")
	writer := bufio.NewWriterSize(response, 256*1024)
	err := stream(ctx, func(proxies []domain.Proxy) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if len(proxies) == 0 {
			return nil
		}
		for _, proxy := range proxies {
			if err := ctx.Err(); err != nil {
				return err
			}
			if _, err := writer.WriteString(support.FormatProxy(proxy, outputFormat) + "\n"); err != nil {
				return err
			}
		}
		if err := writer.Flush(); err != nil {
			return err
		}
		return controller.Flush()
	})
	if ctx.Err() != nil {
		err = ctx.Err()
	}
	if err != nil {
		handleExportProxiesStreamError(response, err, response.HeaderWritten())
		return
	}
	if err := writer.Flush(); err != nil {
		handleExportProxiesStreamError(response, err, response.HeaderWritten())
	}
}

func handleExportProxiesStreamError(w http.ResponseWriter, err error, wroteBytes bool) {
	log.Error("export proxies stream failed", "error", err)
	if wroteBytes {
		// A normal return would terminate the chunked response successfully and
		// let the browser save a partial file. Preserve an interrupted transfer.
		panic(http.ErrAbortHandler)
	}
	w.Header().Del("Content-Disposition")
	if errors.Is(err, context.DeadlineExceeded) {
		writeError(w, "Export timed out. Try exporting a smaller selection.", http.StatusGatewayTimeout)
		return
	}
	writeError(w, "Could not export proxies", http.StatusInternalServerError)
}
