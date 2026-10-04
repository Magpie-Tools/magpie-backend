// Package checkerconfig holds complete, precompiled checker settings. Its loader
// runs at startup, on mutations and during reconciliation, never on a check.
package checkerconfig

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"magpie/internal/api/dto"
	"magpie/internal/domain"

	"github.com/charmbracelet/log"
	"github.com/redis/go-redis/v9"
)

type Plan struct {
	Settings [4]dto.ProtocolCheckSettings
	Keys     [4]string
	Judges   [4][]domain.JudgeWithRegex
	Headers  []string
	counters [4]atomic.Uint64
}

func (p *Plan) NextJudge(index int) (*domain.Judge, string) {
	list := p.Judges[index]
	if len(list) == 0 {
		return nil, ""
	}
	item := list[(p.counters[index].Add(1)-1)%uint64(len(list))]
	return item.Judge, item.Regex
}

type Workspace struct {
	Value          domain.Workspace
	Default        *Plan
	Proxies        *ProxyPlans
	InputSignature string
	BaseSignature  string
	Generation     uint64
}

func (w *Workspace) Plan(proxyID uint64) *Plan {
	if plan := w.Proxies.Get(proxyID); plan != nil {
		return plan
	}
	return w.Default
}

// Build precomputes identities outside the hot path. Only memberships of tags
// that actually have checker rules are supplied by the authoritative loader.
func Build(value domain.Workspace, assignments []domain.ProxyTagAssignment, judges []domain.JudgeWithRegex, headers []string) *Workspace {
	settings := value.DefaultCheckerSettings()
	w := &Workspace{Value: value, Proxies: &ProxyPlans{}, Generation: value.CheckerGeneration}
	configured := make(map[uint64]bool, len(settings.Rules))
	for _, rule := range settings.Rules {
		configured[rule.TagID] = true
	}
	// Group identifiers in place rather than retaining one tag map per route.
	// The loader owns this slice and does not reuse it after compilation.
	if !sort.SliceIsSorted(assignments, func(i, j int) bool { return assignments[i].ProxyID < assignments[j].ProxyID }) {
		sort.Slice(assignments, func(i, j int) bool { return assignments[i].ProxyID < assignments[j].ProxyID })
	}
	compiled := make(map[[4]dto.ProtocolCheckSettings]*Plan)
	compile := func(resolved [4]dto.ProtocolCheckSettings) *Plan {
		if previous := compiled[resolved]; previous != nil {
			return previous
		}
		p := &Plan{Settings: resolved, Headers: append([]string{}, headers...)}
		for index, setting := range resolved {
			if !setting.Enabled {
				continue
			}
			protocol := domain.CheckerProtocols[index]
			scheme := protocol
			if index >= 2 {
				scheme = "http"
				if value.UseHttpsForSocks {
					scheme = "https"
				}
			}
			if setting.Transport == "quic" || setting.Transport == "http3" {
				scheme = "https"
			}
			for _, judge := range judges {
				if judge.Judge != nil && judge.Judge.GetScheme() == scheme {
					p.Judges[index] = append(p.Judges[index], judge)
				}
			}
			sort.Slice(p.Judges[index], func(i, j int) bool { return p.Judges[index][i].Judge.ID < p.Judges[index][j].Judge.ID })
			var identity strings.Builder
			fmt.Fprintf(&identity, "%s\x00%s\x00%d\x00%d", protocol, setting.Transport, setting.Timeout, setting.Retries)
			for _, judge := range p.Judges[index] {
				fmt.Fprintf(&identity, "\x00%d\x00%q\x00%q", judge.Judge.ID, judge.Judge.FullString, judge.Regex)
			}
			for _, header := range headers {
				fmt.Fprintf(&identity, "\x00%q", header)
			}
			digest := sha256.Sum256([]byte(identity.String()))
			p.Keys[index] = hex.EncodeToString(digest[:])
		}
		compiled[resolved] = p
		return p
	}
	// Only small configuration inputs participate in these signatures. They
	// let unchanged reconciliation skip assignment and projection reads.
	identity := struct {
		Defaults   dto.CheckerProfileSettings
		SocksHTTPS bool
		Headers    []string
		Judges     []string
	}{Defaults: settings.Defaults, SocksHTTPS: value.UseHttpsForSocks, Headers: headers}
	for _, judge := range judges {
		if judge.Judge != nil {
			identity.Judges = append(identity.Judges, fmt.Sprintf("%d %q %q", judge.Judge.ID, judge.Judge.FullString, judge.Regex))
		}
	}
	sort.Strings(identity.Judges)
	encoded, _ := json.Marshal(identity)
	w.BaseSignature = string(encoded)
	rules, _ := json.Marshal(settings.Rules)
	policy, _ := json.Marshal([]any{value.AutoRemoveFailingProxies, value.AutoRemoveFailureThreshold, value.FailureAction})
	w.InputSignature = w.BaseSignature + string(rules) + string(policy)
	w.Default = compile(domain.ResolveCheckerSettings(settings, nil))
	tags := make(map[uint64]bool, len(settings.Rules))
	for start := 0; start < len(assignments); {
		proxyID := assignments[start].ProxyID
		clear(tags)
		end := start
		for end < len(assignments) && assignments[end].ProxyID == proxyID {
			assignment := assignments[end]
			if assignment.WorkspaceID == value.ID && configured[assignment.ProxyTagID] {
				tags[assignment.ProxyTagID] = true
			}
			end++
		}
		if len(tags) > 0 {
			plan := compile(domain.ResolveCheckerSettings(settings, tags))
			if plan.Keys != w.Default.Keys {
				w.Proxies.set(proxyID, plan)
			}
		}
		start = end
	}
	return w
}

type Loader func(context.Context, uint) (*Workspace, error)

var state = struct {
	sync.RWMutex
	workspaces map[uint]*Workspace
	loader     Loader
	client     *redis.Client
	ready      bool
}{workspaces: make(map[uint]*Workspace)}

const syncChannel = "magpie:checker_settings:invalidate"

var notificationOrigin = fmt.Sprintf("%d:%d", os.Getpid(), time.Now().UnixNano())

func Lookup(workspaceID uint) (*Workspace, bool) {
	state.RLock()
	defer state.RUnlock()
	return state.workspaces[workspaceID], state.ready
}

func Refresh(ctx context.Context, workspaceID uint) error {
	state.RLock()
	loader := state.loader
	state.RUnlock()
	if loader == nil {
		return nil
	}
	w, err := loader(ctx, workspaceID)
	if err != nil {
		return err
	}
	state.Lock()
	defer state.Unlock()
	if w == nil {
		delete(state.workspaces, workspaceID)
		return nil
	}
	if previous := state.workspaces[workspaceID]; previous == nil || previous.Generation <= w.Generation {
		state.workspaces[workspaceID] = w
	}
	return nil
}

// Notify is called after commit. Failures keep the last complete snapshot and
// are reconciled by the background routine. The Redis event carries no secrets.
func Notify(workspaceID uint) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := Refresh(ctx, workspaceID); err != nil {
		log.Error("refresh checker settings", "workspace_id", workspaceID, "error", err)
	}
	state.RLock()
	client := state.client
	state.RUnlock()
	if client != nil {
		if err := client.Publish(ctx, syncChannel, fmt.Sprintf("%s %d", notificationOrigin, workspaceID)).Err(); err != nil {
			log.Warn("publish checker settings change", "error", err)
		}
	}
}

func Initialize(ctx context.Context, client *redis.Client, loader Loader, list func(context.Context) ([]uint, error)) error {
	state.Lock()
	state.loader = loader
	state.client = client
	state.Unlock()
	var subscription *redis.PubSub
	if client != nil {
		subscription = client.Subscribe(ctx, syncChannel)
		if _, err := subscription.Receive(ctx); err != nil {
			_ = subscription.Close()
			return err
		}
	}
	reconcile := func() error {
		ids, err := list(ctx)
		if err != nil {
			return err
		}
		present := make(map[uint]bool, len(ids))
		for _, id := range ids {
			present[id] = true
			if err := Refresh(ctx, id); err != nil {
				return err
			}
		}
		state.Lock()
		for id := range state.workspaces {
			if !present[id] {
				delete(state.workspaces, id)
			}
		}
		state.Unlock()
		return nil
	}
	if err := reconcile(); err != nil {
		if subscription != nil {
			_ = subscription.Close()
		}
		return err
	}
	state.Lock()
	state.ready = true
	state.Unlock()
	go func() {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		var events <-chan *redis.Message
		if subscription != nil {
			defer subscription.Close()
			events = subscription.Channel()
		}
		for {
			select {
			case <-ctx.Done():
				return
			case message := <-events:
				if message == nil {
					events = nil
					continue
				}
				var id uint
				var origin string
				if _, err := fmt.Sscan(message.Payload, &origin, &id); err == nil && id > 0 && origin != notificationOrigin {
					if err := Refresh(ctx, id); err != nil {
						log.Warn("reload checker settings notification", "error", err)
					}
				}
			case <-ticker.C:
				if err := reconcile(); err != nil {
					log.Warn("reconcile checker settings", "error", err)
				}
			}
		}
	}()
	return nil
}

// Ordinary classification tags do not require a checker refresh.
func TagAssignmentsChanged(workspaceID uint) {
	snapshot, ready := Lookup(workspaceID)
	if !ready {
		return
	}
	if snapshot == nil || len(snapshot.Value.DefaultCheckerSettings().Rules) > 0 {
		Notify(workspaceID)
	}
}
