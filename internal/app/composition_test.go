package app

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"magpie/internal/checkpace"
	"magpie/internal/domain"
	proxyqueue "magpie/internal/jobs/queue/proxy"
	sitequeue "magpie/internal/jobs/queue/sites"
)

// The process has one Pacer, constructed here. Both installed queues schedule
// with it: an added route waits for its owner's cadence point from that
// Pacer's snapshot, and RequeueAll spreads sources over its fastest scrape
// cadence.
// bootstrap.Setup and server.OpenRoutes receive the same value from Run.
func TestInstallPacedQueuesGivesBothQueuesTheProcessPacer(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	t.Setenv("SCRAPE_QUEUE_SHARDS", "1")
	previousProxies, previousSites := proxyqueue.PublicProxyQueue, sitequeue.PublicScrapeSiteQueue
	t.Cleanup(func() { proxyqueue.PublicProxyQueue, sitequeue.PublicScrapeSiteQueue = previousProxies, previousSites })

	pace := installPacedQueues(client)
	if pace == nil || pace.Current() != nil {
		t.Fatal("installPacedQueues must construct a Pacer without a snapshot")
	}
	const checkCadence, scrapeCadence = 65 * time.Minute, 4 * time.Hour
	pace.Publish(checkpace.Uniform(checkCadence, scrapeCadence))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	proxies := &proxyqueue.PublicProxyQueue
	route := domain.Proxy{ID: 42, IP: "192.0.2.42", Port: 40005, Hash: []byte("composition-route"), Workspaces: []domain.Workspace{{ID: 1}}}
	// The route is first due at its owner's cadence point on that Pacer, which
	// no other cadence shares.
	added := time.Now()
	if err := proxies.AddToQueue([]domain.Proxy{route}); err != nil {
		t.Fatal(err)
	}
	want := pace.FirstRouteCheck(pace.Current(), route.ID, route.Workspaces, added)
	if wait := want.Sub(added); wait <= 0 || wait > checkCadence {
		t.Fatalf("the Pacer's first check is %s away at a cadence of %s", wait, checkCadence)
	}
	var score float64
	for _, key := range server.Keys() {
		if server.Type(key) != "zset" || strings.Contains(key, "head") || strings.Contains(key, "scrape") {
			continue
		}
		if found, err := client.ZScore(ctx, key, string(route.Hash)).Result(); err == nil {
			score = found
		}
	}
	// The queue read the clock after this test did; the point may have passed
	// in between, and then the route waits for the next one.
	later := pace.FirstRouteCheck(pace.Current(), route.ID, route.Workspaces, time.Now())
	if score != float64(want.UnixMilli()) && score != float64(later.UnixMilli()) {
		t.Fatalf("the proxy queue did not schedule the route at the Pacer's cadence point: score=%f, want %d", score, want.UnixMilli())
	}

	sites := &sitequeue.PublicScrapeSiteQueue
	if err := sites.AddToQueue([]domain.ScrapeSite{{URL: "https://example.com/a.txt"}, {URL: "https://example.com/b.txt"}}); err != nil {
		t.Fatal(err)
	}
	before := time.Now()
	if count, err := sites.RequeueAll(); err != nil || count != 2 {
		t.Fatalf("RequeueAll = %d, %v", count, err)
	}
	after := time.Now()
	var latest float64
	for _, key := range server.Keys() {
		if !strings.Contains(key, "scrape") || server.Type(key) != "zset" || strings.Contains(key, "head") {
			continue
		}
		members, err := client.ZRangeWithScores(ctx, key, 0, -1).Result()
		if err != nil {
			t.Fatal(err)
		}
		for _, member := range members {
			latest = max(latest, member.Score)
		}
	}
	// Two sources in one shard: the second waits half of the window, and the
	// window is the Pacer's fastest scrape cadence, not the fallback.
	low, high := before.Add(scrapeCadence/2).UnixMilli(), after.Add(scrapeCadence/2).UnixMilli()
	if latest < float64(low) || latest > float64(high) {
		t.Fatalf("RequeueAll spread sources to %f, want [%d, %d] from the Pacer's %s scrape cadence", latest, low, high, scrapeCadence)
	}
}

// Only app constructs the process's Pacer. Everything else receives it, so no
// package can keep a Pacer of its own to fall back on.
func TestOnlyAppConstructsAPacer(t *testing.T) {
	root := filepath.Join("..", "..")
	var sites []string
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if name := entry.Name(); name == ".git" || name == "vendor" || name == "node_modules" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return err
		}
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			if selector, ok := call.Fun.(*ast.SelectorExpr); ok && selector.Sel.Name == "NewPacer" {
				if pkg, ok := selector.X.(*ast.Ident); ok && pkg.Name == "checkpace" {
					rel, _ := filepath.Rel(root, path)
					sites = append(sites, filepath.ToSlash(rel))
				}
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(sites) != 1 || sites[0] != "internal/app/app.go" {
		t.Fatalf("checkpace.NewPacer is called from %v; only internal/app/app.go may construct the process's Pacer", sites)
	}
}
