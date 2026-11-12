package archtest

import (
	"go/ast"
	"strings"
	"testing"
)

// connectionConstructors are the calls that open a database or Redis
// connection or pool, by import path.
var connectionConstructors = map[string]map[string]bool{
	"gorm.io/gorm":                    {"Open": true},
	"database/sql":                    {"Open": true, "OpenDB": true},
	"github.com/jackc/pgx/v5":         {"Connect": true, "ConnectConfig": true},
	"github.com/jackc/pgx/v5/pgconn":  {"Connect": true, "ConnectConfig": true},
	"github.com/jackc/pgx/v5/pgxpool": {"New": true, "NewWithConfig": true},
	"github.com/jackc/pgx/v5/stdlib":  {"OpenDB": true, "OpenDBFromPool": true},
	"github.com/redis/go-redis/v9":    {"NewClient": true, "NewFailoverClient": true, "NewClusterClient": true, "NewUniversalClient": true, "NewRing": true, "NewFailoverClusterClient": true, "NewSentinelClient": true},
}

// TestConnectionCreation allows production connections to be created only
// under internal/platform. rules/connections.txt lists the existing sites
// elsewhere; that list only shrinks.
func TestConnectionCreation(t *testing.T) {
	loaded := repo(t)
	allowed := map[string]int{}
	for _, line := range readRules(t, "connections.txt") {
		if len(line.values) != 1 {
			t.Fatalf("connections.txt:%d: want \"path/file.go: Function\"", line.number)
		}
		allowed[line.key+": "+line.values[0]] = line.number
	}
	seen := map[string]bool{}
	for _, pkg := range loaded.sortedPackages() {
		// The test kit opens test databases. Only _test.go files may import
		// it (TestTestKitStaysInTests), so it creates no production connection.
		if strings.HasPrefix(pkg.name, "platform/") || strings.HasPrefix(pkg.name, "testkit/") {
			continue
		}
		for _, file := range pkg.productionFiles() {
			names := importNames(file.ast)
			for _, decl := range file.ast.Decls {
				function, ok := decl.(*ast.FuncDecl)
				if !ok {
					continue
				}
				ast.Inspect(function, func(node ast.Node) bool {
					call, ok := node.(*ast.CallExpr)
					if !ok {
						return true
					}
					selector, ok := call.Fun.(*ast.SelectorExpr)
					if !ok {
						return true
					}
					qualifier, ok := selector.X.(*ast.Ident)
					if !ok || !connectionConstructors[names[qualifier.Name]][selector.Sel.Name] {
						return true
					}
					site := file.path + ": " + functionName(function)
					if _, ok := allowed[site]; ok {
						seen[site] = true
						return true
					}
					t.Errorf("%s calls %s.%s. Connections are created only under internal/platform; "+
						"rules/connections.txt lists the remaining sites and only shrinks.", site, qualifier.Name, selector.Sel.Name)
					return true
				})
			}
		}
	}
	for site, line := range allowed {
		if !seen[site] {
			t.Errorf("connections.txt:%d: %s no longer creates a connection; remove the line", line, site)
		}
	}
}
