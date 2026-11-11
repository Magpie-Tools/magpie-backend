package archtest

import (
	"go/ast"
	"go/token"
	"strings"
	"testing"
)

// exemptConstructors are the initializers that do not count as package state:
// sentinel errors, compiled regular expressions, Redis scripts and Prometheus
// collectors. //go:embed variables and blank identifiers are exempt too.
var exemptConstructors = map[string]map[string]bool{
	"errors":                       {"New": true},
	"fmt":                          {"Errorf": true},
	"regexp":                       {"MustCompile": true, "MustCompilePOSIX": true},
	"github.com/redis/go-redis/v9": {"NewScript": true},
}

const (
	prometheusPath = "github.com/prometheus/client_golang/prometheus"
	promautoPath   = "github.com/prometheus/client_golang/prometheus/promauto"
)

func exemptInitializer(value ast.Expr, names map[string]string) bool {
	call, ok := value.(*ast.CallExpr)
	if !ok {
		return false
	}
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	qualifier, ok := selector.X.(*ast.Ident)
	if !ok {
		return false
	}
	path := names[qualifier.Name]
	if exemptConstructors[path][selector.Sel.Name] {
		return true
	}
	return (path == prometheusPath || path == promautoPath) && strings.HasPrefix(selector.Sel.Name, "New")
}

func embedded(decl *ast.GenDecl, spec *ast.ValueSpec) bool {
	for _, group := range []*ast.CommentGroup{decl.Doc, spec.Doc} {
		if group == nil {
			continue
		}
		for _, comment := range group.List {
			if strings.HasPrefix(comment.Text, "//go:embed") {
				return true
			}
		}
	}
	return false
}

// packageVariables counts the package-level variables that hold state.
func packageVariables(pkg *sourcePackage) int {
	count := 0
	for _, file := range pkg.productionFiles() {
		names := importNames(file.ast)
		for _, decl := range file.ast.Decls {
			general, ok := decl.(*ast.GenDecl)
			if !ok || general.Tok != token.VAR {
				continue
			}
			for _, spec := range general.Specs {
				value := spec.(*ast.ValueSpec)
				if embedded(general, value) {
					continue
				}
				for index, name := range value.Names {
					if name.Name == "_" {
						continue
					}
					if index < len(value.Values) && exemptInitializer(value.Values[index], names) {
						continue
					}
					count++
				}
			}
		}
	}
	return count
}

// TestPackageLevelState holds each package to its recorded count of
// package-level variables. The count cannot rise: new state belongs in a value
// constructed in app. Lower the number when a variable is removed.
func TestPackageLevelState(t *testing.T) {
	loaded := repo(t)
	recorded := map[string]int{}
	for _, line := range readRules(t, "state.txt") {
		if line.key == "init_goroutine" {
			continue
		}
		recorded[line.key] = atoi(t, "state.txt", line)
	}
	for _, pkg := range loaded.sortedPackages() {
		actual, limit := packageVariables(pkg), recorded[pkg.name]
		switch {
		case actual > limit:
			t.Errorf("%s has %d package-level variables, above its recorded %d. New package-level state is not allowed "+
				"(exceptions: errors.New/fmt.Errorf sentinels, regexp.MustCompile, //go:embed, redis.NewScript, Prometheus collectors). "+
				"A move of existing state raises this package's number and lowers the source package's in rules/state.txt.", pkg.name, actual, limit)
		case actual < limit:
			t.Errorf("%s has %d package-level variables; lower its number in rules/state.txt from %d", pkg.name, actual, limit)
		}
	}
	for name := range recorded {
		if loaded.packages[name] == nil {
			t.Errorf("rules/state.txt lists %s, which is not a package", name)
		}
	}
}

// goroutineStarters returns the package's functions that contain a go statement.
func goroutineStarters(pkg *sourcePackage) map[string]bool {
	starters := map[string]bool{}
	for _, file := range pkg.productionFiles() {
		for _, decl := range file.ast.Decls {
			function, ok := decl.(*ast.FuncDecl)
			if !ok || function.Body == nil || function.Recv != nil {
				continue
			}
			ast.Inspect(function.Body, func(node ast.Node) bool {
				if _, ok := node.(*ast.GoStmt); ok {
					starters[function.Name.Name] = true
				}
				return true
			})
		}
	}
	return starters
}

// TestInitStartsNoGoroutine forbids init() functions that start a goroutine,
// directly or through a function of the same package. Routines start from
// app/bootstrap so their stop order is explicit.
func TestInitStartsNoGoroutine(t *testing.T) {
	loaded := repo(t)
	allowed := map[string]int{}
	for _, line := range readRules(t, "state.txt") {
		if line.key != "init_goroutine" {
			continue
		}
		for _, file := range line.values {
			allowed[file] = line.number
		}
	}
	seen := map[string]bool{}
	for _, pkg := range loaded.sortedPackages() {
		starters := goroutineStarters(pkg)
		for _, file := range pkg.productionFiles() {
			for _, decl := range file.ast.Decls {
				function, ok := decl.(*ast.FuncDecl)
				if !ok || function.Recv != nil || function.Name.Name != "init" || function.Body == nil {
					continue
				}
				starts := false
				ast.Inspect(function.Body, func(node ast.Node) bool {
					switch node := node.(type) {
					case *ast.GoStmt:
						starts = true
					case *ast.CallExpr:
						if callee, ok := node.Fun.(*ast.Ident); ok && callee.Name != "init" && starters[callee.Name] {
							starts = true
						}
					}
					return true
				})
				if !starts {
					continue
				}
				if _, ok := allowed[file.path]; ok {
					seen[file.path] = true
					continue
				}
				t.Errorf("%s: init() starts a goroutine. Start routines from app/bootstrap instead.", file.path)
			}
		}
	}
	for file, line := range allowed {
		if !seen[file] {
			t.Errorf("state.txt:%d: init() in %s no longer starts a goroutine; remove it from the list", line, file)
		}
	}
}

// TestTestKitStaysInTests keeps the shared test kit out of the binary.
func TestTestKitStaysInTests(t *testing.T) {
	for _, pkg := range repo(t).sortedPackages() {
		if strings.HasPrefix(pkg.name, "testkit") {
			continue
		}
		for imported := range pkg.internalImports() {
			if strings.HasPrefix(imported, "testkit") {
				t.Errorf("%s imports %s from a non-test file; only _test.go files may import the test kit", pkg.name, imported)
			}
		}
	}
}
