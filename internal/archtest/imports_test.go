package archtest

import (
	"sort"
	"strings"
	"testing"
)

const hotPathChecklist = "This is a hot-path change. Follow the \"Proxy checker hot path\" checklist in the workspace AGENTS.md: " +
	"count the cryptographic operations, JSON encodes, Redis commands and database queries added per proxy check, " +
	"keep the hash-reuse and no-payload-rewrite tests, update docs/performance/hot-path-operation-baseline.md, " +
	"and only then update internal/archtest/rules/hotpath_pins.txt."

// TestImportTable holds every package to the internal imports listed for it in
// rules/imports.txt. A new dependency is a visible change to that table.
func TestImportTable(t *testing.T) {
	loaded := repo(t)
	table := map[string]map[string]bool{}
	for _, line := range readRules(t, "imports.txt") {
		if _, duplicate := table[line.key]; duplicate {
			t.Errorf("imports.txt:%d: %s is listed twice", line.number, line.key)
		}
		table[line.key] = toSet(line.values)
	}
	for _, pkg := range loaded.sortedPackages() {
		allowed, listed := table[pkg.name]
		if !listed {
			t.Errorf("%s has no row in rules/imports.txt; add one listing its internal imports", pkg.name)
			continue
		}
		actual := pkg.internalImports()
		if added := difference(actual, allowed); len(added) > 0 {
			t.Errorf("%s imports %s, which rules/imports.txt does not allow for it. "+
				"Check rules/layers.txt before adding the dependency to the table.", pkg.name, strings.Join(added, ", "))
		}
		if stale := difference(allowed, actual); len(stale) > 0 {
			t.Errorf("%s no longer imports %s; remove it from the package's row in rules/imports.txt", pkg.name, strings.Join(stale, ", "))
		}
	}
	for name := range table {
		if loaded.packages[name] == nil {
			t.Errorf("rules/imports.txt lists %s, which is not a package", name)
		}
	}
}

type layerRules struct {
	groups  map[string][]string
	forbid  [][2]string
	only    map[string]map[string]bool
	allowed map[[2]string]int // existing violation -> line
}

func readLayerRules(t *testing.T) layerRules {
	t.Helper()
	rules := layerRules{groups: map[string][]string{}, only: map[string]map[string]bool{}, allowed: map[[2]string]int{}}
	for _, line := range readRules(t, "layers.txt") {
		kind, subject, _ := strings.Cut(line.key, " ")
		subject = strings.TrimSpace(subject)
		switch kind {
		case "group":
			rules.groups[subject] = line.values
		case "forbid":
			for _, target := range line.values {
				rules.forbid = append(rules.forbid, [2]string{subject, target})
			}
		case "only":
			rules.only[subject] = toSet(line.values)
		case "violation":
			for _, target := range line.values {
				rules.allowed[[2]string{subject, target}] = line.number
			}
		default:
			t.Fatalf("layers.txt:%d: unknown rule %q", line.number, line.key)
		}
	}
	return rules
}

// members expands a group name, or returns a package or import prefix as is.
func (rules layerRules) members(name string) []string {
	if members, ok := rules.groups[name]; ok {
		return members
	}
	return []string{name}
}

// matchesTarget reports whether an import path falls under a rule target. An
// internal target names one package; an external target is a path prefix.
func matchesTarget(importPath, target string) bool {
	if internal, ok := internalImport(importPath); ok {
		return internal == target
	}
	return strings.Contains(target, ".") && (importPath == target || strings.HasPrefix(importPath, target+"/"))
}

// TestDependencyDirections enforces rules/layers.txt: the directions no package
// may depend in, with the existing violations that are still to be removed.
func TestDependencyDirections(t *testing.T) {
	loaded := repo(t)
	rules := readLayerRules(t)
	seen := map[[2]string]bool{}
	for _, rule := range rules.forbid {
		for _, from := range rules.members(rule[0]) {
			pkg := loaded.packages[from]
			if pkg == nil {
				continue
			}
			for importPath := range pkg.imports() {
				for _, target := range rules.members(rule[1]) {
					if !matchesTarget(importPath, target) {
						continue
					}
					edge := [2]string{from, target}
					if _, allowed := rules.allowed[edge]; allowed {
						seen[edge] = true
						continue
					}
					t.Errorf("%s imports %s: rules/layers.txt forbids %s -> %s", from, importPath, rule[0], rule[1])
				}
			}
		}
	}
	for edge, line := range rules.allowed {
		if !seen[edge] {
			t.Errorf("layers.txt:%d: violation %s -> %s no longer exists; remove the line", line, edge[0], edge[1])
		}
	}
	for name, allowed := range rules.only {
		pkg := loaded.packages[name]
		if pkg == nil {
			t.Errorf("rules/layers.txt restricts %s, which is not a package", name)
			continue
		}
		if extra := difference(pkg.internalImports(), allowed); len(extra) > 0 {
			t.Errorf("%s may import only %s from this module, but imports %s",
				name, strings.Join(sortedKeys(allowed), ", "), strings.Join(extra, ", "))
		}
	}
}

// TestHotPathImportPins fails when a hot-path package gains or loses any
// import, standard library and third-party included.
func TestHotPathImportPins(t *testing.T) {
	loaded := repo(t)
	lines := readRules(t, "hotpath_pins.txt")
	if len(lines) == 0 {
		t.Fatal("rules/hotpath_pins.txt pins no package")
	}
	for _, line := range lines {
		pkg := loaded.packages[line.key]
		if pkg == nil {
			t.Errorf("hotpath_pins.txt:%d: %s is not a package", line.number, line.key)
			continue
		}
		pinned, actual := toSet(line.values), pkg.imports()
		added, removed := difference(actual, pinned), difference(pinned, actual)
		if len(added) == 0 && len(removed) == 0 {
			continue
		}
		t.Errorf("%s changed its imports (added: %s; removed: %s). %s",
			pkg.name, orNone(added), orNone(removed), hotPathChecklist)
	}
}

func orNone(values []string) string {
	if len(values) == 0 {
		return "none"
	}
	return strings.Join(values, ", ")
}

// TestCoreImportersAndSize keeps internal/database from growing unnoticed. The
// importer list only shrinks, except for a package created by moving code out
// of the core. The line ceiling is raised in the diff that adds core code.
func TestCoreImportersAndSize(t *testing.T) {
	loaded := repo(t)
	const core = "database"
	const slack = 100
	var ceiling int
	var listed map[string]bool
	for _, line := range readRules(t, "core.txt") {
		switch line.key {
		case "line_ceiling":
			ceiling = atoi(t, "core.txt", line)
		case "importers":
			listed = toSet(line.values)
		default:
			t.Fatalf("core.txt:%d: unknown key %q", line.number, line.key)
		}
	}
	actual := map[string]bool{}
	for _, pkg := range loaded.sortedPackages() {
		if pkg.internalImports()[core] {
			actual[pkg.name] = true
		}
	}
	if added := difference(actual, listed); len(added) > 0 {
		t.Errorf("%s now import internal/database. Its importer list only shrinks; "+
			"the exception is a package created by moving code out of the core (rules/core.txt).", strings.Join(added, ", "))
	}
	if stale := difference(listed, actual); len(stale) > 0 {
		t.Errorf("%s no longer import internal/database; remove them from importers in rules/core.txt", strings.Join(stale, ", "))
	}

	lines := loaded.packages[core].productionLines()
	if lines > ceiling {
		t.Errorf("internal/database has %d non-test lines, above its ceiling of %d. "+
			"Raise line_ceiling in rules/core.txt in the same diff so the growth is reviewed.", lines, ceiling)
	}
	if ceiling-lines > slack {
		t.Errorf("internal/database has %d non-test lines; lower line_ceiling in rules/core.txt from %d to %d", lines, ceiling, lines)
	}
}

// TestPackageSizeSignals reports, without failing, every other package above
// 6,000 non-test lines and each package's export count. A package that gains
// about 20 exports in one change is due a boundary review.
func TestPackageSizeSignals(t *testing.T) {
	loaded := repo(t)
	const reviewLines = 6000
	var report []string
	for _, pkg := range loaded.sortedPackages() {
		lines := pkg.productionLines()
		flag := ""
		if lines > reviewLines && pkg.name != "database" {
			flag = "  <- above 6,000 lines: review signal"
		}
		report = append(report, formatSignal(pkg.name, lines, exportCount(pkg), flag))
	}
	sort.Strings(report)
	t.Log("package: non-test lines, exported identifiers\n" + strings.Join(report, "\n"))
}
