package archtest

import (
	"go/ast"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// TestEveryPackageHasDocComment requires a package comment in each package.
func TestEveryPackageHasDocComment(t *testing.T) {
	for _, pkg := range repo(t).sortedPackages() {
		documented := false
		for _, file := range pkg.productionFiles() {
			if file.ast.Doc != nil && strings.TrimSpace(file.ast.Doc.Text()) != "" {
				documented = true
			}
		}
		if !documented && len(pkg.productionFiles()) > 0 {
			t.Errorf("%s has no package comment; add a doc.go that says what the package owns", pkg.dir)
		}
	}
}

const performanceIndex = "docs/performance/README.md"

var (
	indexRow      = regexp.MustCompile(`^\|\s*\[[^\]]+\]\(([^)]+)\)\s*\|\s*(\w+)\s*\|`)
	markdownLink  = regexp.MustCompile(`\]\(([^)\s]+)\)`)
	goTestCommand = regexp.MustCompile(`go test[^\n` + "`" + `]*`)
)

// markdownFiles returns the checked documentation files and the historical
// performance notes that are skipped. Every performance note must have a row
// in the index that marks it current or historical.
func markdownFiles(t *testing.T, root string) (checked []string, historical map[string]bool) {
	t.Helper()
	historical = map[string]bool{}
	indexed := map[string]bool{}
	raw, err := os.ReadFile(filepath.Join(root, performanceIndex))
	if err != nil {
		t.Fatalf("read performance index: %v", err)
	}
	for _, line := range strings.Split(string(raw), "\n") {
		match := indexRow.FindStringSubmatch(line)
		if match == nil {
			continue
		}
		note := "docs/performance/" + match[1]
		indexed[note] = true
		switch match[2] {
		case "historical":
			historical[note] = true
		case "current":
		default:
			t.Errorf("%s: %s has status %q; use current or historical", performanceIndex, match[1], match[2])
		}
	}
	candidates := []string{"README.md", "RUNBOOK.md", "CONTEXT.md", "AGENTS.md", "PASSWORD_RECOVERY.md"}
	_ = filepath.WalkDir(filepath.Join(root, "docs"), func(path string, entry os.DirEntry, err error) error {
		if err == nil && !entry.IsDir() && strings.HasSuffix(path, ".md") {
			relative, _ := filepath.Rel(root, path)
			candidates = append(candidates, filepath.ToSlash(relative))
		}
		return nil
	})
	for _, file := range candidates {
		if _, err := os.Stat(filepath.Join(root, file)); err != nil {
			continue
		}
		if strings.HasPrefix(file, "docs/performance/") && file != performanceIndex && !indexed[file] {
			t.Errorf("%s has no row in %s; add one that marks it current or historical", file, performanceIndex)
		}
		if !historical[file] {
			checked = append(checked, file)
		}
	}
	for note := range indexed {
		if _, err := os.Stat(filepath.Join(root, note)); err != nil {
			t.Errorf("%s lists %s, which does not exist", performanceIndex, note)
		}
	}
	sort.Strings(checked)
	return checked, historical
}

// TestDocumentationLinksResolve checks every relative link in the maintained
// documentation. Historical performance notes are skipped.
func TestDocumentationLinksResolve(t *testing.T) {
	root := repo(t).root
	checked, _ := markdownFiles(t, root)
	for _, file := range checked {
		raw, err := os.ReadFile(filepath.Join(root, file))
		if err != nil {
			t.Fatal(err)
		}
		for _, match := range markdownLink.FindAllStringSubmatch(string(raw), -1) {
			target := match[1]
			if strings.Contains(target, "://") || strings.HasPrefix(target, "#") || strings.HasPrefix(target, "mailto:") {
				continue
			}
			target, _, _ = strings.Cut(target, "#")
			resolved := filepath.Join(root, filepath.Dir(file), target)
			if _, err := os.Stat(resolved); err != nil {
				t.Errorf("%s links to %s, which does not exist", file, match[1])
			}
		}
	}
}

// testNames returns the top-level Test and Benchmark functions of a package
// directory, test files included.
func testNames(loaded *repository, dir string) []string {
	pkg := loaded.packages[packageName(dir)]
	if pkg == nil {
		return nil
	}
	var names []string
	for _, file := range pkg.files {
		if !file.isTest {
			continue
		}
		for _, decl := range file.ast.Decls {
			if function, ok := decl.(*ast.FuncDecl); ok && function.Recv == nil {
				names = append(names, function.Name.Name)
			}
		}
	}
	return names
}

// TestDocumentedTestCommandsNameRealTests checks that each go test snippet in
// the maintained documentation names a package that exists and, when it has a
// -run or -bench pattern, that the pattern matches a test in that package.
func TestDocumentedTestCommandsNameRealTests(t *testing.T) {
	loaded := repo(t)
	checked, _ := markdownFiles(t, loaded.root)
	for _, file := range checked {
		raw, err := os.ReadFile(filepath.Join(loaded.root, file))
		if err != nil {
			t.Fatal(err)
		}
		// Join shell and prose line continuations before matching commands.
		text := strings.ReplaceAll(string(raw), "\\\n", " ")
		for _, command := range goTestCommand.FindAllString(text, -1) {
			checkTestCommand(t, loaded, file, command)
		}
	}
}

func checkTestCommand(t *testing.T, loaded *repository, file, command string) {
	t.Helper()
	fields := strings.Fields(command)
	var dirs []string
	patterns := map[string]string{}
	for index := 2; index < len(fields); index++ {
		// Trim trailing punctuation only: a leading "./" names a package, and a
		// trailing "..." is a package pattern rather than a full stop.
		field := strings.TrimRight(fields[index], ",;:")
		if !strings.HasSuffix(field, "...") {
			field = strings.TrimRight(field, ".")
		}
		switch {
		case field == "-run" || field == "-bench":
			if index+1 < len(fields) {
				patterns[field] = strings.Trim(fields[index+1], `'",.;:`)
				index++
			}
		case strings.HasPrefix(field, "-run=") || strings.HasPrefix(field, "-bench="):
			flag, value, _ := strings.Cut(field, "=")
			patterns[flag] = strings.Trim(value, `'"`)
		case strings.HasPrefix(field, "./") && !strings.Contains(field, "..."):
			dirs = append(dirs, strings.TrimPrefix(field, "./"))
		}
	}
	for _, dir := range dirs {
		if loaded.packages[packageName(dir)] == nil {
			t.Errorf("%s: %q names ./%s, which is not a package", file, command, dir)
			continue
		}
		for flag, pattern := range patterns {
			if pattern == "" || pattern == "^$" || pattern == "." {
				continue
			}
			// Only the top-level name is checked; subtest parts follow a slash.
			top, _, _ := strings.Cut(pattern, "/")
			expression, err := regexp.Compile(top)
			if err != nil {
				t.Errorf("%s: %q has an invalid %s pattern: %v", file, command, flag, err)
				continue
			}
			prefix := "Test"
			if flag == "-bench" {
				prefix = "Benchmark"
			}
			matched := false
			for _, name := range testNames(loaded, dir) {
				if strings.HasPrefix(name, prefix) && expression.MatchString(name) {
					matched = true
					break
				}
			}
			if !matched {
				t.Errorf("%s: %q matches no %s function in ./%s. Update the snippet, or mark the note historical in %s.",
					file, command, prefix, dir, performanceIndex)
			}
		}
	}
}
