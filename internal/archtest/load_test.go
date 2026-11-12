package archtest

import (
	"bufio"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
)

const modulePath = "magpie"

// sourceFile is one parsed Go file.
type sourceFile struct {
	path   string // relative to the repository root
	isTest bool
	lines  int
	ast    *ast.File
}

// sourcePackage is one directory of Go files. Its name is the import path
// without the module prefix and, for internal packages, without "internal/".
type sourcePackage struct {
	name  string
	dir   string // relative to the repository root
	files []*sourceFile
}

type repository struct {
	root     string
	fset     *token.FileSet
	packages map[string]*sourcePackage
}

var (
	loadOnce   sync.Once
	loadedRepo *repository
	loadErr    error
)

// repo parses every package once; all rules share the result.
func repo(t *testing.T) *repository {
	t.Helper()
	loadOnce.Do(func() { loadedRepo, loadErr = loadRepository() })
	if loadErr != nil {
		t.Fatalf("load repository: %v", loadErr)
	}
	return loadedRepo
}

func loadRepository() (*repository, error) {
	root, err := repositoryRoot()
	if err != nil {
		return nil, err
	}
	loaded := &repository{root: root, fset: token.NewFileSet(), packages: map[string]*sourcePackage{}}
	for _, top := range []string{"cmd", "internal"} {
		err := filepath.WalkDir(filepath.Join(root, top), func(path string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() {
				if entry.Name() == "testdata" {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(path, ".go") {
				return nil
			}
			return loaded.addFile(path)
		})
		if err != nil {
			return nil, err
		}
	}
	return loaded, nil
}

func repositoryRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", os.ErrNotExist
		}
		dir = parent
	}
}

func (r *repository) addFile(path string) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	parsed, err := parser.ParseFile(r.fset, path, raw, parser.ParseComments|parser.SkipObjectResolution)
	if err != nil {
		return err
	}
	relative, err := filepath.Rel(r.root, path)
	if err != nil {
		return err
	}
	relative = filepath.ToSlash(relative)
	dir := filepath.ToSlash(filepath.Dir(relative))
	name := packageName(dir)
	pkg := r.packages[name]
	if pkg == nil {
		pkg = &sourcePackage{name: name, dir: dir}
		r.packages[name] = pkg
	}
	lines := strings.Count(string(raw), "\n")
	if len(raw) > 0 && raw[len(raw)-1] != '\n' {
		lines++
	}
	pkg.files = append(pkg.files, &sourceFile{
		path: relative, isTest: strings.HasSuffix(relative, "_test.go"), lines: lines, ast: parsed,
	})
	return nil
}

// packageName shortens a repository directory to the name used in rule files.
func packageName(dir string) string {
	return strings.TrimPrefix(dir, "internal/")
}

// internalImport returns the rule-file name of an import inside this module.
func internalImport(importPath string) (string, bool) {
	if !strings.HasPrefix(importPath, modulePath+"/") {
		return "", false
	}
	return packageName(strings.TrimPrefix(importPath, modulePath+"/")), true
}

func (r *repository) sortedPackages() []*sourcePackage {
	names := make([]string, 0, len(r.packages))
	for name := range r.packages {
		names = append(names, name)
	}
	sort.Strings(names)
	packages := make([]*sourcePackage, 0, len(names))
	for _, name := range names {
		packages = append(packages, r.packages[name])
	}
	return packages
}

// productionFiles are the files compiled into the binary.
func (p *sourcePackage) productionFiles() []*sourceFile {
	files := make([]*sourceFile, 0, len(p.files))
	for _, file := range p.files {
		if !file.isTest {
			files = append(files, file)
		}
	}
	return files
}

func (p *sourcePackage) productionLines() int {
	lines := 0
	for _, file := range p.productionFiles() {
		lines += file.lines
	}
	return lines
}

// imports returns the import paths of the package's production files.
func (p *sourcePackage) imports() map[string]bool {
	imports := map[string]bool{}
	for _, file := range p.productionFiles() {
		for _, spec := range file.ast.Imports {
			path, err := strconv.Unquote(spec.Path.Value)
			if err == nil {
				imports[path] = true
			}
		}
	}
	return imports
}

func (p *sourcePackage) internalImports() map[string]bool {
	internal := map[string]bool{}
	for path := range p.imports() {
		if name, ok := internalImport(path); ok {
			internal[name] = true
		}
	}
	return internal
}

// importNames maps the local name of each import in a file to its path.
func importNames(file *ast.File) map[string]string {
	names := map[string]string{}
	for _, spec := range file.Imports {
		path, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			continue
		}
		local := path[strings.LastIndex(path, "/")+1:]
		if strings.HasPrefix(local, "v") && len(local) > 1 && strings.Trim(local[1:], "0123456789") == "" {
			// A major-version suffix such as go-redis/v9 names the element before it.
			trimmed := strings.TrimSuffix(path, "/"+local)
			local = trimmed[strings.LastIndex(trimmed, "/")+1:]
			local = strings.TrimPrefix(local, "go-")
		}
		if spec.Name != nil {
			local = spec.Name.Name
		}
		names[local] = path
	}
	return names
}

// ruleLine is one data line of a rule file.
type ruleLine struct {
	number int
	key    string
	values []string
}

// readRules parses "key: value value" lines. '#' starts a comment.
func readRules(t *testing.T, name string) []ruleLine {
	t.Helper()
	path := filepath.Join(repo(t).root, "internal", "archtest", "rules", name)
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open rule file: %v", err)
	}
	defer file.Close()
	var lines []ruleLine
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 1<<16), 1<<20)
	for number := 1; scanner.Scan(); number++ {
		text := scanner.Text()
		if comment := strings.Index(text, "#"); comment >= 0 {
			text = text[:comment]
		}
		text = strings.TrimSpace(text)
		if text == "" {
			continue
		}
		key, rest, found := strings.Cut(text, ":")
		if !found {
			t.Fatalf("%s:%d: want \"key: values\", got %q", name, number, text)
		}
		lines = append(lines, ruleLine{number: number, key: strings.TrimSpace(key), values: strings.Fields(rest)})
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	return lines
}

func sortedKeys(set map[string]bool) []string {
	keys := make([]string, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// difference returns the members of left that are missing from right.
func difference(left, right map[string]bool) []string {
	var missing []string
	for key := range left {
		if !right[key] {
			missing = append(missing, key)
		}
	}
	sort.Strings(missing)
	return missing
}

func toSet(values []string) map[string]bool {
	set := make(map[string]bool, len(values))
	for _, value := range values {
		set[value] = true
	}
	return set
}
