// Command onboarding measures what a newcomer must read for a fixed set of
// tasks. It applies the written procedure in docs/onboarding/README.md to the
// task list in docs/onboarding/tasks.txt and prints, per task, the files that
// must be read with their approximate token counts (bytes/4).
//
// The measurement is mechanical and repeatable: it parses the source, never
// runs it, and depends on nothing outside the standard library.
//
//	go run ./internal/tools/onboarding            # measure this tree
//	go run ./internal/tools/onboarding -root DIR  # measure another checkout
//	go run ./internal/tools/onboarding -markdown  # table for the checkpoint log
package main

import (
	"bufio"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const modulePath = "magpie"

type declaration struct {
	pkg      *pkgInfo
	file     string // repository-relative
	name     string // Name, or Type#Method
	node     ast.Node
	start    int
	end      int
	imports  map[string]string // local import name -> path, of the declaring file
	testFunc bool
}

type pkgInfo struct {
	dir   string
	name  string // package clause
	decls map[string][]*declaration
}

type repository struct {
	root     string
	fset     *token.FileSet
	packages map[string]*pkgInfo // by directory
	tests    map[string][]*declaration
	sizes    map[string]int
}

type task struct {
	name    string
	anchors []string
	tests   []string
}

type read struct {
	file   string
	bytes  int
	reason string
}

type measurement struct {
	task          task
	full          []read
	partial       []read
	listings      []read
	searchFiles   int
	searchBytes   int
	unresolved    []string
	anchorPkgDirs []string
}

func main() {
	root := flag.String("root", ".", "repository root to measure")
	tasksPath := flag.String("tasks", "", "task list (default: docs/onboarding/tasks.txt under the current directory)")
	markdown := flag.Bool("markdown", false, "print only the summary table as Markdown")
	flag.Parse()
	if *tasksPath == "" {
		*tasksPath = filepath.Join("docs", "onboarding", "tasks.txt")
	}
	tasks, err := readTasks(*tasksPath)
	if err != nil {
		fail(err)
	}
	repo, err := load(*root)
	if err != nil {
		fail(err)
	}
	failed := false
	var results []measurement
	for _, item := range tasks {
		result := repo.measure(item)
		if len(result.unresolved) > 0 {
			failed = true
		}
		results = append(results, result)
	}
	if *markdown {
		printMarkdown(results)
	} else {
		printText(results)
	}
	if failed {
		fail(fmt.Errorf("some anchors or tests did not resolve to exactly one declaration"))
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "onboarding:", err)
	os.Exit(1)
}

func readTasks(path string) ([]task, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var tasks []task
	scanner := bufio.NewScanner(file)
	for number := 1; scanner.Scan(); number++ {
		text := scanner.Text()
		if comment := strings.Index(text, "#"); comment >= 0 {
			text = text[:comment]
		}
		text = strings.TrimSpace(text)
		if text == "" {
			continue
		}
		key, value, found := strings.Cut(text, ":")
		value = strings.TrimSpace(value)
		if !found || value == "" {
			return nil, fmt.Errorf("%s:%d: want \"task|anchor|test: value\"", path, number)
		}
		switch key {
		case "task":
			tasks = append(tasks, task{name: value})
		case "anchor", "test":
			if len(tasks) == 0 {
				return nil, fmt.Errorf("%s:%d: %s before the first task", path, number, key)
			}
			current := &tasks[len(tasks)-1]
			if key == "anchor" {
				current.anchors = append(current.anchors, value)
			} else {
				current.tests = append(current.tests, value)
			}
		default:
			return nil, fmt.Errorf("%s:%d: unknown key %q", path, number, key)
		}
	}
	return tasks, scanner.Err()
}

func load(root string) (*repository, error) {
	repo := &repository{
		root: root, fset: token.NewFileSet(), packages: map[string]*pkgInfo{},
		tests: map[string][]*declaration{}, sizes: map[string]int{},
	}
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
			relative, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			relative = filepath.ToSlash(relative)
			info, err := entry.Info()
			if err != nil {
				return err
			}
			repo.sizes[relative] = int(info.Size())
			if strings.HasSuffix(path, ".go") {
				return repo.parse(path, relative)
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return repo, nil
}

func (r *repository) parse(path, relative string) error {
	parsed, err := parser.ParseFile(r.fset, path, nil, parser.ParseComments|parser.SkipObjectResolution)
	if err != nil {
		return err
	}
	dir := filepath.ToSlash(filepath.Dir(relative))
	isTest := strings.HasSuffix(relative, "_test.go")
	pkg := r.packages[dir]
	if pkg == nil {
		pkg = &pkgInfo{dir: dir, decls: map[string][]*declaration{}}
		r.packages[dir] = pkg
	}
	if !isTest {
		pkg.name = parsed.Name.Name
	}
	imports := map[string]string{}
	for _, spec := range parsed.Imports {
		importPath, err := strconv.Unquote(spec.Path.Value)
		if err != nil || !strings.HasPrefix(importPath, modulePath+"/") {
			continue
		}
		local := importPath[strings.LastIndex(importPath, "/")+1:]
		if spec.Name != nil {
			local = spec.Name.Name
		}
		imports[local] = strings.TrimPrefix(importPath, modulePath+"/")
	}
	add := func(name string, node ast.Node, doc *ast.CommentGroup) {
		start := node.Pos()
		if doc != nil {
			start = doc.Pos()
		}
		item := &declaration{
			pkg: pkg, file: relative, name: name, node: node, imports: imports,
			start: r.fset.Position(start).Offset, end: r.fset.Position(node.End()).Offset,
		}
		if isTest {
			if _, isFunc := node.(*ast.FuncDecl); isFunc && !strings.Contains(name, "#") {
				item.testFunc = true
				r.tests[name] = append(r.tests[name], item)
			}
			return
		}
		pkg.decls[name] = append(pkg.decls[name], item)
	}
	for _, decl := range parsed.Decls {
		switch decl := decl.(type) {
		case *ast.FuncDecl:
			add(funcName(decl), decl, decl.Doc)
		case *ast.GenDecl:
			if decl.Tok == token.IMPORT {
				continue
			}
			for _, spec := range decl.Specs {
				var node ast.Node = spec
				doc := decl.Doc
				if len(decl.Specs) == 1 {
					node = decl
				}
				switch spec := spec.(type) {
				case *ast.TypeSpec:
					if len(decl.Specs) > 1 && spec.Doc != nil {
						doc = spec.Doc
					}
					add(spec.Name.Name, node, doc)
				case *ast.ValueSpec:
					if len(decl.Specs) > 1 {
						doc = spec.Doc
					}
					for _, name := range spec.Names {
						add(name.Name, node, doc)
					}
				}
			}
		}
	}
	return nil
}

func funcName(decl *ast.FuncDecl) string {
	if decl.Recv == nil || len(decl.Recv.List) == 0 {
		return decl.Name.Name
	}
	receiver := decl.Recv.List[0].Type
	if star, ok := receiver.(*ast.StarExpr); ok {
		receiver = star.X
	}
	if index, ok := receiver.(*ast.IndexExpr); ok {
		receiver = index.X
	}
	if ident, ok := receiver.(*ast.Ident); ok {
		return ident.Name + "#" + decl.Name.Name
	}
	return decl.Name.Name
}

// resolveAnchor finds the one declaration an anchor names. "pkg.Name" limits
// the search to packages with that package clause; "Name" searches them all;
// "file:dir/base.ext" names a non-Go file by the end of its path.
func (r *repository) resolveAnchor(anchor string) ([]*declaration, string) {
	wantPkg, name := "", anchor
	if before, after, found := strings.Cut(anchor, "."); found {
		wantPkg, name = before, after
	}
	var matches []*declaration
	for _, pkg := range r.packages {
		if wantPkg != "" && pkg.name != wantPkg {
			continue
		}
		matches = append(matches, pkg.decls[name]...)
	}
	return matches, name
}

func (r *repository) measure(item task) measurement {
	result := measurement{task: item}
	full := map[string]string{}
	partial := map[string]int{}
	counted := map[*declaration]bool{}
	listingDirs := map[string]bool{}
	anchorPkgs := map[string]*pkgInfo{}
	var anchors []*declaration

	for _, anchor := range item.anchors {
		if base, isFile := strings.CutPrefix(anchor, "file:"); isFile {
			var found []string
			for path := range r.sizes {
				if path == base || strings.HasSuffix(path, "/"+base) {
					found = append(found, path)
				}
			}
			if len(found) != 1 {
				result.unresolved = append(result.unresolved, fmt.Sprintf("%s (%d files)", anchor, len(found)))
				continue
			}
			full[found[0]] = "anchor"
			listingDirs[filepath.ToSlash(filepath.Dir(found[0]))] = true
			continue
		}
		matches, _ := r.resolveAnchor(anchor)
		if len(matches) != 1 {
			result.unresolved = append(result.unresolved, fmt.Sprintf("%s (%d declarations)", anchor, len(matches)))
			continue
		}
		anchors = append(anchors, matches[0])
		full[matches[0].file] = "anchor"
		listingDirs[matches[0].pkg.dir] = true
		anchorPkgs[matches[0].pkg.dir] = matches[0].pkg
	}
	for _, name := range item.tests {
		matches := r.tests[name]
		if len(matches) != 1 {
			result.unresolved = append(result.unresolved, fmt.Sprintf("test %s (%d declarations)", name, len(matches)))
			continue
		}
		if _, ok := full[matches[0].file]; !ok {
			full[matches[0].file] = "contract test"
		}
		listingDirs[matches[0].pkg.dir] = true
	}

	// Orientation: the root guide and the doc.go of each anchor package.
	if _, err := os.Stat(filepath.Join(r.root, "AGENTS.md")); err == nil {
		info, _ := os.Stat(filepath.Join(r.root, "AGENTS.md"))
		r.sizes["AGENTS.md"] = int(info.Size())
		full["AGENTS.md"] = "orientation"
	}
	for dir := range anchorPkgs {
		doc := dir + "/doc.go"
		if _, ok := r.sizes[doc]; ok {
			if _, already := full[doc]; !already {
				full[doc] = "orientation"
			}
		}
	}

	// One-hop definitions: declarations of this module that an anchor names.
	for _, anchor := range anchors {
		for _, used := range r.references(anchor) {
			if counted[used] || used == anchor {
				continue
			}
			counted[used] = true
			if _, whole := full[used.file]; whole {
				continue
			}
			partial[used.file] += used.end - used.start
		}
	}

	for file, reason := range full {
		result.full = append(result.full, read{file: file, bytes: r.sizes[file], reason: reason})
	}
	for file, bytes := range partial {
		result.partial = append(result.partial, read{file: file, bytes: bytes, reason: "definitions used by an anchor"})
	}
	for dir := range listingDirs {
		result.listings = append(result.listings, read{file: dir + "/", bytes: r.listingBytes(dir), reason: "directory listing"})
	}
	for dir, pkg := range anchorPkgs {
		result.anchorPkgDirs = append(result.anchorPkgDirs, dir)
		seen := map[string]bool{}
		for _, decls := range pkg.decls {
			for _, decl := range decls {
				if !seen[decl.file] {
					seen[decl.file] = true
					result.searchFiles++
					result.searchBytes += r.sizes[decl.file]
				}
			}
		}
	}
	for _, list := range [][]read{result.full, result.partial, result.listings} {
		sort.Slice(list, func(i, j int) bool { return list[i].file < list[j].file })
	}
	sort.Strings(result.anchorPkgDirs)
	sort.Strings(result.unresolved)
	return result
}

// references resolves the package-level names an anchor declaration uses:
// bare identifiers declared in its own package and pkg.Name selectors whose
// qualifier is an import of this module. Method calls are not followed.
func (r *repository) references(anchor *declaration) []*declaration {
	local := map[string]bool{}
	ast.Inspect(anchor.node, func(node ast.Node) bool {
		switch node := node.(type) {
		case *ast.Field:
			for _, name := range node.Names {
				local[name.Name] = true
			}
		case *ast.AssignStmt:
			if node.Tok == token.DEFINE {
				for _, target := range node.Lhs {
					if ident, ok := target.(*ast.Ident); ok {
						local[ident.Name] = true
					}
				}
			}
		case *ast.RangeStmt:
			if node.Tok == token.DEFINE {
				for _, target := range []ast.Expr{node.Key, node.Value} {
					if ident, ok := target.(*ast.Ident); ok {
						local[ident.Name] = true
					}
				}
			}
		case *ast.ValueSpec:
			if _, isDecl := anchor.node.(*ast.FuncDecl); isDecl {
				for _, name := range node.Names {
					local[name.Name] = true
				}
			}
		}
		return true
	})
	found := map[*declaration]bool{}
	selectorNames := map[*ast.Ident]bool{}
	ast.Inspect(anchor.node, func(node ast.Node) bool {
		switch node := node.(type) {
		case *ast.SelectorExpr:
			selectorNames[node.Sel] = true
			qualifier, ok := node.X.(*ast.Ident)
			if !ok || local[qualifier.Name] {
				return true
			}
			if dir, imported := anchor.imports[qualifier.Name]; imported {
				if pkg := r.packages[dir]; pkg != nil {
					for _, decl := range pkg.decls[node.Sel.Name] {
						found[decl] = true
					}
				}
			}
		case *ast.KeyValueExpr:
			if key, ok := node.Key.(*ast.Ident); ok {
				selectorNames[key] = true
			}
		case *ast.Ident:
			if selectorNames[node] || local[node.Name] {
				return true
			}
			for _, decl := range anchor.pkg.decls[node.Name] {
				found[decl] = true
			}
		}
		return true
	})
	list := make([]*declaration, 0, len(found))
	for decl := range found {
		list = append(list, decl)
	}
	sort.Slice(list, func(i, j int) bool {
		if list[i].file != list[j].file {
			return list[i].file < list[j].file
		}
		return list[i].start < list[j].start
	})
	return list
}

// listingBytes is the size of a directory listing: one entry name per line.
func (r *repository) listingBytes(dir string) int {
	entries, err := os.ReadDir(filepath.Join(r.root, dir))
	if err != nil {
		return 0
	}
	total := 0
	for _, entry := range entries {
		total += len(entry.Name()) + 1
	}
	return total
}

func tokens(bytes int) int { return (bytes + 3) / 4 }

func (m measurement) totals() (files, partialFiles, readTokens, listingTokens int) {
	for _, item := range m.full {
		readTokens += tokens(item.bytes)
	}
	for _, item := range m.partial {
		readTokens += tokens(item.bytes)
	}
	for _, item := range m.listings {
		listingTokens += tokens(item.bytes)
	}
	return len(m.full), len(m.partial), readTokens, listingTokens
}

func printText(results []measurement) {
	for _, result := range results {
		files, partialFiles, readTokens, listingTokens := result.totals()
		fmt.Printf("== %s\n", result.task.name)
		for _, item := range result.full {
			fmt.Printf("  read     %7d tokens  %-14s %s\n", tokens(item.bytes), item.reason, item.file)
		}
		for _, item := range result.partial {
			fmt.Printf("  partial  %7d tokens  %-14s %s\n", tokens(item.bytes), "definitions", item.file)
		}
		for _, item := range result.listings {
			fmt.Printf("  listing  %7d tokens  %-14s %s\n", tokens(item.bytes), "", item.file)
		}
		for _, missing := range result.unresolved {
			fmt.Printf("  UNRESOLVED %s\n", missing)
		}
		fmt.Printf("  total: %d files in full, %d in part, %d tokens (%d read + %d listing)\n",
			files, partialFiles, readTokens+listingTokens, readTokens, listingTokens)
		fmt.Printf("  search space: %d non-test files, %d tokens, in %s\n\n",
			result.searchFiles, tokens(result.searchBytes), strings.Join(result.anchorPkgDirs, ", "))
	}
	printMarkdown(results)
}

func printMarkdown(results []measurement) {
	fmt.Println("| Task | Files in full | Files in part | Must-read tokens | of which listings | Search-space files | Search-space tokens |")
	fmt.Println("| --- | ---: | ---: | ---: | ---: | ---: | ---: |")
	var sumFiles, sumPartial, sumTokens, sumListing, sumSearchFiles, sumSearch int
	for _, result := range results {
		files, partialFiles, readTokens, listingTokens := result.totals()
		fmt.Printf("| %s | %d | %d | %d | %d | %d | %d |\n", result.task.name, files, partialFiles,
			readTokens+listingTokens, listingTokens, result.searchFiles, tokens(result.searchBytes))
		sumFiles += files
		sumPartial += partialFiles
		sumTokens += readTokens + listingTokens
		sumListing += listingTokens
		sumSearchFiles += result.searchFiles
		sumSearch += tokens(result.searchBytes)
	}
	fmt.Printf("| **Task set** | %d | %d | %d | %d | %d | %d |\n", sumFiles, sumPartial, sumTokens, sumListing, sumSearchFiles, sumSearch)
}
