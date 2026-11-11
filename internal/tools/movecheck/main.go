// Command movecheck verifies that a change is a pure move: every top-level
// declaration of the new tree is, token for token, a declaration of the old
// tree once package qualifiers and the export case of identifiers are
// normalized. It is the "rename-only diff" that leaf moves are verified by.
//
//	go run ./internal/tools/movecheck OLD-DIR NEW-DIR
//	scripts/move-check.sh BASE-REVISION        # compares a revision with the working tree
//
// For each declaration it compares the token sequence, ignoring comments and
// formatting, after two normalizations:
//
//   - a qualifier that names a package of this module is dropped, so pg.Open
//     and Open compare equal (a local variable with a package's name is not a
//     qualifier and is kept);
//   - the first letter of every identifier is compared case-insensitively, so
//     isPostgresDialect and IsPostgresDialect compare equal.
//
// It prints the declarations that changed, were added or were removed, and
// exits non-zero when any changed. Added and removed declarations are listed
// for the reviewer: a pure move adds only the wrappers its message explains.
package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/scanner"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

const modulePath = "magpie"

type declaration struct {
	name string // normalized
	file string
	text string // normalized token sequence
	test bool
}

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: movecheck OLD-DIR NEW-DIR")
		os.Exit(2)
	}
	before, err := load(os.Args[1])
	if err != nil {
		fail(err)
	}
	after, err := load(os.Args[2])
	if err != nil {
		fail(err)
	}

	// Pair declarations with identical normalized text first; what remains
	// changed, appeared or disappeared.
	remaining := map[string][]*declaration{}
	for _, decl := range before {
		key := decl.name + "\x00" + decl.text
		remaining[key] = append(remaining[key], decl)
	}
	moved, unmatchedAfter := 0, []*declaration{}
	for _, decl := range after {
		key := decl.name + "\x00" + decl.text
		if candidates := remaining[key]; len(candidates) > 0 {
			match := candidates[0]
			// Prefer the candidate from the same file, so an unmoved
			// declaration does not count as moved.
			for index, candidate := range candidates {
				if candidate.file == decl.file {
					match = candidate
					candidates[0], candidates[index] = candidates[index], candidates[0]
					break
				}
			}
			remaining[key] = candidates[1:]
			if match.file != decl.file {
				moved++
			}
			continue
		}
		unmatchedAfter = append(unmatchedAfter, decl)
	}
	unmatchedBefore := map[string][]*declaration{}
	for _, candidates := range remaining {
		for _, decl := range candidates {
			unmatchedBefore[decl.name] = append(unmatchedBefore[decl.name], decl)
		}
	}

	var changed, added, removed []string
	for _, decl := range unmatchedAfter {
		if candidates := unmatchedBefore[decl.name]; len(candidates) > 0 {
			old := candidates[0]
			unmatchedBefore[decl.name] = candidates[1:]
			changed = append(changed, fmt.Sprintf("%s  %s -> %s\n%s", label(decl), old.file, decl.file, firstDifference(old.text, decl.text)))
			continue
		}
		added = append(added, fmt.Sprintf("%s  %s", label(decl), decl.file))
	}
	for _, candidates := range unmatchedBefore {
		for _, decl := range candidates {
			removed = append(removed, fmt.Sprintf("%s  %s", label(decl), decl.file))
		}
	}
	sort.Strings(changed)
	sort.Strings(added)
	sort.Strings(removed)

	fmt.Printf("declarations: %d before, %d after, %d moved to another file unchanged\n", len(before), len(after), moved)
	printList("changed", changed)
	printList("added", added)
	printList("removed", removed)
	if len(changed) > 0 {
		os.Exit(1)
	}
}

func label(decl *declaration) string {
	if decl.test {
		return decl.name + " (test)"
	}
	return decl.name
}

func printList(title string, items []string) {
	fmt.Printf("%s: %d\n", title, len(items))
	for _, item := range items {
		fmt.Println("  " + strings.ReplaceAll(item, "\n", "\n    "))
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "movecheck:", err)
	os.Exit(2)
}

func load(root string) ([]*declaration, error) {
	var declarations []*declaration
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
			relative, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			found, err := parseFile(path, filepath.ToSlash(relative))
			declarations = append(declarations, found...)
			return err
		})
		if err != nil {
			return nil, err
		}
	}
	return declarations, nil
}

func parseFile(path, relative string) ([]*declaration, error) {
	source, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	fset := token.NewFileSet()
	parsed, err := parser.ParseFile(fset, path, source, parser.ParseComments)
	if err != nil {
		return nil, err
	}
	qualifiers := map[string]bool{}
	for _, spec := range parsed.Imports {
		importPath, err := strconv.Unquote(spec.Path.Value)
		if err != nil || !strings.HasPrefix(importPath, modulePath+"/") {
			continue
		}
		local := importPath[strings.LastIndex(importPath, "/")+1:]
		if spec.Name != nil {
			local = spec.Name.Name
		}
		qualifiers[local] = true
	}
	// Only an identifier the file does not declare can name an imported
	// package. A local variable called like a package is not a qualifier.
	packageUses := map[int]bool{}
	for _, ident := range parsed.Unresolved {
		if qualifiers[ident.Name] {
			packageUses[fset.Position(ident.Pos()).Offset] = true
		}
	}
	isTest := strings.HasSuffix(relative, "_test.go")
	var declarations []*declaration
	add := func(name string, node ast.Node) {
		start, end := fset.Position(node.Pos()).Offset, fset.Position(node.End()).Offset
		declarations = append(declarations, &declaration{
			name: foldFirst(name), file: relative, test: isTest,
			text: normalize(source[start:end], start, packageUses),
		})
	}
	for _, decl := range parsed.Decls {
		switch decl := decl.(type) {
		case *ast.FuncDecl:
			name := decl.Name.Name
			if decl.Recv != nil && len(decl.Recv.List) > 0 {
				name = receiverName(decl.Recv.List[0].Type) + "." + name
			}
			add(name, decl)
		case *ast.GenDecl:
			if decl.Tok == token.IMPORT {
				continue
			}
			for _, spec := range decl.Specs {
				switch spec := spec.(type) {
				case *ast.TypeSpec:
					add(spec.Name.Name, spec)
				case *ast.ValueSpec:
					names := make([]string, 0, len(spec.Names))
					for _, name := range spec.Names {
						names = append(names, foldFirst(name.Name))
					}
					add(decl.Tok.String()+" "+strings.Join(names, ","), spec)
				}
			}
		}
	}
	return declarations, nil
}

func receiverName(expr ast.Expr) string {
	if star, ok := expr.(*ast.StarExpr); ok {
		expr = star.X
	}
	if index, ok := expr.(*ast.IndexExpr); ok {
		expr = index.X
	}
	if ident, ok := expr.(*ast.Ident); ok {
		return ident.Name
	}
	return "?"
}

// normalize returns the declaration's tokens, one per line, without comments,
// with module qualifiers dropped and identifier first letters folded.
func normalize(source []byte, base int, packageUses map[int]bool) string {
	fset := token.NewFileSet()
	file := fset.AddFile("", fset.Base(), len(source))
	var scan scanner.Scanner
	scan.Init(file, source, nil, 0)
	type item struct {
		tok    token.Token
		lit    string
		offset int
	}
	var items []item
	for {
		pos, tok, lit := scan.Scan()
		if tok == token.EOF {
			break
		}
		if tok == token.SEMICOLON && lit == "\n" {
			continue // an automatically inserted semicolon is formatting
		}
		items = append(items, item{tok, lit, base + file.Offset(pos)})
	}
	var out strings.Builder
	for index := 0; index < len(items); index++ {
		current := items[index]
		if current.tok == token.IDENT && packageUses[current.offset] &&
			index+2 < len(items) && items[index+1].tok == token.PERIOD && items[index+2].tok == token.IDENT {
			index++ // drop the qualifier and its period
			continue
		}
		switch {
		case current.tok == token.IDENT:
			out.WriteString(foldFirst(current.lit))
		case current.lit != "":
			out.WriteString(current.lit)
		default:
			out.WriteString(current.tok.String())
		}
		out.WriteByte('\n')
	}
	return out.String()
}

func foldFirst(name string) string {
	first, size := utf8.DecodeRuneInString(name)
	if first == utf8.RuneError {
		return name
	}
	return string(unicode.ToLower(first)) + name[size:]
}

// firstDifference shows the tokens around the first place two sequences differ.
func firstDifference(before, after string) string {
	old, updated := strings.Split(before, "\n"), strings.Split(after, "\n")
	index := 0
	for index < len(old) && index < len(updated) && old[index] == updated[index] {
		index++
	}
	context := func(tokens []string) string {
		start, end := index-6, index+8
		if start < 0 {
			start = 0
		}
		if end > len(tokens) {
			end = len(tokens)
		}
		return strings.Join(tokens[start:end], " ")
	}
	return "- " + context(old) + "\n+ " + context(updated)
}
