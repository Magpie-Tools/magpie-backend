package archtest

import (
	"fmt"
	"go/ast"
	"go/token"
	"strconv"
	"testing"
)

func atoi(t *testing.T, file string, line ruleLine) int {
	t.Helper()
	if len(line.values) != 1 {
		t.Fatalf("%s:%d: %s takes one number", file, line.number, line.key)
	}
	value, err := strconv.Atoi(line.values[0])
	if err != nil {
		t.Fatalf("%s:%d: %v", file, line.number, err)
	}
	return value
}

func formatSignal(name string, lines, exports int, flag string) string {
	return fmt.Sprintf("  %-24s %6d lines %5d exports%s", name, lines, exports, flag)
}

// exportCount counts exported package-level identifiers and methods.
func exportCount(pkg *sourcePackage) int {
	exports := 0
	for _, file := range pkg.productionFiles() {
		for _, decl := range file.ast.Decls {
			switch decl := decl.(type) {
			case *ast.FuncDecl:
				if decl.Name.IsExported() {
					exports++
				}
			case *ast.GenDecl:
				if decl.Tok == token.IMPORT {
					continue
				}
				for _, spec := range decl.Specs {
					switch spec := spec.(type) {
					case *ast.TypeSpec:
						if spec.Name.IsExported() {
							exports++
						}
					case *ast.ValueSpec:
						for _, name := range spec.Names {
							if name.IsExported() {
								exports++
							}
						}
					}
				}
			}
		}
	}
	return exports
}
