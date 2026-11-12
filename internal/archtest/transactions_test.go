package archtest

import (
	"fmt"
	"go/ast"
	"sort"
	"strings"
	"testing"
)

const gormPath = "gorm.io/gorm"

// transactionControls are the calls that begin, end or nest a transaction.
var transactionControls = map[string]bool{
	"Transaction": true, "Begin": true, "Commit": true, "Rollback": true, "SavePoint": true, "RollbackTo": true,
}

// takesGormHandle reports whether a function type has a *gorm.DB parameter.
func takesGormHandle(funcType *ast.FuncType, gormName string) bool {
	if funcType == nil || funcType.Params == nil || gormName == "" {
		return false
	}
	for _, field := range funcType.Params.List {
		star, ok := field.Type.(*ast.StarExpr)
		if !ok {
			continue
		}
		selector, ok := star.X.(*ast.SelectorExpr)
		if !ok || selector.Sel.Name != "DB" {
			continue
		}
		if qualifier, ok := selector.X.(*ast.Ident); ok && qualifier.Name == gormName {
			return true
		}
	}
	return false
}

// transactionControlCalls lists the transaction-control method calls in a body.
func transactionControlCalls(body *ast.BlockStmt) []string {
	var calls []string
	if body == nil {
		return calls
	}
	ast.Inspect(body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		if selector, ok := call.Fun.(*ast.SelectorExpr); ok && transactionControls[selector.Sel.Name] {
			calls = append(calls, selector.Sel.Name)
		}
		return true
	})
	return calls
}

// TestTransactionOwnership enforces the transaction rule per function, without
// call-graph analysis: outside internal/database, a function that receives a
// *gorm.DB runs on its caller's handle, so it may not call Transaction, Begin,
// Commit, Rollback, SavePoint or RollbackTo. The gorm import is resolved per
// file, so an aliased import is covered. A package that opens its own
// transactions does so from a function without a handle parameter.
func TestTransactionOwnership(t *testing.T) {
	loaded := repo(t)
	allowed := map[string]int{}
	for _, line := range readRules(t, "transactions.txt") {
		if line.key != "allow" || len(line.values) != 1 {
			t.Fatalf("transactions.txt:%d: want \"allow: package.Function\"", line.number)
		}
		allowed[line.values[0]] = line.number
	}
	seen := map[string]bool{}
	for _, pkg := range loaded.sortedPackages() {
		if pkg.name == "database" {
			continue
		}
		for _, file := range pkg.productionFiles() {
			gormName := ""
			for local, path := range importNames(file.ast) {
				if path == gormPath {
					gormName = local
				}
			}
			if gormName == "" {
				continue
			}
			for _, decl := range file.ast.Decls {
				function, ok := decl.(*ast.FuncDecl)
				if !ok {
					continue
				}
				name := pkg.name + "." + functionName(function)
				var violations []string
				if takesGormHandle(function.Type, gormName) {
					violations = append(violations, transactionControlCalls(function.Body)...)
				}
				ast.Inspect(function, func(node ast.Node) bool {
					literal, ok := node.(*ast.FuncLit)
					if ok && takesGormHandle(literal.Type, gormName) {
						violations = append(violations, transactionControlCalls(literal.Body)...)
					}
					return true
				})
				if len(violations) == 0 {
					continue
				}
				if _, ok := allowed[name]; ok {
					seen[name] = true
					continue
				}
				sort.Strings(violations)
				t.Errorf("%s (%s) receives a *gorm.DB and calls %s. A function on its caller's handle must not begin, "+
					"commit or roll back; open the transaction in the owning package's store instead. "+
					"A pure move may list the function in rules/transactions.txt until a follow-up reshapes it.",
					name, file.path, strings.Join(uniqueStrings(violations), ", "))
			}
		}
	}
	for name, line := range allowed {
		if !seen[name] {
			t.Errorf("transactions.txt:%d: %s no longer breaks the transaction rule; remove the line", line, name)
		}
	}
}

func functionName(function *ast.FuncDecl) string {
	if function.Recv == nil || len(function.Recv.List) == 0 {
		return function.Name.Name
	}
	receiver := function.Recv.List[0].Type
	if star, ok := receiver.(*ast.StarExpr); ok {
		receiver = star.X
	}
	if index, ok := receiver.(*ast.IndexExpr); ok {
		receiver = index.X
	}
	if ident, ok := receiver.(*ast.Ident); ok {
		return fmt.Sprintf("%s.%s", ident.Name, function.Name.Name)
	}
	return function.Name.Name
}

func uniqueStrings(values []string) []string {
	var unique []string
	for index, value := range values {
		if index == 0 || value != values[index-1] {
			unique = append(unique, value)
		}
	}
	return unique
}
