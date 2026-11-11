// Command sqllog turns a PostgreSQL statement log into a digest of
// per-transaction statement sequences with their multiplicity. Two digests of
// the same tests are equal when the code issues the same statements, in the
// same order, inside the same transactions.
//
// scripts/sql-log.sh starts a disposable PostgreSQL with log_statement=all and
// log_line_prefix='SQL|%p|%v|', runs the tests listed in
// docs/onboarding/sql-log.md and pipes the server log through this command.
//
// Statements are grouped by backend process and virtual transaction ID, so an
// explicit transaction is one sequence and an autocommit statement is its own.
// Values that differ between runs are normalized: schema names with a time
// suffix, quoted literals, long numbers, placeholder numbers, savepoint names,
// the clause order of CREATE TABLE, and the length of placeholder lists and
// repeated VALUES rows (variable IN lists).
package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
)

const prefix = "SQL|"

type transaction struct {
	statements []string
}

var (
	statementStart = regexp.MustCompile(`^LOG:\s+(?:statement|execute [^:]*): (.*)$`)
	whitespace     = regexp.MustCompile(`\s+`)
	quoted         = regexp.MustCompile(`'(?:[^']|'')*'`)
	timeSuffix     = regexp.MustCompile(`_\d{9,}`)
	longNumber     = regexp.MustCompile(`\b\d{9,}\b`)
	placeholder    = regexp.MustCompile(`\$\d+`)
	placeholders   = regexp.MustCompile(`\$\?(?:\s*,\s*\$\?)+`)
	statementCache = regexp.MustCompile(`\b(?:stmtcache|stmt)_[0-9a-f]+\b`)
	savepoint      = regexp.MustCompile(`(?i)\bSAVEPOINT \S+`)
)

func main() {
	ignorePath := flag.String("ignore", "", "file of regular expressions; a normalized statement matching one is dropped")
	flag.Parse()
	var ignored []*regexp.Regexp
	if *ignorePath != "" {
		raw, err := os.ReadFile(*ignorePath)
		if err != nil {
			fmt.Fprintln(os.Stderr, "sqllog:", err)
			os.Exit(1)
		}
		for _, line := range strings.Split(string(raw), "\n") {
			line = strings.TrimSpace(line)
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			ignored = append(ignored, regexp.MustCompile(line))
		}
	}

	transactions := map[string]*transaction{}
	var order []string
	var current *strings.Builder
	var currentKey string
	flush := func() {
		if current == nil {
			return
		}
		statement := normalize(current.String())
		current = nil
		for _, pattern := range ignored {
			if pattern.MatchString(statement) {
				return
			}
		}
		entry := transactions[currentKey]
		if entry == nil {
			entry = &transaction{}
			transactions[currentKey] = entry
			order = append(order, currentKey)
		}
		entry.statements = append(entry.statements, statement)
	}

	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 0, 1<<20), 64<<20)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, prefix) {
			// A continuation line of a multi-line statement.
			if current != nil && (strings.HasPrefix(line, "\t") || strings.HasPrefix(line, " ")) {
				current.WriteString(" ")
				current.WriteString(line)
			}
			continue
		}
		flush()
		fields := strings.SplitN(line, "|", 4)
		if len(fields) != 4 {
			continue
		}
		match := statementStart.FindStringSubmatch(fields[3])
		if match == nil {
			continue
		}
		currentKey = fields[1] + "|" + fields[2]
		current = &strings.Builder{}
		current.WriteString(match[1])
	}
	flush()
	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "sqllog:", err)
		os.Exit(1)
	}

	counts := map[string]int{}
	bodies := map[string]string{}
	statements := 0
	for _, key := range order {
		body := strings.Join(transactions[key].statements, "\n")
		statements += len(transactions[key].statements)
		sum := sha256.Sum256([]byte(body))
		id := hex.EncodeToString(sum[:8])
		counts[id]++
		bodies[id] = body
	}
	ids := make([]string, 0, len(counts))
	for id := range counts {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return bodies[ids[i]] < bodies[ids[j]] })
	fmt.Printf("# %d transactions, %d distinct sequences, %d statements\n", len(order), len(ids), statements)
	for _, id := range ids {
		fmt.Printf("\n== %s x%d\n%s\n", id, counts[id], bodies[id])
	}
}

func normalize(statement string) string {
	statement = whitespace.ReplaceAllString(strings.TrimSpace(statement), " ")
	statement = quoted.ReplaceAllString(statement, "'?'")
	statement = timeSuffix.ReplaceAllString(statement, "_N")
	statement = longNumber.ReplaceAllString(statement, "N")
	statement = statementCache.ReplaceAllString(statement, "stmt")
	statement = placeholder.ReplaceAllString(statement, "$?")
	statement = placeholders.ReplaceAllString(statement, "$?..")
	statement = savepoint.ReplaceAllString(statement, "SAVEPOINT sp")
	return collapseRows(sortTableDefinition(statement))
}

// sortTableDefinition orders the column and constraint clauses of a CREATE
// TABLE statement. GORM emits constraints in map order, which differs from run
// to run.
func sortTableDefinition(statement string) string {
	if !strings.HasPrefix(statement, "CREATE TABLE ") {
		return statement
	}
	open := strings.Index(statement, "(")
	if open < 0 {
		return statement
	}
	end := closing(statement, open)
	if end < 0 {
		return statement
	}
	var clauses []string
	depth, start := 0, open+1
	for index := open + 1; index < end; index++ {
		switch statement[index] {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				clauses = append(clauses, strings.TrimSpace(statement[start:index]))
				start = index + 1
			}
		}
	}
	clauses = append(clauses, strings.TrimSpace(statement[start:end]))
	sort.Strings(clauses)
	return statement[:open+1] + strings.Join(clauses, ",") + statement[end:]
}

// collapseRows replaces a run of identical parenthesized groups, such as the
// rows of a multi-row VALUES list, with one group followed by ",..".
func collapseRows(statement string) string {
	var out strings.Builder
	index := 0
	for index < len(statement) {
		if statement[index] != '(' {
			out.WriteByte(statement[index])
			index++
			continue
		}
		end := closing(statement, index)
		if end < 0 {
			out.WriteString(statement[index:])
			break
		}
		group := statement[index : end+1]
		next := end + 1
		repeats := 0
		for {
			cursor := next
			for cursor < len(statement) && statement[cursor] == ' ' {
				cursor++
			}
			if cursor >= len(statement) || statement[cursor] != ',' {
				break
			}
			cursor++
			for cursor < len(statement) && statement[cursor] == ' ' {
				cursor++
			}
			if !strings.HasPrefix(statement[cursor:], group) {
				break
			}
			next = cursor + len(group)
			repeats++
		}
		if repeats > 0 {
			out.WriteString(group)
			out.WriteString(",..")
			index = next
			continue
		}
		// Descend so nested repeated groups are collapsed too.
		out.WriteByte('(')
		index++
	}
	return out.String()
}

// closing returns the index of the parenthesis that closes the one at open.
func closing(statement string, open int) int {
	depth := 0
	for index := open; index < len(statement); index++ {
		switch statement[index] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return index
			}
		}
	}
	return -1
}
