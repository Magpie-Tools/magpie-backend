// Package archtest holds the architecture tests that run under go test ./...
// and the data they enforce. The rules and allowlists are text files in the
// rules directory: the allowed-imports table, forbidden dependency directions,
// the pinned imports of the hot-path packages, the internal/database line
// ceiling and importer list, the transaction allowlist, the connection-creation
// sites and the per-package variable counts.
//
// An allowlist only shrinks. A failing rule names the file to change and why;
// see AGENTS.md and
// docs/adr/0015-extract-feature-packages-behind-architecture-tests.md.
package archtest
