# Onboarding measurement

The package extraction in
[ADR 0015](../adr/0015-extract-feature-packages-behind-architecture-tests.md)
exists to make the backend cheaper to learn and safer to change. This directory
holds the fixed task set that tests that claim, the written procedure that
measures it, and every checkpoint.

## Task set

[tasks.txt](tasks.txt) fixes five tasks: add a field to alert rules, change how
failed proxies are requeued, add a column to the proxy filter projection,
change workspace invitation acceptance, and change the email outbox retry
policy. Each task names its **anchors**, the declarations that must change or
cannot be avoided, and its **contract tests**. Anchors are symbol names, never
paths, so a moved file is still found and a pure move does not edit the task
list.

## Procedure

`go run ./internal/tools/onboarding` (or `make onboarding`) applies these rules
mechanically. It parses the source and depends on no other tool, so anyone can
repeat a measurement on any revision with `-root`.

For each task, the files that must be read are:

1. **Orientation.** The root `AGENTS.md` when it exists, and the `doc.go` of
   every package that declares an anchor.
2. **Location.** One directory listing for every directory that holds an
   anchor or a contract test, counted as the bytes of its entry names.
3. **Anchor files**, in full.
4. **Definitions used by an anchor**, one hop: every package-level function,
   type, variable or constant of this module that an anchor declaration names,
   counted at the size of that declaration with its doc comment. Bare names
   resolve inside the anchor's package and `pkg.Name` through the file's
   imports. Method calls are not followed. A file already read in full is not
   counted again.
5. **Contract test files**, in full.

Tokens are bytes divided by four, rounded up per file.

The tool also prints the **search space**: the non-test files, and their
tokens, of the packages that declare the task's anchors. That is the code a
package boundary cannot rule out. It is context, not the stop-rule metric.

## Stop rule

Extraction stops when a re-run shows no further drop in **must-read tokens for
the task set**. Checkpoints are taken after step 0, after `alerts`, and after
the peripheral features.

What this metric can and cannot show, decided before any move was made:

- A pure move changes no symbol and no reference, so anchor files, used
  definitions and contract tests stay the same size. Only orientation and
  location can change: a task whose anchors leave a large package stops paying
  for that package's listing and `doc.go`.
- Documentation adds to what is read. Step 0 therefore raises the number; the
  count cannot express that a map makes the remaining reading easier to find.

## Checkpoints

### Baseline: `3f66f5d`, before step 0

| Task | Files in full | Files in part | Must-read tokens | of which listings | Search-space files | Search-space tokens |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Add a field to alert rules | 6 | 6 | 16286 | 2097 | 165 | 246449 |
| Change how failed proxies are requeued | 8 | 2 | 28591 | 182 | 6 | 20869 |
| Add a column to the proxy filter projection | 5 | 4 | 18323 | 1549 | 105 | 186124 |
| Change workspace invitation acceptance | 4 | 10 | 16210 | 1913 | 131 | 237278 |
| Change the email outbox retry policy | 4 | 3 | 9660 | 2064 | 121 | 212000 |
| **Task set** | 27 | 25 | 89070 | 7805 | 528 | 902720 |
