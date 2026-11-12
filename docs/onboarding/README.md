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

The per-commit checks, the SQL-log comparisons and the open gates are in
[verification-log.md](verification-log.md).

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

### After step 0

Measured on the last commit of step 0.

| Task | Files in full | Files in part | Must-read tokens | of which listings | Search-space files | Search-space tokens |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Add a field to alert rules | 11 | 6 | 18200 | 2099 | 164 | 245762 |
| Change how failed proxies are requeued | 11 | 2 | 30443 | 192 | 6 | 20869 |
| Add a column to the proxy filter projection | 8 | 4 | 20129 | 1547 | 104 | 185437 |
| Change workspace invitation acceptance | 8 | 10 | 18080 | 1913 | 130 | 236592 |
| Change the email outbox retry policy | 8 | 3 | 11545 | 2066 | 120 | 211228 |
| **Task set** | 46 | 25 | 98397 | 7817 | 524 | 899888 |

The task set rose from 89,070 to 98,397 must-read tokens. Step 0 moved no code:
the rise is the root `AGENTS.md`, read once per task, and the new `doc.go` of
each anchor package. Deleting the deprecated wrappers removed one file from
`internal/database`, which four tasks count in their search space. This
checkpoint is the reference for the extraction steps that follow; the stop rule
compares each later checkpoint with the one before it.

### After `alerts`

Measured on the commit that adds this section, after `platform/pg`,
`platform/schema`, `evidence`, the test kit, `mail` and both `alerts` moves.

| Task | Files in full | Files in part | Must-read tokens | of which listings | Search-space files | Search-space tokens |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Add a field to alert rules | 11 | 6 | 15873 | 871 | 100 | 93605 |
| Change how failed proxies are requeued | 11 | 2 | 30642 | 192 | 6 | 20868 |
| Add a column to the proxy filter projection | 8 | 5 | 20270 | 1435 | 93 | 152144 |
| Change workspace invitation acceptance | 8 | 11 | 18200 | 1801 | 119 | 203318 |
| Change the email outbox retry policy | 7 | 4 | 10145 | 631 | 37 | 21606 |
| **Task set** | 45 | 28 | 95130 | 4930 | 355 | 491541 |

The task set fell from 98,397 to 95,130 must-read tokens (-3.3%), so the stop
rule does not fire. The drop comes from the two moved features:

- **Alert rules** fell from 18,200 to 15,873: the anchors now sit in a package
  of 15 files, so the directory listing shrank from 2,099 to 871 tokens and
  the alerts `doc.go` replaced the core's.
- **Email outbox** fell from 11,545 to 10,145 for the same reason.
- The other three tasks rose by 120 to 199 tokens each. Their anchors did not
  move; they pay for the longer `AGENTS.md` package map.

The search space a package boundary cannot rule out fell from 899,888 to
491,541 tokens, mostly for the two moved features (alerts 245,762 to 93,605,
mail 211,228 to 21,606).

### After the peripheral features (D5)

Measured on the commit that adds this section, after `account`, `abuseipdb`,
`geolite`, `blacklist`, `judgestore`, `scrapestore`, `devseed` and
`workspace`.

| Task | Files in full | Files in part | Must-read tokens | of which listings | Search-space files | Search-space tokens |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Add a field to alert rules | 11 | 6 | 16052 | 873 | 101 | 93754 |
| Change how failed proxies are requeued | 11 | 2 | 30815 | 192 | 6 | 20868 |
| Add a column to the proxy filter projection | 8 | 5 | 20329 | 1321 | 83 | 118434 |
| Change workspace invitation acceptance | 8 | 11 | 17269 | 643 | 64 | 77839 |
| Change the email outbox retry policy | 7 | 4 | 10326 | 633 | 38 | 21674 |
| **Task set** | 45 | 28 | 94791 | 3662 | 292 | 332569 |

The task set fell from 95,130 to 94,791 must-read tokens (-339, -0.4%). That
is a drop, so the stop rule as written does not fire. It is small, and it
comes from one task:

- **Workspace invitations** fell from 18,200 to 17,269 (-931): its anchors
  moved to `internal/workspace`, whose listing (643 tokens) replaced the
  core's (1,801).
- **Proxy filter projection** fell by the shrinking core listing (1,435 to
  1,321 tokens) but read the longer `AGENTS.md`, for +59 in total.
- The other three tasks rose by 173 to 181 tokens each: their anchors did not
  move, and the package map grew by the eight new packages.

The search space a package boundary cannot rule out fell from 491,541 to
332,569 tokens, mostly for the invitations task (203,318 to 77,839) and the
projection task (152,144 to 118,434).

The trend for the remaining work is visible in these numbers: a move lowers
the count only for tasks whose anchors leave `internal/database`; every new
package adds a row to `AGENTS.md`, which all five tasks read. Of the planned
work, only the `readmodel` pilot moves anchors of the task set (the
projection task).


### Re-run after the `readmodel` pilot (D7)

Measured on `59aab12`, the pilot commit, with the anchor resolver of
`277ebf1`. Four anchors had stopped resolving after changes that kept their
symbols (three functions became `checker.Runtime` methods, and
`ensureReadModelSchema` became `schema.EnsureReadModelSchema`); the resolver
now follows them. On `f5baa1f` it reproduces the D5 checkpoint exactly.

| Task | Files in full | Files in part | Must-read tokens | of which listings | Search-space files | Search-space tokens |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Add a field to alert rules | 11 | 6 | 16102 | 873 | 101 | 93949 |
| Change how failed proxies are requeued | 11 | 4 | 31935 | 201 | 7 | 21623 |
| Add a column to the proxy filter projection | 11 | 5 | 20841 | 1374 | 91 | 134198 |
| Change workspace invitation acceptance | 8 | 11 | 17319 | 643 | 64 | 78207 |
| Change the email outbox retry policy | 7 | 4 | 10376 | 633 | 38 | 21675 |
| **Task set** | 48 | 30 | 96573 | 3724 | 301 | 349652 |

The task set rose from 94,791 to 96,573 must-read tokens (+1,782, +1.9%).
That is no drop, so **the stop rule fires: no further package is split out of
the core.** The rise has four sources, each measured on its own commit:

| Commit | Change | Task set | Difference |
| --- | --- | ---: | ---: |
| `f5baa1f` | D5 checkpoint | 94,791 | |
| `189f40b` | Batcher owner, `CountActiveInstances`, GraphQL schema split | 95,223 | +432 |
| `e24dc03` | Pacer owner of the check usage meter | 95,861 | +638 |
| `0422769` | Read-model schema and chunk size to the platform, test preparation | 96,105 | +244 |
| `59aab12` | `readmodel` pilot | 96,573 | +468 |

- The requeue task grew by 1,120 tokens with the two hot-path ownership
  changes: its anchors are the checker worker's methods, which now read the
  runtime's Pacer.
- The projection task, the only one whose anchors left the core, grew by 512:
  its anchors now span `domain`, `platform/schema`, `readmodel` and the core's
  proxy list query. The pilot added `readmodel/doc.go` and a package-map row;
  the core's listing shrank (1,427 to 1,374 tokens), but by less.
- Every task reads the package map, which grew by one row.

The pilot is kept for its other results: a compiler-checked dependency
direction and a core 1,179 lines smaller, with the gates recorded in
[verification-log.md](verification-log.md). It does not make the task set
cheaper to read.
