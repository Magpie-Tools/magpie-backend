# Remove duplicate proxy filter indexes

The million-route, eight-workspace release fixture had 32 indexes on
`user_proxy_filter_indexes`. Thirteen were exact copies under different names.
GORM's model tags created one set, and `ensureReadModelSchema` created another.
The copies used 1,075,077,120 bytes, about 1.00 GiB, at the time of the read-only
catalog snapshot.

At 09:37:49 UTC the table had about 1.40 GB of heap and 3.41 GB of indexes.
PostgreSQL's cumulative counters showed 138,974 updates and only 127 HOT updates.
Over the next 113.09 seconds, the database generated 2.359 GB of WAL. During that
window, projection update counters advanced by 14,431, ownership updates by
148,192, and proxy metadata updates by 88,520. The database was also inserting
latest evidence and recalculating reputations. Several sampled sessions waited
on WAL writes. A Docker sample showed PostgreSQL using 403.7% CPU and 3.50 GiB of
its 4 GiB memory limit.

These observations establish concurrent write pressure. They do not assign all
WAL or latency to the duplicate indexes. The same startup also ran expensive
snapshot/count readers, ingestion, reputation refresh and retention. The scoped
193-route projection plan used indexed ownership and per-workspace evidence
lookups, rather than a global latest-evidence scan. The application still timed
out projection batches and retried them as the dirty set grew to 5,000 routes.

The retained catalog snapshots and the unexecuted projection plan are in
`.codex-tmp/release-remediation-20261020/local-baseline/diagnosis/` in the workspace.

## Migration

The model tags now use the explicit schema's canonical index names. The schema
initializer removes only the thirteen known old names, and only when the
canonical index is ready, valid and equivalent. It compares access method, key
and included columns, collation, operator classes, sort/null order, expressions,
partial predicates, uniqueness and storage options. It preserves custom names,
constraint-owned indexes, replica identity and the selected cluster index.
PostgreSQL describes these semantics in its
[index catalog documentation](https://www.postgresql.org/docs/17/catalog-pg-index.html).
The comparison reads the `NULLS NOT DISTINCT` flag through JSON so PostgreSQL 14
can run it without that newer catalog column.

The default page index, active-route partial index, GiST and trigram indexes,
and the three filter-option expression indexes have different definitions or
names and remain in place. The migration uses ordinary `DROP INDEX` statements
during schema initialization. Those statements take table locks; this is not an
online migration procedure for an independently active workload.

`TestProxyFilterIndexStartupDeduplicatesAndStaysIdempotentPostgres` reproduces the
old physical copies, keeps projection data, runs AutoMigrate and initialization
twice, and verifies that every canonical definition remains unchanged.
`TestProxyFilterIndexCleanupPreservesDifferentAndCustomIndexesPostgres` exercises
different sort/null ordering, predicates, operator classes, collations, access
methods, included columns, expressions, storage settings and uniqueness. It also
keeps a custom duplicate and the chosen cluster index.

## Update-cost measurement

`TestProxyFilterIndexUpdateCostPopulatedPostgres` is an opt-in regression. It
prepopulates routes, eight shared ownerships per route and the complete
projection inventory before measurement. It seeds current latest evidence for
one quarter of the inventory and every candidate route. Each measured upsert
updates existing rows. It advances an indexed latest-check value before each
sample, matching the write requirement of a fresh checker result.

The comparison measures the original 32-index set, the 19 canonical indexes
after cleanup, and the final 23-index set with the new page and filter-option
indexes. Each phase measures three actual projection upserts for 193 and 5,000
global routes. `EXPLAIN ANALYZE` records execution time, buffers and WAL for each
statement. Assertions require zero inserts, exactly eight updated ownership rows
per requested route, less WAL-record work after cleanup, retained inventory and
matching current healthy evidence for every candidate owner. All original
foreign keys are restored and validated after bulk loading and before timing.

The disposable measurement database has four CPU slots, a 5 GiB memory limit,
512 MiB shared buffers and 16 MiB work memory. Its memory-plus-swap limit is
10 GiB. These differ from the 4 GiB database limit and 128 MiB shared buffers in
the actual release fixture. The isolated comparison measures index update cost;
the full release fixture must establish capacity under concurrent application
work.

Automatic vacuum remains enabled in this fixture. A read-only activity sample
observed vacuum of the newly populated projection table during the original
index set's update trials. Elapsed times can therefore include maintenance I/O;
the per-statement WAL records come directly from each analyzed upsert.

Reproduce with disposable PostgreSQL services:

```sh
MAGPIE_TEST_POSTGRES_DSN='host=127.0.0.1 port=32770 user=magpie_read_model_test password=magpie_read_model_test dbname=magpie_read_model_test sslmode=disable' \
  GOMAXPROCS=4 go test ./internal/database -run '^TestProxyFilterIndex' -count=1 -v

MAGPIE_TEST_POSTGRES_DSN='host=127.0.0.1 port=32770 user=magpie_read_model_test password=magpie_read_model_test dbname=magpie_read_model_test sslmode=disable' \
MAGPIE_TEST_PROXY_FILTER_INDEX_ROUTES=1000000 \
MAGPIE_TEST_PROXY_FILTER_INDEX_EVIDENCE_DIR=/absolute/path/to/evidence \
  GOMAXPROCS=4 go test ./internal/database -run '^TestProxyFilterIndexUpdateCostPopulatedPostgres$' -count=1 -v -timeout=30m
```

## Checker cost

The change adds no cryptographic operations, JSON encodes, Redis commands or
database statements per proxy check. Schema initialization performs one catalog
comparison and, on an affected installation, up to thirteen index drops.
Normal requeues and queue credential handling are unchanged.

The projection still updates indexed values for new checks. Removing duplicate
copies reduces index maintenance, but does not make those updates eligible for
[HOT](https://www.postgresql.org/docs/17/storage-hot.html). This patch does not
change duplicate invalidations from ingestion and reputation completion,
unconditional successful failure-counter resets, or metadata writes of unchanged
values. The next full preflight determines whether those paths need further work.

## Recorded million-route results

The isolated run completed with 1,000,000 routes, eight workspaces,
8,000,000 ownership rows and 8,000,000 preexisting projection rows.
It retained 2,041,152 latest pointers. All four original foreign keys
were valid during measurement. Every sample updated the existing candidate
owner rows and inserted zero projection rows.

| Global routes | Index set | Median SQL time | Mean WAL records | Mean WAL bytes |
| ---: | --- | ---: | ---: | ---: |
| 193 | Original 32 | 138.4 ms | 54,759 | 4.59 MiB |
| 193 | Canonical 19 | 97.9 ms | 34,286 | 3.17 MiB |
| 193 | Final 23 | 79.6 ms | 40,676 | 4.34 MiB |
| 5,000 | Original 32 | 6,482.5 ms | 1,415,987 | 453.64 MiB |
| 5,000 | Canonical 19 | 6,258.9 ms | 888,643 | 335.29 MiB |
| 5,000 | Final 23 | 6,018.5 ms | 1,052,065 | 366.41 MiB |

Duplicate cleanup reclaimed 1,034,584,064 bytes and took
1,284.0 ms. The initializer that recreated the four selected
read indexes took 28.07 seconds. Index size
snapshots across phases also include the bloat from preceding update samples;
the reclaimed-byte value compares sizes immediately before and after cleanup.

Samples ran sequentially in one database. Each phase warmed its candidate
rows first. The later phases inherit cache state and update bloat, and
`EXPLAIN ANALYZE` adds instrumentation overhead. These are measured index
update costs, not proof that concurrent release traffic meets its SLO.

Evidence includes all 18 analyzed plans, exact index definitions, source
hashes, resource limits and command results under
`.codex-tmp/release-remediation-20261020/projection-indexes/` in the workspace.

The targeted startup/idempotence and semantic-preservation tests also passed
on disposable PostgreSQL 14.24. PostgreSQL 14 correctly skips
the test that creates PostgreSQL 15 NULLS NOT DISTINCT indexes.
