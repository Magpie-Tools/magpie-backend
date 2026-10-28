# Reimport inventory work

The ordered-round candidate passed all twenty preparation stages, including
stopped repair, before its next representative catch-up attempt failed on
26 October. The SQL client probe exceeded its unchanged 15-second bound after
3,192.661 seconds of catch-up. All three successful projection samples matched
240 of 1,024 owner pairs; 49 intervals had no projection credit. The startup
observer had one collection timeout and application shutdown failed its drain.
These failures remain preserved. No recovery or load duration passed.

Failure-time activity showed projection WALWrite and probe WALInsert waits.
Startup observations also showed 279 subscription-lock waits, with query age up
to 165.195 seconds, and 151 active-inventory counts during repeated source
imports. The import transaction locked the workspace subscription, looked up
existing management, and then recounted active routes even when every imported
route already belonged to that workspace. Refreshing its billing usage repeated
the count and wrote unchanged capacity. This competes with ingestion, projection
updates and metric snapshots; the observations do not isolate a causal throughput
effect.

The import still takes its existing subscription lock. When all input routes
are already managed, planning preserves their lifecycle without a capacity
scan. Ownership insertion uses `ON CONFLICT DO NOTHING`, whose affected-row count
identifies actual additions. Existing credentials receive a separate bounded
UPDATE that changes no lifecycle columns and cannot recreate management deleted
between the two statements. Only an actual addition triggers the existing
authoritative capacity recount and usage refresh. New capacity-paused ownership
still follows the same quota and accounting path.

Credentials pass through the existing model validation/encryption hook once per
imported ownership. Both SQL statements use the prepared ciphertext; the extra
statement does not add another encryption pass. The credential UPDATE binds
three parameters per row and one workspace parameter, within the existing,
more conservative managed-row batch size. It changes only the selected
workspace's credentials and avoids writing identical ciphertext.

A conflict-containing import adds at most one bounded credential UPDATE per
ownership batch. An all-new batch needs no second statement. An import with no
new ownership eliminates the active-inventory recount, its usage upsert and
their table checks. Limited plans also avoid the planning recount for all-known
routes. This tradeoff replaces work proportional to the workspace inventory
with work bounded by the import. The physical checker loop adds zero crypto,
route hashes, JSON encodes, Redis commands, credential loads or database queries.
The plaintext Redis default and stable encryption key are unchanged.

One read-only diagnosis on the retained million-route/eight-workspace fixture
measured the eliminated count and retained point lookup, with unchanged resources
and indexes. The count used a parallel sequential scan and recorded 263,363
shared read blocks in 339.657 ms; the single-route ownership lookup used two
index scans and recorded six shared read blocks in 1.585 ms. These are different
queries run on stopped application data. Shared reads are not physical disk-byte
measurements, and these timings do not establish a live application speedup.
Both transactions were read-only and rolled back. Raw plans remain private in
`takeover-20261025/ordered-rounds-failure-20261026/import-count-work/`.

Six focused test groups passed normally in 21.432 seconds and under race in
66.213 seconds, without skips. They cover finite and unlimited quotas, existing
active/paused/archived ownership, unchanged usage/peak/deltas, mixed additions,
encrypted credential updates scoped to one owner, deletion between the conflict
probe and UPDATE, and the existing PostgreSQL import/parameter cases through
65,536 routes. The owned PostgreSQL stopped cleanly, without OOM or restart.

The incomplete older paired benchmark framework was not repeated. Fresh full
normal/race/vet/build/source checks, stopped repair and new concrete-candidate
review remain required on this source. Strict catch-up, coordinated recovery,
corrected smoke and all 25 load hours must still pass. Resource, index, SQL
deadline and acceptance limits are unchanged; item 4 remains deferred.
