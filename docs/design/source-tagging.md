# Automatic tags for scrape sources

The user confirmed this design for implementation.

## Agreed behavior

- Any source subscription can select several existing tags from its workspace's tag catalog. An empty selection disables automatic tagging.
- Configuration is available in the Add Sources dialog and on an existing source's detail page. The existing tag manager remains available for creating and editing tags.
- One tag selection applies to every newly added source in an import. Re-adding an already-subscribed source preserves its current automatic-tag configuration.
- Saving a rule does not tag historical source results. Each future scrape adds the configured tags to every accepted proxy it observes, including proxies the workspace already manages.
- Assignment is additive. Existing manual tags and tags assigned by other sources stay intact, and an already-assigned tag is not duplicated.
- If a user manually removes a configured tag from a proxy, a later scrape of that proxy from the source adds it again.
- Tags are assigned when scraped proxies are saved, including paused proxies and proxies that later fail health checks. Assignment does not change the managed proxy's lifecycle state.
- Results use the source's current saved tag selection when they are processed, including when the selection changes during a running scrape.
- Clearing or changing a rule, or removing its source subscription, leaves existing proxy tag assignments in place.

## Existing ownership and lifecycle

A scrape source is one webpage URL. Each workspace has its own subscription, source settings, tag catalog, and managed proxy assignments. Existing source-setting permissions apply: workspace operators, admins, and owners can edit rules, and viewers can read them.

Rules reference tag identities, so renaming a tag changes its displayed name. Deleting a tag removes its assignments and rule references; subsequent scraping does not recreate a deleted tag. Removing a source subscription removes that workspace's rule. Adding the source again starts a new subscription with the settings chosen for that import.

Only workspaces that participated in the scrape and still subscribe using its fetch mode receive assignments. The same route can belong to other workspaces that did not scrape this source, so the route's full list of workspace owners must not determine tag recipients.

## Implementation and verification

Persist tag selections against workspace source subscriptions in PostgreSQL through the existing migration path. Keep optional source-setting updates independent: omitted tag configuration preserves the current selection, while an explicit empty selection clears it. Validate tag ownership and save related source-setting changes atomically.

Add missing assignments during scrape ingestion after managed proxies exist. Use bounded or set-based inserts with duplicate protection and current subscription, fetch-mode, and tag ownership checks. Rules do not need manual-assignment provenance or per-proxy suppression state.

The checker operation budget remains unchanged: zero additional cryptographic operations, JSON encodes, Redis commands, and database queries per proxy check. Tag rules and assignments remain outside proxy queue payloads.

Scrape ingestion adds one set-based assignment statement per batch of up to 500
participating workspaces and 500 observed proxy IDs, inside a transaction. Each
statement binds at most 1,003 parameters and inserts only assignments selected
by live rules. Repeated observations use `ON CONFLICT DO NOTHING`.

Verify workspace and fetch-mode isolation, rediscovery of existing proxies, additive assignments, idempotence, restoration after manual removal, paused proxies, current settings during ingestion, tag/source deletion, and import behavior. Verify the Add Sources and source-detail controls, including permissions and clearing a selection.

Update the scraping and proxy user guides, source REST API documentation, storage reference, and deployment repository's feature and migration guidance with the implementation. Run backend tests, race tests, vet, and build, plus the relevant frontend checks and documentation builds.

See [ADR-0008](../adr/0008-treat-source-tags-as-persistent-assignments.md) for the assignment-lifetime decision and [CONTEXT.md](../../CONTEXT.md) for the domain terms.
