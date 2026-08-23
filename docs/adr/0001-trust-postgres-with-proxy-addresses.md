# Trust PostgreSQL with proxy addresses

Magpie stores canonical proxy hosts in plaintext because IP sorting, subnet search, and blacklist range queries need address semantics. Literal IP hosts also have a native PostgreSQL `inet` projection; DNS hostnames do not. Database access controls and storage encryption protect addresses. Application encryption protects the username and password on each proxy access, and a keyed, case-sensitive fingerprint identifies the proxy route. We rejected encrypted IPs with blind prefix indexes because that design cannot preserve useful IP ordering and would require up to 32 extra index entries per route. Existing `proxies` and `user_proxies` table names remain so route IDs and their statistics do not need rewriting. [ADR 0003](0003-store-proxy-hosts-with-an-ip-projection.md) records the hostname extension and split storage model.

## Consequences

A PostgreSQL reader can see proxy hosts and ports but cannot decrypt proxy usernames and passwords without `PROXY_ENCRYPTION_KEY`. Redis has a different trust and performance decision documented in [ADR 0002](0002-keep-hot-proxy-credentials-plaintext-in-redis.md). Migrations that move legacy PostgreSQL credentials require the existing key.
