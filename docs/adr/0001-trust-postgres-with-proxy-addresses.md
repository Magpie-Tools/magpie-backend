# Trust PostgreSQL with proxy addresses

Magpie stores proxy IP addresses as native PostgreSQL `inet` values because IP sorting, subnet search, and blacklist range queries need address semantics. Database access controls and storage encryption protect addresses. Application encryption protects the username and password on each proxy access, and a keyed, case-sensitive fingerprint identifies the proxy route. We rejected encrypted IPs with blind prefix indexes because that design cannot preserve useful IP ordering and would require up to 32 extra index entries per route. Existing `proxies` and `user_proxies` table names remain so route IDs and their statistics do not need rewriting.

## Consequences

A PostgreSQL reader can see proxy IP addresses and ports but cannot decrypt proxy usernames and passwords without `PROXY_ENCRYPTION_KEY`. Redis has a different trust and performance decision documented in [ADR 0002](0002-keep-hot-proxy-credentials-plaintext-in-redis.md). Migrations that move legacy PostgreSQL credentials require the existing key.
