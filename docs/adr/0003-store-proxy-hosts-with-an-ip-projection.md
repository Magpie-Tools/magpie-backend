# Store proxy hosts with an IP projection

Magpie treats a canonical host, port, and credential pair as the proxy route identity. The `host` column stores either an IP address or DNS hostname, while a nullable PostgreSQL `inet` projection stores literal IP addresses for CIDR search and blacklist matching. Converting the existing IP column to plain text was rejected because it would discard native address validation and indexed range operations.

## Consequences

A provider gateway remains one route when its DNS answers change. Magpie resolves the hostname through the normal network dialer during a check without an explicit pre-resolution step or additional database, Redis, JSON, or cryptographic work in the checker loop. GeoLite, AbuseIPDB, and IP blacklists apply only to literal IP routes because a gateway hostname may resolve to changing infrastructure addresses.
