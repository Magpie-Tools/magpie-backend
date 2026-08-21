# Keep hot proxy credentials plaintext in Redis by default

Magpie continuously checks millions of proxy routes. Encrypting credentials when requeueing and decrypting them when dequeuing made authenticated encryption the checker's dominant CPU cost. PostgreSQL remains the encrypted system of record, while Redis is treated as private, trusted, high-throughput runtime infrastructure. Queue credentials are therefore plaintext by default. Current queue payloads carry the already-computed keyed route fingerprint, and ordinary requeues update only scheduling state.

`PROXY_QUEUE_ENCRYPT_CREDENTIALS=true` remains available for deployments whose Redis threat model requires application-level credential encryption. That mode accepts per-dequeue decryption overhead; payload encryption happens only when a payload is added or changed. Legacy plaintext and encrypted payloads remain readable and are rewritten lazily into the selected current format.

## Consequences

A Redis reader or backup reader can see proxy IP addresses, ports, usernames, and passwords in the default mode. Redis must stay on a private network with authentication and restricted ACLs; remote connections should use TLS, and volumes and backups must be encrypted. High-volume installations should keep the default unless benchmarks show that encrypted mode meets their throughput target. Changing `PROXY_ENCRYPTION_KEY` before legacy encrypted queue payloads are rewritten prevents those payloads from being decrypted.
