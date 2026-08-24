---
status: superseded by ADR-0005
---

# Keep proxy tags user-owned

A proxy route can be shared by several users, but tags represent each user's organization of their own proxy pool. Magpie stores tag assignments against proxy access records instead of global proxy routes, so assigning or renaming a tag never changes what another user sees. This preserves the route-sharing boundary, allows several tags per proxy access, and keeps tag data out of the Redis queue and checker loop.
