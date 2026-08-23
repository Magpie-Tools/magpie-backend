# Proxy management

Magpie imports, checks, and organizes proxy routes for individual users.

## Language

**Proxy route**:
A host and port combined with one exact credential pair. The host may be an IP address or DNS hostname, and DNS changes do not create a new route. Health and reputation belong to this identity.
_Avoid_: Proxy endpoint, proxy record

**Proxy access**:
A user's ownership of a proxy route, including that user's credential copy and failure state.
_Avoid_: User proxy, proxy permission
