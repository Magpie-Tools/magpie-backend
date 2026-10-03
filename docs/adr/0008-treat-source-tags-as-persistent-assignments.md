---
status: accepted
---

# Treat source tags as persistent assignments

Source tagging rules add missing ordinary workspace tag assignments when future scrapes observe proxies, including already-managed proxies, and restore manually removed tags on rediscovery while the rule remains configured. Saving a rule does not change previously scraped proxies, and changing or deleting a rule or its source leaves existing assignments in place. We chose additive, persistent assignments over source-derived labels because manual tags and tags from other sources must remain intact.
