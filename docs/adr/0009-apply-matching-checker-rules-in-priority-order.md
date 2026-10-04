---
status: accepted
---

# Apply matching checker rules in priority order

A managed proxy can have several tags, and their checker rules can conflict. Start with the workspace's Default settings and apply every matching rule in increasing priority order. The top tag has highest priority and applies last. Users arrange configured tags in the Tag priority popup with drag handles or Move up and Move down controls.

Default and each tag have one shared transport, timeout, and retry count for all enabled protocols. The user revised the original per-protocol design to keep the existing grouped settings editor and change request settings per tag. An Add rule's explicit settings therefore update both newly added and already-enabled checks. Protocol selection remains independent of those shared fields.

Replace chooses the rule's protocol set, Add merges it, and Remove excludes selected protocols. Remove changes only the selection. Transport, timeout, and retries inherit independently from Default and preceding matching rules. Re-enabling a protocol retains the current shared settings. Rules with an empty Add selection can change only the shared fields.

Judges, HTTPS routing for SOCKS, and automatic failure actions remain workspace-wide. Existing transport combinations still apply. Validation checks every reachable tag combination so SOCKS cannot inherit QUIC or HTTP/3. No new operations enter the checker hot path.

An empty effective selection skips checking without increasing the failure streak or taking a failure action. Edits affect the next scheduled check; a running check completes with its captured settings.
