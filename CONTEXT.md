# Magpie resource model

Magpie imports, checks, and organizes proxy routes for workspaces. User accounts gain access to workspace resources through memberships.

## Language

**User account**:
A person's login identity and personal credentials. A user account does not own managed infrastructure or contribute capacity.
_Avoid_: Workspace account, customer account

**Workspace**:
Magpie's resource, collaboration, quota, and subscription boundary. A workspace may have one member or many without changing its ownership model.
_Avoid_: Account, organization, team

**Workspace membership**:
A user account's access to one workspace, including its role and billing-administrator status. Removing a membership changes access only.
_Avoid_: Proxy access, seat ownership

**Workspace invitation**:
A workspace-owned, time-limited offer for an existing user account to become a workspace member with a specified role and billing-administrator status. It grants no workspace access before acceptance and remains manageable when its inviter leaves.
_Avoid_: Workspace membership, immediate member addition

**Workspace role**:
A membership's permission level inside one workspace. Workspace roles are owner, admin, operator, and viewer.
_Avoid_: User role, global role

**Workspace subscription**:
A workspace's plan and billing lifecycle. Member subscriptions and pooled member allowances do not exist.
_Avoid_: User subscription, donated plan

**Workspace capacity**:
The maximum number of managed proxies that may be active in a workspace. Capacity belongs to the workspace and does not change when a member leaves.
_Avoid_: Space, member allowance, donated slots

**Proxy route**:
A host and port combined with one exact credential pair. The host may be an IP address or DNS hostname, and DNS changes do not create a new route. Check measurements and reputation belong to this identity.
_Avoid_: Proxy endpoint, proxy record

**Managed proxy**:
A workspace's management of one proxy route, including its encrypted credential copy, lifecycle state, failure state, and tag assignments. One workspace-to-route association consumes one unit of managed route capacity.
_Avoid_: Proxy access, user proxy, proxy permission

**Paused managed proxy**:
A managed proxy retained by its workspace but excluded from active checking and rotation.
_Avoid_: Deleted proxy, removed proxy

**Managed proxy deletion**:
Removal of a workspace's management of a proxy route, including its failure state and tag assignments. Other workspaces' management of the same route is unaffected.
_Avoid_: Route deletion, proxy ban

**Failure streak**:
The number of consecutive eligible check cycles in which a managed proxy had no successful result. A successful cycle ends the streak.
_Avoid_: Total failures, failed requests

**Failure action**:
A workspace's chosen response when a managed proxy reaches its failure-streak threshold, either pausing or deleting that managed proxy.
_Avoid_: Automatic removal, failure cleanup

**Proxy tag**:
A workspace-owned name and color used to classify that workspace's managed proxies.
_Avoid_: Global proxy label, route tag

**Tag assignment**:
The association between one proxy tag and one managed proxy. A managed proxy may have several tag assignments.
_Avoid_: Proxy category

**Default checker settings**:
A workspace's baseline checker settings for every managed proxy in that workspace.
_Avoid_: Global checker settings, personal checker settings

**Tag checker rule**:
A workspace's adjustment to checker settings for managed proxies assigned one particular proxy tag.
_Avoid_: Tag default, proxy checker category

**Checker profile**:
The selected proxy protocols and shared transport, timeout, and retry choices in a workspace's Default settings or one tag checker rule.
_Avoid_: Per-protocol budget, protocol profile

**Effective checker settings**:
The checker settings applicable to one managed proxy after combining its workspace's defaults with every matching tag checker rule.
_Avoid_: Global tag settings, selected editor settings

**Managed proxy health**:
A workspace's assessment of a managed proxy based on results for its effective checker settings. Results from other workspaces or obsolete settings do not determine this assessment.
_Avoid_: Global proxy health, shared workspace health

**Checker rule priority**:
The relative precedence of tag checker rules that match the same managed proxy. Higher-priority rules apply after lower-priority rules.
_Avoid_: Tag creation order, tag name order

**Scrape source**:
A webpage URL from which Magpie extracts proxy routes.
_Avoid_: Proxy provider, source website

**Source subscription**:
A workspace's use of one scrape source, including its source settings.
_Avoid_: Source ownership, personal source

**Source tagging rule**:
A workspace's selection of tags to add to managed proxies observed in each future scrape of one subscribed source. These assignments accumulate with existing tags and remain until explicitly removed.
_Avoid_: Source label, automatic proxy category

**Organization**:
A future billing and identity parent for several workspaces. Organizations are not part of the current ownership path.
_Avoid_: Workspace

**Team**:
A future permission group inside one workspace. A team does not own resources or capacity.
_Avoid_: Workspace, membership
