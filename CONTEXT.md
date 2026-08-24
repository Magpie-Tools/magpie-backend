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
A host and port combined with one exact credential pair. The host may be an IP address or DNS hostname, and DNS changes do not create a new route. Health and reputation belong to this identity.
_Avoid_: Proxy endpoint, proxy record

**Managed proxy**:
A workspace's management of one proxy route, including its encrypted credential copy, lifecycle state, failure state, and tag assignments. One workspace-to-route association consumes one unit of managed route capacity.
_Avoid_: Proxy access, user proxy, proxy permission

**Proxy tag**:
A workspace-owned name and color used to classify that workspace's managed proxies.
_Avoid_: Global proxy label, route tag

**Tag assignment**:
The association between one proxy tag and one managed proxy. A managed proxy may have several tag assignments.
_Avoid_: Proxy category

**Organization**:
A future billing and identity parent for several workspaces. Organizations are not part of the current ownership path.
_Avoid_: Workspace

**Team**:
A future permission group inside one workspace. A team does not own resources or capacity.
_Avoid_: Workspace, membership
