package checkerconfig

const proxyPlanShardCount = 4096

// Maps and plans are immutable once published. Editing routes copies only
// their shards; a million-route snapshot is never copied for one tag edit.
type ProxyPlans struct {
	shards [proxyPlanShardCount]map[uint64]*Plan
	count  int
}

func (p *ProxyPlans) Get(id uint64) *Plan {
	if p == nil {
		return nil
	}
	return p.shards[id&(proxyPlanShardCount-1)][id]
}
func (p *ProxyPlans) Len() int {
	if p == nil {
		return 0
	}
	return p.count
}
func (p *ProxyPlans) Range(visit func(uint64, *Plan)) {
	if p == nil {
		return
	}
	for _, shard := range p.shards {
		for id, plan := range shard {
			visit(id, plan)
		}
	}
}

// set is used only while building an unpublished snapshot.
func (p *ProxyPlans) set(id uint64, plan *Plan) {
	index := id & (proxyPlanShardCount - 1)
	if p.shards[index] == nil {
		p.shards[index] = make(map[uint64]*Plan)
	}
	if p.shards[index][id] == nil {
		p.count++
	}
	p.shards[index][id] = plan
}

// Merge preserves all unchanged shards and the preceding complete snapshot.
// Build the patch from current assignments of every changed route, including
// routes whose checker tags were removed since the preceding revision.
func Merge(previous, patch *Workspace, ids []uint64) *Workspace {
	patch.Proxies = mergeProxyPlans(previous.Proxies, patch.Proxies, ids)
	patch.Default = previous.Default
	return patch
}
func mergeProxyPlans(previous, patch *ProxyPlans, ids []uint64) *ProxyPlans {
	var next *ProxyPlans
	copied := make(map[uint64]bool)
	for _, id := range ids {
		before, after := previous.Get(id), patch.Get(id)
		if before == after || (before != nil && after != nil && before.Keys == after.Keys) {
			continue
		}
		if next == nil {
			next = &ProxyPlans{}
			if previous != nil {
				*next = *previous
			}
		}
		index := id & (proxyPlanShardCount - 1)
		if !copied[index] {
			old := next.shards[index]
			cloned := make(map[uint64]*Plan, len(old)+1)
			for route, plan := range old {
				cloned[route] = plan
			}
			next.shards[index] = cloned
			copied[index] = true
		}
		if after == nil {
			delete(next.shards[index], id)
			next.count--
		} else {
			next.shards[index][id] = after
			if before == nil {
				next.count++
			}
		}
	}
	if next == nil {
		return previous
	}
	return next
}
