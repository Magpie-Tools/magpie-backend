package checkerconfig

import (
	"sync"
	"testing"
)

func TestMergeKeepsPublishedPlansImmutable(t *testing.T) {
	defaultPlan := &Plan{Keys: [4]string{"default"}}
	originalPlan := &Plan{Keys: [4]string{"original"}}
	replacement := &Plan{Keys: [4]string{"replacement"}}
	added := &Plan{Keys: [4]string{"added"}}
	previous := &Workspace{Default: defaultPlan, Proxies: &ProxyPlans{}}
	previous.Proxies.set(1, originalPlan)
	previous.Proxies.set(4097, originalPlan)
	previous.Proxies.set(2, originalPlan)
	patch := &Workspace{Default: &Plan{}, Proxies: &ProxyPlans{}}
	patch.Proxies.set(1, replacement)
	patch.Proxies.set(8193, added)

	var readers sync.WaitGroup
	readers.Add(1)
	go func() {
		defer readers.Done()
		for range 10000 {
			if previous.Plan(1) != originalPlan || previous.Plan(4097) != originalPlan || previous.Plan(8193) != defaultPlan || previous.Proxies.Len() != 3 {
				t.Error("published snapshot changed while a worker read it")
				return
			}
		}
	}()
	for range 100 {
		next := Merge(previous, &Workspace{Default: patch.Default, Proxies: patch.Proxies}, []uint64{1, 4097, 8193})
		if next.Default != defaultPlan || next.Plan(1) != replacement || next.Plan(4097) != defaultPlan || next.Plan(8193) != added || next.Plan(2) != originalPlan || next.Proxies.Len() != 3 {
			t.Fatal("patch did not replace, remove, and add the affected routes")
		}
	}
	readers.Wait()
	identical := &ProxyPlans{}
	identical.set(1, &Plan{Keys: originalPlan.Keys})
	if got := mergeProxyPlans(previous.Proxies, identical, []uint64{1, 999}); got != previous.Proxies {
		t.Fatal("unchanged plans copied a published snapshot")
	}
}

func BenchmarkTaggedSnapshotLookup(b *testing.B) {
	w := &Workspace{Default: &Plan{}, Proxies: &ProxyPlans{}}
	shared := &Plan{Keys: [4]string{"tagged"}}
	for id := uint64(1); id <= 1000000; id++ {
		w.Proxies.set(id, shared)
	}
	state.Lock()
	state.workspaces = map[uint]*Workspace{1: w}
	state.ready = true
	state.Unlock()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		snapshot, _ := Lookup(1)
		if snapshot.Plan(uint64(i%1000000+1)) != shared {
			b.Fatal("missing tagged plan")
		}
	}
}
