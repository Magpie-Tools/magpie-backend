package database

import "sort"

// normalizeWorkspaceIDs returns a sorted, deduplicated copy of workspace IDs.
func normalizeWorkspaceIDs(workspaceIDs []uint) []uint {
	if len(workspaceIDs) == 0 {
		return nil
	}

	normalized := append([]uint(nil), workspaceIDs...)
	sort.Slice(normalized, func(i, j int) bool {
		return normalized[i] < normalized[j]
	})

	out := normalized[:0]
	var previous uint
	hasPrevious := false
	for _, workspaceID := range normalized {
		if hasPrevious && workspaceID == previous {
			continue
		}
		out = append(out, workspaceID)
		previous = workspaceID
		hasPrevious = true
	}

	return out
}
