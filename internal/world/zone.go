package world

import (
	"github.com/inview-club/shardovod/internal/infrastructure/ops"
)

// BuildZoneList returns an ordered zone list derived from a shard slice.
// "UNASSIGNED" is always first; node names follow in order of appearance.
func BuildZoneList(shards []ops.Shard) []string {
	seen := map[string]bool{"UNASSIGNED": true}
	zones := []string{"UNASSIGNED"}
	for _, s := range shards {
		if s.Node != "" && !seen[s.Node] {
			seen[s.Node] = true
			zones = append(zones, s.Node)
		}
	}
	return zones
}
