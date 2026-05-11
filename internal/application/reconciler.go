package application

import (
	"context"

	mc "github.com/ch4t5ky/shardovod/internal/domain/minecraft"
	minecraft "github.com/ch4t5ky/shardovod/internal/domain/minecraft"
	opensearch "github.com/ch4t5ky/shardovod/internal/domain/opensearch"
	os "github.com/ch4t5ky/shardovod/internal/domain/opensearch"
	log "github.com/sirupsen/logrus"
)

// ---------- Shards -----------------------------------------------------------

func (s *Syncer) reconcileShards(ctx context.Context, shards []*os.Shard) {
	current := indexShards(shards)

	for _, shard := range shards {
		if shard.IsSystem() {
			continue
		}
		prev, existed := s.prevShards[shard.ShardID]
		switch {
		case !existed:
			s.onShardAdded(ctx, shard)
		case prev.State != shard.State || prev.NodeID != shard.NodeID:
			s.onShardUpdated(ctx, prev, shard)
		}
		s.prevShards[shard.ShardID] = shard
	}

	for id, prev := range s.prevShards {
		if _, ok := current[id]; !ok {
			s.onShardRemoved(ctx, prev)
			delete(s.prevShards, id)
		}
	}
}

func (s *Syncer) onShardAdded(ctx context.Context, shard *os.Shard) {
	sheepID := shard.ShardID
	sheep := minecraft.NewSheep(sheepID, "", shard.ShardID, s.origin)
	sheep.Color = SheepColor(shard.State, shard.IsPrimary())

	var spawnLoc mc.Location
	if shard.IsUnassigned() {
		spawnLoc = s.unassignedLocation()
	} else {
		penID, ok := s.mapping.PenByNode(shard.NodeID)
		if !ok {
			log.Warnf("[syncer] no pen for node %s on shard add", shard.NodeID)
			spawnLoc = s.unassignedLocation()
		} else {
			spawnLoc = s.pens[penID].SpawnLocation()
		}
	}
	sheep.Position = spawnLoc

	s.sheep[sheepID] = sheep
	s.mapping.BindShard(sheepID, shard.ShardID)

	s.commander.SpawnSheep(ctx, sheep, nil)
	log.Infof("[syncer] spawn sheep for shard %s", shard.ShardID)
}

func (s *Syncer) onShardUpdated(ctx context.Context, prev, next *os.Shard) {
	sheepID, ok := s.mapping.SheepByShard(next.ShardID)
	if !ok {
		return
	}
	sheep := s.sheep[sheepID]

	// цвет изменился → перекрасить
	if prev.State != next.State || prev.IsPrimary() != next.IsPrimary() {
		sheep.Color = SheepColor(next.State, next.IsPrimary())
		s.commander.UpdateSheepColor(ctx, sheep.SheepID, sheep.Color)
		log.Infof("[syncer] update sheep color for shard %s", next.ShardID)
	}

	// нода сменилась → переместить овцу
	if prev.NodeID != next.NodeID {
		var dest mc.Location
		if next.IsUnassigned() {
			dest = s.unassignedLocation()
			log.Infof("[syncer] move sheep %s to unassigned zone", sheepID)
		} else {
			penID, ok := s.mapping.PenByNode(next.NodeID)
			if !ok {
				log.Warnf("[syncer] no pen for node %s", next.NodeID)
				return
			}
			dest = s.pens[penID].SpawnLocation()
			log.Infof("[syncer] move sheep %s to pen of node %s", sheepID, next.NodeID)
		}
		s.commander.MoveSheep(ctx, sheepID, dest)
	}
}

func (s *Syncer) onShardRemoved(ctx context.Context, shard *opensearch.Shard) {
	sheepID, ok := s.mapping.SheepByShard(shard.ShardID)
	if !ok {
		return
	}
	s.commander.KillSheep(ctx, sheepID)

	s.mapping.UnbindShard(sheepID, shard.ShardID)
	delete(s.sheep, sheepID)
	log.Infof("[syncer] delete sheep for shard %s", shard.ShardID)
}

// ---------- Nodes ------------------------------------------------------------

func (s *Syncer) reconcileNodes(ctx context.Context, nodes []*os.Node) {
	current := indexNodes(nodes)

	for _, node := range nodes {
		if node.Role != os.NodeRoleData {
			continue
		}
		prev, existed := s.prevNodes[node.NodeID]
		switch {
		case !existed:
			s.onNodeAdded(ctx, node)
		case prev.Name != node.Name:
			s.onNodeUpdated(ctx, prev, node)
		}
		s.prevNodes[node.NodeID] = node
	}

	for id, prev := range s.prevNodes {
		if _, ok := current[id]; !ok {
			s.onNodeRemoved(ctx, prev)
			delete(s.prevNodes, id)
		}
	}
}

func (s *Syncer) onNodeAdded(ctx context.Context, node *os.Node) {
	if penID, ok := s.mapping.PenByNode(node.NodeID); ok {
		pen := s.pens[penID]
		pen.Status = mc.PenStatusActive
		s.commander.BuildPen(ctx, pen)
		log.Infof("[syncer] restore pen for node %s", node.NodeID)
		return
	}

	index := len(s.pens)
	pen := mc.NewPenAt("pen-"+node.NodeID, node.Name, s.penBase(), index)
	s.pens[pen.PenID] = pen
	s.mapping.BindNode(pen.PenID, node.NodeID)
	s.commander.BuildPen(ctx, pen)
	log.Infof("[syncer] build pen for node %s at index %d", node.NodeID, index)
}

func (s *Syncer) onNodeUpdated(ctx context.Context, prev, next *os.Node) {
	penID, ok := s.mapping.PenByNode(next.NodeID)
	if !ok {
		return
	}
	pen := s.pens[penID]
	pen.Name = next.Name
	s.commander.BuildPen(ctx, pen)
	log.Infof("[syncer] rename pen for node %s → %s", next.NodeID, next.Name)
}

func (s *Syncer) onNodeRemoved(ctx context.Context, node *os.Node) {
	penID, ok := s.mapping.PenByNode(node.NodeID)
	if !ok {
		return
	}
	pen := s.pens[penID]
	pen.Status = mc.PenStatusOffline
	s.commander.SetPenOffline(ctx, pen)
	log.Infof("[syncer] node %s offline, pen %s deactivated", node.NodeID, penID)
}

// ---------- Helpers ----------------------------------------------------------

func (s *Syncer) resolve(shard *os.Shard) (*mc.Sheep, *mc.Pen, bool) {
	sheepID, ok := s.mapping.SheepByShard(shard.ShardID)
	if !ok {
		log.Warnf("[syncer] no sheep for shard %s", shard.ShardID)
		return nil, nil, false
	}
	penID, ok := s.mapping.PenByNode(shard.NodeID)
	if !ok {
		log.Warnf("[syncer] no pen for node %s", shard.NodeID)
		return nil, nil, false
	}
	return s.sheep[sheepID], s.pens[penID], true
}

// penBase вычисляет Min первого загона из origin (центра первой ноды).
func (s *Syncer) penBase() mc.Location {
	return mc.Location{
		X: s.origin.X - mc.PenWidth/2,
		Y: s.origin.Y,
		Z: s.origin.Z - mc.PenDepth/2,
	}
}

// unassignedLocation — точка за пределами всех загонов для UNASSIGNED шардов.
func (s *Syncer) unassignedLocation() mc.Location {
	base := s.penBase()
	return mc.Location{
		X: base.X - 3,
		Y: s.origin.Y,
		Z: s.origin.Z,
	}
}

func indexShards(shards []*os.Shard) map[string]*os.Shard {
	idx := make(map[string]*os.Shard, len(shards))
	for _, s := range shards {
		idx[s.ShardID] = s
	}
	return idx
}

func indexNodes(nodes []*os.Node) map[string]*os.Node {
	idx := make(map[string]*os.Node, len(nodes))
	for _, n := range nodes {
		if n.Role == os.NodeRoleData {
			idx[n.NodeID] = n
		}
	}
	return idx
}
