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

	sheep := minecraft.NewSheep(sheepID, "", shard.ShardID, spawnLoc) // убрали origin
	sheep.Color = SheepColor(shard.State, shard.IsPrimary())

	s.sheep[sheepID] = sheep
	s.mapping.BindShard(sheepID, shard.ShardID)

	s.commander.SpawnSheep(ctx, sheep)
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

func (s *Syncer) nextPenPosition() (col, row int) {
	index := len(s.pens)
	// чётные — вправо от центра (0, 1, 2, ...)
	// нечётные — влево от центра (-1, -2, ...)
	var offset int
	if index%2 == 0 {
		offset = index / 2
	} else {
		offset = -(index + 1) / 2
	}

	centerRow := (s.penAreaMax.Z - s.penAreaMin.Z) / 2 / (minecraft.PenDepth + minecraft.PenGap)
	row = centerRow + offset
	col = 0
	return col, row
}

func (s *Syncer) onNodeAdded(ctx context.Context, node *os.Node) {
	if penID, ok := s.mapping.PenByNode(node.NodeID); ok {
		pen := s.pens[penID]
		pen.Status = mc.PenStatusActive
		s.commander.BuildPen(ctx, pen)
		log.Infof("[syncer] restore pen for node %s", node.NodeID)
		return
	}

	col, row := s.nextPenPosition()
	pen := mc.NewPenAtGrid("pen-"+node.NodeID, node.Name, s.penAreaMin, col, row)
	s.pens[pen.PenID] = pen
	s.mapping.BindNode(pen.PenID, node.NodeID)
	s.commander.BuildPen(ctx, pen)
	log.Infof("[syncer] build pen for node %s at col=%d row=%d", node.NodeID, col, row)
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

// unassignedLocation — точка за левым краем области загонов
func (s *Syncer) unassignedLocation() mc.Location {
	return mc.Location{
		X: s.penAreaMin.X - 3,
		Y: s.penAreaMin.Y,
		Z: s.penAreaMin.Z - 3,
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
