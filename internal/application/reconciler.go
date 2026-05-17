package application

import (
	"context"
	"fmt"

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

func (s *Syncer) reconcileSheepPresence(ctx context.Context) {
	for sheepID, sheep := range s.sheep {
		exists, err := s.commander.SheepExistsByName(ctx, sheep.SheepID)
		if err != nil {
			log.Errorf("[syncer] check sheep %s: %v", sheepID, err)
			continue
		}
		if !exists {
			log.Infof("[syncer] sheep %s missing, respawning", sheepID)
			s.commander.SpawnSheep(ctx, sheep)
		}
	}
}

func (s *Syncer) onShardAdded(ctx context.Context, shard *os.Shard) {
	sheepID := shard.ShardID
	var spawnLoc mc.Location

	if shard.IsUnassigned() {
		loc, ok := s.findFreeLocation()
		if !ok {
			log.Warnf("[syncer] no free location for unassigned shard %s", shard.ShardID)
			return
		}
		spawnLoc = loc
	} else {
		penID, ok := s.mapping.PenByNode(shard.NodeID)
		if !ok {
			log.Warnf("[syncer] no pen for node %s on shard add", shard.NodeID)
			return
		}
		spawnLoc = s.pens[penID].SpawnLocation()
	}

	sheep := minecraft.NewSheep(sheepID, "", shard.ShardID, spawnLoc)
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
			loc, ok := s.findFreeLocation()
			if !ok {
				log.Warnf("[syncer] no free location for unassigned sheep %s", sheepID)
				return
			}
			dest = loc
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
		sheep.Position = dest
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

func (s *Syncer) findFreeLocation() (mc.Location, bool) {
	for x := s.penAreaMin.X; x <= s.penAreaMax.X; x += 2 {
		for z := s.penAreaMin.Z; z <= s.penAreaMax.Z; z += 2 {
			loc := mc.Location{X: x, Y: s.penAreaMin.Y, Z: z}
			if !s.isOccupied(loc) {
				return loc, true
			}
		}
	}
	return mc.Location{}, false
}

func (s *Syncer) isOccupied(loc mc.Location) bool {
	for _, pen := range s.pens {
		if pen.Bounds.Contains(loc) {
			return true
		}
	}
	return false
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

// ---------- Indices ------------------------------------------------------------
func (s *Syncer) reconcileIndices(ctx context.Context, indices []*opensearch.Index) {
	current := indexIndices(indices)

	visible := make([]*opensearch.Index, 0, len(indices))
	for _, idx := range indices {
		if !idx.IsSystem() {
			visible = append(visible, idx)
		}
	}

	prevVisible := make([]*opensearch.Index, 0, len(s.prevIndices))
	for _, idx := range s.prevIndices {
		if !idx.IsSystem() {
			prevVisible = append(prevVisible, idx)
		}
	}

	if len(s.prevIndices) == 0 {
		// первый тик — создаём
		s.commander.DeleteHologram(ctx, indicesHologramName)
		s.createIndicesHologram(ctx, visible)
	} else if len(visible) != len(prevVisible) {
		// количество изменилось — пересоздаём
		s.commander.DeleteHologram(ctx, indicesHologramName)
		s.createIndicesHologram(ctx, visible)
	} else {
		// количество то же — обновляем только изменившиеся строки
		// строки голограммы: 1=заголовок, 2..N=индексы
		for i, idx := range visible {
			prev, ok := s.prevIndices[idx.Id]
			if !ok || prev.Health != idx.Health || prev.DocsCount != idx.DocsCount || prev.Size != idx.Size {
				s.commander.SetHologramLine(ctx, indicesHologramName, i+2, formatIndexLine(idx))
			}
		}
	}

	s.prevIndices = current
}

func indexIndices(indices []*os.Index) map[string]*os.Index {
	idx := make(map[string]*os.Index, len(indices))
	for _, ind := range indices {
		idx[ind.Id] = ind
	}
	return idx
}

func (s *Syncer) createIndicesHologram(ctx context.Context, indices []*opensearch.Index) {
	s.commander.CreateHologram(ctx, indicesHologramName, s.hologramLoc)
	// первая строка уже создана — задаём заголовок через set
	s.commander.SetHologramLine(ctx, indicesHologramName, 1, "<#00BFFF>Indices</#4B0082>")
	// остальные строки добавляем через add
	for _, idx := range indices {
		s.commander.AddHologramLine(ctx, indicesHologramName, formatIndexLine(idx))
	}
}

func formatIndexLine(idx *opensearch.Index) string {
	color := healthColor(idx.Health)
	return fmt.Sprintf("%s%s &#AAAAAA%d docs %s", color, idx.Name, idx.DocsCount, idx.Size)
}
