package application

type Mapping struct {
	penToNode    map[string]string
	nodeToPen    map[string]string
	sheepToShard map[string]string
	shardToSheep map[string]string
}

func NewMapping() *Mapping {
	return &Mapping{
		penToNode:    make(map[string]string),
		nodeToPen:    make(map[string]string),
		sheepToShard: make(map[string]string),
		shardToSheep: make(map[string]string),
	}
}

func (m *Mapping) BindNode(penID, nodeName string) {
	m.penToNode[penID] = nodeName
	m.nodeToPen[nodeName] = penID
}

func (m *Mapping) BindShard(sheepID, shardID string) {
	m.sheepToShard[sheepID] = shardID
	m.shardToSheep[shardID] = sheepID
}

func (m *Mapping) NodeByPen(penID string) (string, bool) {
	id, ok := m.penToNode[penID]
	return id, ok
}

func (m *Mapping) PenByNode(nodeName string) (string, bool) {
	id, ok := m.nodeToPen[nodeName]
	return id, ok
}

func (m *Mapping) ShardBySheep(sheepID string) (string, bool) {
	id, ok := m.sheepToShard[sheepID]
	return id, ok
}

func (m *Mapping) SheepByShard(shardID string) (string, bool) {
	id, ok := m.shardToSheep[shardID]
	return id, ok
}

func (m *Mapping) UnbindShard(sheepID, shardID string) {
	delete(m.sheepToShard, sheepID)
	delete(m.shardToSheep, shardID)
}
