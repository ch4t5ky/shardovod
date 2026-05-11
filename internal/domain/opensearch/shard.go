package opensearch

import "fmt"

type ShardState string

const (
	ShardStateStarted      ShardState = "STARTED"
	ShardStateInitializing ShardState = "INITIALIZING"
	ShardStateRelocating   ShardState = "RELOCATING"
	ShardStateUnassigned   ShardState = "UNASSIGNED"
)

type Shard struct {
	ShardID   string // "{index}:{num}:{role}"
	IndexName string
	Shard     int
	Role      string // "p", "r1", "r2", ...
	NodeID    string
	State     ShardState
}

func NewShard(indexName string, shard int, role string, nodeID string, state ShardState) *Shard {
	return &Shard{
		ShardID:   fmt.Sprintf("%s:%d:%s", indexName, shard, role),
		IndexName: indexName,
		Shard:     shard,
		Role:      role,
		NodeID:    nodeID,
		State:     state,
	}
}

func (s *Shard) IsPrimary() bool {
	return s.Role == "p"
}

// IsSystem возвращает true для служебных индексов OpenSearch (.kibana, .opendistro и т.п.).
// Индексы data stream (.ds-*) НЕ считаются системными.
func (s *Shard) IsSystem() bool {
	return len(s.IndexName) > 0 &&
		s.IndexName[0] == '.' &&
		len(s.IndexName) > 3 &&
		s.IndexName[:4] != ".ds-"
}

// IsUnassigned возвращает true если шард не назначен ни одной ноде.
func (s *Shard) IsUnassigned() bool {
	return s.State == ShardStateUnassigned || s.NodeID == ""
}
