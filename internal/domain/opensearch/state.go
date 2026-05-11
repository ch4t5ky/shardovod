package opensearch

type ClusterState struct {
	Nodes  map[string]*Node
	Shards map[string]*Shard
}

func NewClusterState() *ClusterState {
	return &ClusterState{
		Nodes:  make(map[string]*Node),
		Shards: make(map[string]*Shard),
	}
}
