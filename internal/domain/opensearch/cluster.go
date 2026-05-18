package opensearch

type ClusterState struct {
	Settings *ClusterSettings
}

func NewClusterState() *ClusterState {
	return &ClusterState{
		Settings: &ClusterSettings{
			MaxShardsPerNode: 1000,
		},
	}
}

type ClusterSettings struct {
	MaxShardsPerNode int
}
