package opensearch

type NodeRole string

const (
	NodeRoleMaster      NodeRole = "master"
	NodeRoleData        NodeRole = "data"
	NodeRoleCoordinator NodeRole = "coordinator"
)

type Node struct {
	NodeID   string
	Name     string
	Role     NodeRole
	ShardIDs []string
}

func NewNode(nodeID, name string, role NodeRole) *Node {
	return &Node{
		NodeID:   nodeID,
		Name:     name,
		Role:     role,
		ShardIDs: []string{},
	}
}

func (n *Node) AddShard(shardID string) {
	n.ShardIDs = append(n.ShardIDs, shardID)
}

func (n *Node) RemoveShard(shardID string) {
	filtered := n.ShardIDs[:0]
	for _, id := range n.ShardIDs {
		if id != shardID {
			filtered = append(filtered, id)
		}
	}
	n.ShardIDs = filtered
}
