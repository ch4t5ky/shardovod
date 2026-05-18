package opensearch

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"

	ops "github.com/ch4t5ky/shardovod/internal/domain/opensearch"
	"github.com/opensearch-project/opensearch-go/v4"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
	log "github.com/sirupsen/logrus"
)

type OpensearchClient struct {
	client *opensearchapi.Client
}

func NewOpensearch(
	addresses []string,
	username, password string,
	clientCertPath, clientKeyPath string,
) (*OpensearchClient, error) {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true, // только если реально нужно
	}

	// Клиентский сертификат + ключ
	if clientCertPath != "" && clientKeyPath != "" {
		cert, err := tls.LoadX509KeyPair(clientCertPath, clientKeyPath)
		if err != nil {
			log.Errorf("failed to load client certificate: %s", err.Error())
			return nil, err
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	client, err := opensearchapi.NewClient(
		opensearchapi.Config{
			Client: opensearch.Config{
				Transport: &http.Transport{
					TLSClientConfig: tlsConfig,
				},
				Addresses: addresses,
				Username:  username,
				Password:  password,
			},
		},
	)
	if err != nil {
		log.Errorf("failed to create client: %s", err.Error())
		return nil, err
	}
	return &OpensearchClient{
		client: client,
	}, nil
}

func (c *OpensearchClient) GetShards(ctx context.Context) ([]*ops.Shard, error) {
	resp, err := c.client.Cat.Shards(ctx, &opensearchapi.CatShardsReq{})
	if err != nil {
		return nil, fmt.Errorf("cat shards: %w", err)
	}

	shards := make([]*ops.Shard, 0, len(resp.Shards))
	replicaIdx := make(map[string]int) // "index:shardNum" → счётчик

	for _, s := range resp.Shards {
		node := ""
		if s.Node != nil {
			node = *s.Node
		}

		role := "p"
		if s.Prirep != "p" {
			key := fmt.Sprintf("%s:%d", s.Index, s.Shard)
			replicaIdx[key]++
			role = fmt.Sprintf("r%d", replicaIdx[key])
		}

		shard := ops.NewShard(s.Index, s.Shard, role, node, ops.ShardState(s.State))
		shards = append(shards, shard)
	}

	return shards, nil
}

func (c *OpensearchClient) GetNodes(ctx context.Context) ([]*ops.Node, error) {
	resp, err := c.client.Nodes.Info(ctx, &opensearchapi.NodesInfoReq{Params: opensearchapi.NodesInfoParams{}})
	if err != nil {
		return nil, fmt.Errorf("cat nodes: %w", err)
	}

	nodes := make([]*ops.Node, 0, len(resp.Nodes))

	for id, n := range resp.Nodes {
		role := parseRole(n.Roles)
		if role != ops.NodeRoleData {
			continue
		}
		nodes = append(nodes, ops.NewNode(id, n.Name, role))
	}

	return nodes, nil
}

func parseRole(roles []string) ops.NodeRole {
	for _, r := range roles {
		if r == "data" {
			return ops.NodeRoleData
		}
	}
	for _, r := range roles {
		if r == "master" || r == "cluster_manager" {
			return ops.NodeRoleMaster
		}
	}
	return ops.NodeRoleCoordinator
}

func (c *OpensearchClient) GetNodeStats(ctx context.Context, nodeID string) (*ops.NodeStats, error) {
	resp, err := c.client.Nodes.Stats(ctx, &opensearchapi.NodesStatsReq{
		NodeID: []string{nodeID},
	})
	if err != nil {
		return nil, fmt.Errorf("node stats %s: %w", nodeID, err)
	}

	n, ok := resp.Nodes[nodeID]
	if !ok {
		return nil, fmt.Errorf("node %s not found in stats response", nodeID)
	}

	diskUsed := n.FS.Total.TotalInBytes - n.FS.Total.AvailableInBytes
	diskPct := 0
	if n.FS.Total.TotalInBytes > 0 {
		diskPct = int(diskUsed * 100 / n.FS.Total.TotalInBytes)
	}

	return &ops.NodeStats{
		CPUPercent:  n.OS.CPU.Percent,
		HeapPercent: n.JVM.Mem.HeapUsedPercent,
		DiskPercent: diskPct,
	}, nil
}

func (c *OpensearchClient) GetIndices(ctx context.Context) ([]*ops.Index, error) {
	resp, err := c.client.Cat.Indices(ctx, &opensearchapi.CatIndicesReq{})
	if err != nil {
		return nil, fmt.Errorf("cat indices: %w", err)
	}

	indices := make([]*ops.Index, 0, len(resp.Indices))
	docsCount := 0
	size := ""
	for _, ind := range resp.Indices {
		if ind.DocsCount != nil {
			docsCount = *ind.DocsCount
		}

		if ind.StoreSize != nil {
			size = *ind.StoreSize
		}
		indices = append(indices, ops.NewIndex(ind.UUID, ind.Index, ops.IndexHealth(ind.Health), docsCount, size))
	}
	return indices, nil
}

func (c *OpensearchClient) GetClusterSettings(ctx context.Context) (*ops.ClusterSettings, error) {
	_, err := c.client.Cluster.GetSettings(ctx, &opensearchapi.ClusterGetSettingsReq{
		Params: opensearchapi.ClusterGetSettingsParams{
			IncludeDefaults: opensearchapi.ToPointer(true),
		},
	})
	if err != nil {
		return nil, fmt.Errorf("cluster settings: %w", err)
	}

	settings := &ops.ClusterSettings{
		MaxShardsPerNode: 1000, // fallback
	}

	return settings, nil
}
