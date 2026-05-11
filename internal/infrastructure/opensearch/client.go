package opensearch

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"strings"

	ops "github.com/ch4t5ky/shardovod/internal/domain/opensearch"
	"github.com/opensearch-project/opensearch-go/v4"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
	log "github.com/sirupsen/logrus"
)

type OpensearchClient struct {
	client *opensearchapi.Client
}

func NewOpensearch(addresses []string, username, password string) (*OpensearchClient, error) {
	client, err := opensearchapi.NewClient(
		opensearchapi.Config{
			Client: opensearch.Config{
				Transport: &http.Transport{
					TLSClientConfig: &tls.Config{
						InsecureSkipVerify: true,
					},
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
		nodeID := ""
		if s.Node != nil {
			nodeID = *s.Node
		}

		role := "p"
		if s.Prirep != "p" {
			key := fmt.Sprintf("%s:%d", s.Index, s.Shard)
			replicaIdx[key]++
			role = fmt.Sprintf("r%d", replicaIdx[key])
		}

		shard := ops.NewShard(s.Index, s.Shard, role, nodeID, ops.ShardState(s.State))
		shards = append(shards, shard)
	}

	return shards, nil
}

func (c *OpensearchClient) GetNodes(ctx context.Context) ([]*ops.Node, error) {
	resp, err := c.client.Cat.Nodes(ctx, &opensearchapi.CatNodesReq{})
	if err != nil {
		return nil, fmt.Errorf("cat nodes: %w", err)
	}

	nodes := make([]*ops.Node, 0, len(resp.Nodes))
	for _, n := range resp.Nodes {
		nodes = append(nodes, ops.NewNode(n.Name, n.Name, parseRole(n.Role)))
	}

	return nodes, nil
}

// parseRole парсит поле node.role из CAT API
// Строка вида "dimr": d=data, i=ingest, m=master, r=remote_cluster
// Координирующая нода не имеет ролей → "-"
func parseRole(raw string) ops.NodeRole {
	switch {
	case strings.Contains(raw, "m"):
		return ops.NodeRoleMaster
	case strings.Contains(raw, "d"):
		return ops.NodeRoleData
	default:
		return ops.NodeRoleCoordinator
	}
}
