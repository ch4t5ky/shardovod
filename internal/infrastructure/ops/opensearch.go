package ops

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"

	"github.com/opensearch-project/opensearch-go/v4"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
	log "github.com/sirupsen/logrus"
)

type Shard struct {
	Index  string `json:"index"`
	Shard  string `json:"shard"`
	Prirep string `json:"prirep"`
	State  string `json:"state"`
	Docs   string `json:"docs"`
	Node   string `json:"node"`
}

func (s Shard) IsPrimary() bool { return s.Prirep == "p" }

func (s Shard) DocsCount() int64 {
	v, err := strconv.ParseInt(strings.TrimSpace(s.Docs), 10, 64)
	if err != nil {
		return 0
	}
	return v
}

type diskRow struct {
	Node    string `json:"node"`
	DiskPct string `json:"disk.percent"`
}

type Client struct {
	raw *opensearch.Client
}

func New(addresses []string, username, password string) (*Client, error) {
	api, err := opensearchapi.NewClient(
		opensearchapi.Config{
			Client: opensearch.Config{
				Transport: &http.Transport{
					TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
				},
				Addresses: addresses,
				Username:  username,
				Password:  password,
			},
		},
	)
	if err != nil {
		log.Errorf("failed to create opensearch client: %s", err.Error())
		return nil, err
	}
	return &Client{raw: api.Client}, nil
}

func (c *Client) GetShards(ctx context.Context) ([]Shard, error) {
	body, err := c.getJSON(ctx, "/_cat/shards?format=json&bytes=b&h=index,shard,prirep,state,docs,node")
	if err != nil {
		return nil, err
	}

	var shards []Shard
	if err := json.Unmarshal(body, &shards); err != nil {
		return nil, fmt.Errorf("unmarshal shards: %w", err)
	}

	out := shards[:0]
	for _, s := range shards {
		out = append(out, s)
	}
	return out, nil
}

func (c *Client) GetNodeDiskUsage(ctx context.Context) (map[string]float64, error) {
	body, err := c.getJSON(ctx, "/_cat/allocation?format=json&h=node,disk.percent")
	if err != nil {
		return nil, err
	}

	var rows []diskRow
	if err := json.Unmarshal(body, &rows); err != nil {
		return nil, fmt.Errorf("unmarshal allocation: %w", err)
	}

	result := make(map[string]float64, len(rows))
	for _, row := range rows {
		if row.Node == "" || row.DiskPct == "" {
			continue
		}
		v, err := strconv.ParseFloat(strings.TrimSpace(row.DiskPct), 64)
		if err == nil {
			result[row.Node] = math.Max(0, math.Min(100, v))
		}
	}
	return result, nil
}

func (c *Client) getJSON(ctx context.Context, path string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	resp, err := c.raw.Perform(req)
	if err != nil {
		return nil, fmt.Errorf("perform: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d: %s", resp.StatusCode, body)
	}
	return body, nil
}
