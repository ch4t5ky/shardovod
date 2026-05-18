package application

import (
	"context"
	"time"

	"github.com/ch4t5ky/shardovod/internal/domain/minecraft"
	"github.com/ch4t5ky/shardovod/internal/domain/opensearch"
	log "github.com/sirupsen/logrus"
)

const (
	indicesHologramName = "shardovod_indices"
	statsHologramName   = "shardovod_stats"
)

type OSClient interface {
	GetShards(ctx context.Context) ([]*opensearch.Shard, error)
	GetNodes(ctx context.Context) ([]*opensearch.Node, error)
	GetNodeStats(ctx context.Context, nodeId string) (*opensearch.NodeStats, error)
	GetIndices(ctx context.Context) ([]*opensearch.Index, error)
	GetClusterSettings(ctx context.Context) (*opensearch.ClusterSettings, error)
}

type minecraftCommander interface {
	SpawnSheep(ctx context.Context, sheep *minecraft.Sheep)
	UpdateSheepColor(ctx context.Context, sheepID string, color minecraft.Color)
	KillSheep(ctx context.Context, sheepID string)
	MoveSheep(ctx context.Context, sheepID string, to minecraft.Location)
	SheepExistsByName(ctx context.Context, name string) (bool, error)
	KillAllSheeps(ctx context.Context)

	BuildPen(ctx context.Context, pen *minecraft.Pen)
	SetPenOffline(ctx context.Context, pen *minecraft.Pen)
	DestroyAllPens(ctx context.Context, bounds minecraft.Bounds)

	CreateHologram(ctx context.Context, name string, loc minecraft.Location)
	AddHologramLine(ctx context.Context, name, text string)
	SetHologramLine(ctx context.Context, name string, line int, text string)
	DeleteHologram(ctx context.Context, name string)
	RemoveHologramLine(ctx context.Context, name string, line int)
	HologramExists(ctx context.Context, name string) (bool, error)
}

type Syncer struct {
	osClient   OSClient
	commander  minecraftCommander
	mapping    *Mapping
	interval   time.Duration
	penAreaMin minecraft.Location
	penAreaMax minecraft.Location
	penCols    int

	prevShards  map[string]*opensearch.Shard // shardID → Shard
	prevNodes   map[string]*opensearch.Node  // nodeID → Node
	prevIndices map[string]*opensearch.Index // indexID -> Index

	sheep        map[string]*minecraft.Sheep // sheepID → Sheep
	pens         map[string]*minecraft.Pen   // penID → Pen
	clusterState *opensearch.ClusterState

	indicesHologram *minecraft.Hologram
	statsHologram   *minecraft.Hologram
}

func NewSyncer(
	osClient OSClient,
	commander minecraftCommander,
	interval time.Duration,
	penAreaMin minecraft.Location,
	penAreaMax minecraft.Location,
	indicesHologramLoc minecraft.Location,
	statsHologramLoc minecraft.Location,
) *Syncer {
	cols := (penAreaMax.X - penAreaMin.X) / (minecraft.PenWidth + 1)
	if cols < 1 {
		cols = 1
	}

	return &Syncer{
		osClient:        osClient,
		commander:       commander,
		mapping:         NewMapping(),
		interval:        interval,
		penAreaMin:      penAreaMin,
		penAreaMax:      penAreaMax,
		penCols:         cols,
		prevShards:      make(map[string]*opensearch.Shard),
		prevNodes:       make(map[string]*opensearch.Node),
		prevIndices:     make(map[string]*opensearch.Index),
		sheep:           make(map[string]*minecraft.Sheep),
		pens:            make(map[string]*minecraft.Pen),
		clusterState:    nil,
		indicesHologram: minecraft.NewHologram(indicesHologramName, indicesHologramLoc),
		statsHologram:   minecraft.NewHologram(statsHologramName, statsHologramLoc),
	}
}

func (s *Syncer) RegisterPen(pen *minecraft.Pen, nodeID string) {
	s.pens[pen.PenID] = pen
	s.mapping.BindNode(pen.PenID, nodeID)
}

func (s *Syncer) RegisterSheep(sheep *minecraft.Sheep, shardID string) {
	s.sheep[sheep.SheepID] = sheep
	s.mapping.BindShard(sheep.SheepID, shardID)
}

func (s *Syncer) Run(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.tick(ctx); err != nil {
				log.Errorf("[syncer] tick error: %v", err)
			}
		}
	}
}

func (s *Syncer) Bootstrap(ctx context.Context) error {
	s.commander.DestroyAllPens(ctx, minecraft.Bounds{
		Min: s.penAreaMin,
		Max: s.penAreaMax,
	})
	s.commander.KillAllSheeps(ctx)

	for _, h := range []*minecraft.Hologram{s.indicesHologram, s.statsHologram} {
		s.commander.DeleteHologram(ctx, h.Name)
	}

	settings, err := s.osClient.GetClusterSettings(ctx)
	if err != nil {
		log.Warnf("[syncer] can't get cluster settings: %v", err)
		return err

	}
	s.clusterState = opensearch.NewClusterState()
	s.clusterState.Settings = settings

	log.Infof("[syncer] max_shards_per_node = %d", s.clusterState.Settings.MaxShardsPerNode)

	return nil
}

func (s *Syncer) tick(ctx context.Context) error {
	s.reconcileSheepPresence(ctx)

	nodes, err := s.osClient.GetNodes(ctx)
	if err != nil {
		log.Errorf("[syncer] failed to get nodes: %v", err)
	}
	s.reconcileNodes(ctx, nodes)

	shards, err := s.osClient.GetShards(ctx)
	if err != nil {
		log.Errorf("[syncer] failed to get shards: %v", err)
	}
	s.reconcileShards(ctx, shards)

	indices, err := s.osClient.GetIndices(ctx)
	if err != nil {
		log.Errorf("[syncer] failed to get indices: %v", err)
	}
	s.reconcileIndices(ctx, indices)

	s.reconcileNodeStats(ctx)

	return nil
}
