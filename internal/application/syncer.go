package application

import (
	"context"
	"time"

	"github.com/ch4t5ky/shardovod/internal/domain/minecraft"
	"github.com/ch4t5ky/shardovod/internal/domain/opensearch"
	log "github.com/sirupsen/logrus"
)

type OSClient interface {
	GetShards(ctx context.Context) ([]*opensearch.Shard, error)
	GetNodes(ctx context.Context) ([]*opensearch.Node, error)
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
}

type Syncer struct {
	osClient   OSClient
	commander  minecraftCommander
	mapping    *Mapping
	interval   time.Duration
	penAreaMin minecraft.Location
	penAreaMax minecraft.Location
	penCols    int

	prevShards map[string]*opensearch.Shard // shardID → Shard
	prevNodes  map[string]*opensearch.Node  // nodeID → Node
	sheep      map[string]*minecraft.Sheep  // sheepID → Sheep
	pens       map[string]*minecraft.Pen    // penID → Pen
}

func NewSyncer(
	osClient OSClient,
	commander minecraftCommander,
	interval time.Duration,
	penAreaMin minecraft.Location,
	penAreaMax minecraft.Location,
) *Syncer {
	cols := (penAreaMax.X - penAreaMin.X) / (minecraft.PenWidth + 1)
	if cols < 1 {
		cols = 1
	}

	return &Syncer{
		osClient:   osClient,
		commander:  commander,
		mapping:    NewMapping(),
		interval:   interval,
		penAreaMin: penAreaMin,
		penAreaMax: penAreaMax,
		penCols:    cols,
		prevShards: make(map[string]*opensearch.Shard),
		prevNodes:  make(map[string]*opensearch.Node),
		sheep:      make(map[string]*minecraft.Sheep),
		pens:       make(map[string]*minecraft.Pen),
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
	return nil
}

func (s *Syncer) tick(ctx context.Context) error {
	nodes, err := s.osClient.GetNodes(ctx)
	if err != nil {
		log.Errorf("[syncer] failed to get nodes: %v", err)
	} else {
		s.reconcileNodes(ctx, nodes)
	}

	shards, err := s.osClient.GetShards(ctx)
	if err != nil {
		log.Errorf("[syncer] failed to get shards: %v", err)
	} else {
		s.reconcileShards(ctx, shards)
	}

	return nil
}
