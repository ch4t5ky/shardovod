package application

import (
	"context"
	"fmt"
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
	SpawnSheep(ctx context.Context, sheep *minecraft.Sheep, pen *minecraft.Pen)
	UpdateSheepColor(ctx context.Context, sheepID string, color minecraft.Color)
	KillSheep(ctx context.Context, sheepID string)
	BuildPen(ctx context.Context, pen *minecraft.Pen)
	SheepExistsByName(ctx context.Context, name string) (bool, error)
	KillAllSheeps(ctx context.Context)
	SetPenOffline(ctx context.Context, pen *minecraft.Pen)
	MoveSheep(ctx context.Context, sheepID string, to minecraft.Location)
}

type Syncer struct {
	osClient  OSClient
	commander minecraftCommander
	mapping   *Mapping
	interval  time.Duration
	origin    minecraft.Location

	prevShards map[string]*opensearch.Shard // shardID → Shard
	prevNodes  map[string]*opensearch.Node  // nodeID → Node
	sheep      map[string]*minecraft.Sheep  // sheepID → Sheep
	pens       map[string]*minecraft.Pen    // penID → Pen
}

func NewSyncer(
	osClient OSClient,
	commander minecraftCommander,
	interval time.Duration,
	origin minecraft.Location,
) *Syncer {
	return &Syncer{
		osClient:   osClient,
		commander:  commander,
		mapping:    NewMapping(),
		interval:   interval,
		origin:     origin,
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
	s.commander.KillAllSheeps(ctx)

	shards, err := s.osClient.GetShards(ctx)
	if err != nil {
		return fmt.Errorf("bootstrap: get shards: %w", err)
	}

	// актуальные shardID из OpenSearch
	current := make(map[string]struct{}, len(shards))
	for _, shard := range shards {
		if shard.IsSystem() && shard.NodeID != "" {
			current[shard.ShardID] = struct{}{}
		}
	}

	// проверяем каждый шард — есть ли овца в мире
	for _, shard := range shards {
		if shard.IsSystem() || shard.NodeID == "" {
			continue
		}

		exists, err := s.commander.SheepExistsByName(ctx, shard.ShardID)
		if err != nil {
			log.Warnf("[syncer] bootstrap: check sheep for shard %s: %v", shard.ShardID, err)
			continue
		}

		if exists {
			// восстанавливаем маппинг
			penID, ok := s.mapping.PenByNode(shard.NodeID)
			if !ok {
				log.Warnf("[syncer] bootstrap: no pen for node %s", shard.NodeID)
				continue
			}
			sheep := minecraft.NewSheep(shard.ShardID, penID, shard.ShardID, s.origin)
			sheep.Color = SheepColor(shard.State, shard.IsPrimary())
			s.sheep[sheep.SheepID] = sheep
			s.mapping.BindShard(sheep.SheepID, shard.ShardID)
		}
	}

	// убиваем стейл-овец — те, что в маппинге, но не в текущих шардах
	for shardID, sheepID := range s.mapping.shardToSheep {
		if _, ok := current[shardID]; !ok {
			log.Infof("[syncer] bootstrap: killing stale sheep for shard %s", shardID)
			s.commander.KillSheep(ctx, sheepID)
			s.mapping.UnbindShard(sheepID, shardID)
			delete(s.sheep, sheepID)
		}
	}

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
