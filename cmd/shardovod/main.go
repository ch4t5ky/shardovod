package main

import (
	"context"

	"github.com/ch4t5ky/shardovod/internal/application"
	"github.com/ch4t5ky/shardovod/internal/config"
	mc "github.com/ch4t5ky/shardovod/internal/domain/minecraft"
	"github.com/ch4t5ky/shardovod/internal/infrastructure/minecraft"
	"github.com/ch4t5ky/shardovod/internal/infrastructure/opensearch"
	log "github.com/sirupsen/logrus"
)

func main() {
	cfg, err := config.New()
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	ctx := context.Background()

	mcComm := minecraft.NewCommander(cfg.RCONAddr, cfg.RCONPassword)
	defer mcComm.Close()

	ops, err := opensearch.NewOpensearch(cfg.OpenSearchAddresses, cfg.OpenSearchUsername, cfg.OpenSearchPassword)
	if err != nil {
		log.Fatalf("failed to create client: %v", err)
	}

	log.Infof("pen area: min=(%d,%d) max=(%d,%d) y=%d",
		cfg.PenAreaMinX, cfg.PenAreaMinZ,
		cfg.PenAreaMaxX, cfg.PenAreaMaxZ,
		cfg.PenAreaY,
	)

	syncer := application.NewSyncer(
		ops,
		mcComm,
		cfg.PollInterval,
		mc.Location{X: cfg.PenAreaMinX, Y: cfg.PenAreaY, Z: cfg.PenAreaMinZ},
		mc.Location{X: cfg.PenAreaMaxX, Y: cfg.PenAreaY, Z: cfg.PenAreaMaxZ},
	)

	// восстанавливаем маппинг из мира
	if err := syncer.Bootstrap(ctx); err != nil {
		log.Fatalf("bootstrap failed: %v", err)
	}

	syncer.Run(ctx)
}
