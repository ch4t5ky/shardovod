package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/inview-club/shardovod/internal/cluster"
	"github.com/inview-club/shardovod/internal/config"
	"github.com/inview-club/shardovod/internal/infrastructure/minecraft"
	"github.com/inview-club/shardovod/internal/infrastructure/ops"
	"github.com/inview-club/shardovod/internal/world"
	log "github.com/sirupsen/logrus"
)

func main() {
	cfg, err := config.New()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	osClient, err := ops.New(
		cfg.OpenSearchAddresses,
		cfg.OpenSearchUsername,
		cfg.OpenSearchPassword,
	)
	if err != nil {
		log.Fatalf("opensearch: %v", err)
	}

	mc := minecraft.NewCommander(cfg.RCONAddr, cfg.RCONPassword)
	watcher := cluster.NewWatcher(osClient, cfg.PollInterval)
	syncer := world.NewSyncer(mc, world.Pos{X: cfg.PenX, Y: cfg.PenY, Z: cfg.PenZ})

	defer mc.Close()
	defer watcher.Stop()

	watcher.Start()

	log.Info("waiting for first snapshot from OpenSearch...")

	snap := <-watcher.Updates
	log.Infof("got first snapshot: %d shards", len(snap.Shards))

	zones := world.BuildZoneList(snap.Shards)
	syncer.BuildPen(zones)

	created, _, _, _ := syncer.Sync(snap.Shards, snap.DiskUsage)
	mc.Say("shardovod ready — " + itoa(created) + " shards")

	go func() {
		for snap := range watcher.Updates {
			c, u, m, r := syncer.Sync(snap.Shards, snap.DiskUsage)
			if c+u+m+r > 0 {
				log.Infof("sync: +%d created  ~%d updated  >%d moved  -%d removed", c, u, m, r)
			}
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Info("shutting down")
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	b := [20]byte{}
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
