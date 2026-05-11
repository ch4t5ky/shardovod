package cluster

import (
	"context"
	"fmt"
	"time"

	"github.com/inview-club/shardovod/internal/infrastructure/ops"
	log "github.com/sirupsen/logrus"
)

// Snapshot is an immutable point-in-time view of the cluster state.
type Snapshot struct {
	Shards    []ops.Shard
	DiskUsage map[string]float64 // node name → used disk %
}

// Watcher polls OpenSearch on a fixed interval and pushes a new Snapshot to
// Updates whenever shards or disk usage change.
type Watcher struct {
	client   *ops.Client
	interval time.Duration
	Updates  chan Snapshot
	stop     chan struct{}

	prevShards map[string]ops.Shard
	prevDisk   map[string]float64
}

func NewWatcher(client *ops.Client, interval time.Duration) *Watcher {
	return &Watcher{
		client:     client,
		interval:   interval,
		Updates:    make(chan Snapshot, 1),
		stop:       make(chan struct{}),
		prevShards: make(map[string]ops.Shard),
		prevDisk:   make(map[string]float64),
	}
}

func (w *Watcher) Start() {
	go w.run()
	log.Infof("[cluster] watcher started, interval=%s", w.interval)
}

func (w *Watcher) Stop() { close(w.stop) }

func (w *Watcher) run() {
	w.poll()
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		select {
		case <-w.stop:
			return
		case <-ticker.C:
			w.poll()
		}
	}
}

func (w *Watcher) poll() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	shards, err := w.client.GetShards(ctx)
	if err != nil {
		log.Errorf("[cluster] poll shards error: %v", err)
		return
	}

	disk, err := w.client.GetNodeDiskUsage(ctx)
	if err != nil {
		log.Warnf("[cluster] poll disk error: %v — keeping previous values", err)
		disk = w.prevDisk // non-fatal: keep last known values
	}

	currentShards := indexShards(shards)
	if shardsEqual(w.prevShards, currentShards) && diskEqual(w.prevDisk, disk) {
		return
	}

	log.Infof("[cluster] state changed — %d shards", len(shards))
	select {
	case <-w.Updates: // drain stale
	default:
	}
	w.Updates <- Snapshot{Shards: shards, DiskUsage: disk}
	w.prevShards = currentShards
	w.prevDisk = disk
}

// ---------- diff helpers -----------------------------------------------------

func indexShards(shards []ops.Shard) map[string]ops.Shard {
	m := make(map[string]ops.Shard)
	counter := make(map[string]int)
	for _, s := range shards {
		base := s.Index + ":" + s.Shard + ":" + s.Prirep
		n := counter[base]
		counter[base]++
		m[fmt.Sprintf("%s:%d", base, n)] = s
	}
	return m
}

func shardsEqual(a, b map[string]ops.Shard) bool {
	if len(a) != len(b) {
		return false
	}
	for k, va := range a {
		vb, ok := b[k]
		if !ok || va.State != vb.State || va.Node != vb.Node || va.Docs != vb.Docs {
			return false
		}
	}
	return true
}

func diskEqual(a, b map[string]float64) bool {
	if len(a) != len(b) {
		return false
	}
	for k, va := range a {
		if vb, ok := b[k]; !ok || va != vb {
			return false
		}
	}
	return true
}
