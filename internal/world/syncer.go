package world

import (
	"fmt"
	"math"
	"sort"

	"github.com/inview-club/shardovod/internal/infrastructure/minecraft"
	"github.com/inview-club/shardovod/internal/infrastructure/ops"
	log "github.com/sirupsen/logrus"
)

// ---------- Layout constants -------------------------------------------------

const (
	hexRadius  = 8  // circumradius of each zone hex in blocks
	hexSpacing = 22 // X distance between hex centres (2*R + 6 gap)
)

// ---------- Pos --------------------------------------------------------------

type Pos struct {
	X, Y, Z int
}

func (p Pos) String() string { return fmt.Sprintf("%d %d %d", p.X, p.Y, p.Z) }

// ---------- Shard entry ------------------------------------------------------

type shardEntry struct {
	key     string
	zone    string
	state   string
	primary bool
	docs    int64
}

// ---------- Zone data --------------------------------------------------------

type zoneData struct {
	center      Pos
	floorBlocks [][2]int // sorted, cached for fast disk floor updates
	diskPct     float64  // last applied disk %
}

// ---------- Syncer -----------------------------------------------------------

// Syncer manages hex zone pens and reconciles shard state with the Minecraft world.
// Not thread-safe — call from one goroutine.
type Syncer struct {
	cmd    *minecraft.Commander
	origin Pos // world origin (first hex centre is placed here)

	zones       map[string]*zoneData
	zoneOrder   []string // insertion order (stable column indices)
	activeZones map[string]bool
	nextIdx     int

	shards map[string]*shardEntry
}

func NewSyncer(cmd *minecraft.Commander, origin Pos) *Syncer {
	return &Syncer{
		cmd:         cmd,
		origin:      origin,
		zones:       make(map[string]*zoneData),
		activeZones: make(map[string]bool),
		shards:      make(map[string]*shardEntry),
	}
}

// BuildPen places initial hex pens for the first snapshot's zone list.
func (s *Syncer) BuildPen(zones []string) {
	s.cmd.Say("Shardovod: building pens...")
	for _, z := range zones {
		s.addZone(z)
	}
	s.cmd.Say("Pens ready!")
}

// Sync reconciles the world with a new snapshot. Returns change counts.
func (s *Syncer) Sync(shards []ops.Shard, diskUsage map[string]float64) (created, updated, moved, removed int) {
	newZoneList := BuildZoneList(shards)
	newShardMap := buildShardMap(shards)

	newZoneSet := make(map[string]bool, len(newZoneList))
	for _, z := range newZoneList {
		newZoneSet[z] = true
	}

	// --- Zone structure changes ----------------------------------------------

	for _, zone := range newZoneList {
		if !s.activeZones[zone] {
			if _, known := s.zones[zone]; known {
				s.restoreZone(zone)
			} else {
				s.addZone(zone)
			}
		}
	}

	for zone := range s.activeZones {
		if zone != "UNASSIGNED" && !newZoneSet[zone] {
			s.removeZone(zone)
		}
	}

	// --- Disk floor update ---------------------------------------------------

	for zone, zd := range s.zones {
		if !s.activeZones[zone] {
			continue
		}
		pct, ok := diskUsage[zone]
		if !ok {
			pct = 0
		}
		if pct != zd.diskPct {
			s.paintDiskFloor(zone, pct)
		}
	}

	// --- Shard reconciliation ------------------------------------------------

	for key, shard := range newShardMap {
		zone := shard.Node
		if zone == "" || !s.activeZones[zone] {
			zone = "UNASSIGNED"
		}
		zd := s.zones[zone]
		docs := shard.DocsCount()
		state := shard.State

		existing, exists := s.shards[key]

		if exists {
			if existing.zone != zone {
				// Shard relocated — recolour in-place then fly to the new pen.
				oldZd := s.zones[existing.zone]
				if existing.state != state {
					s.cmd.RecolorSheep(key, minecraft.ColorID(state))
				}
				s.cmd.FlyTo(
					key,
					oldZd.center.X, oldZd.center.Z,
					zd.center.X, zd.center.Z,
					s.origin.Y,
				)
				if existing.docs != docs {
					s.cmd.UpdateSheepName(key, docs)
				}
				existing.zone = zone
				existing.state = state
				existing.docs = docs
				moved++

			} else if existing.state != state {
				// Colour changed, sheep stays in the same pen — patch NBT in-place.
				s.cmd.RecolorSheep(key, minecraft.ColorID(state))
				if existing.docs != docs {
					s.cmd.UpdateSheepName(key, docs)
				}
				existing.state = state
				existing.docs = docs
				updated++

			} else if existing.docs != docs {
				// Only doc count changed — update floating label only.
				s.cmd.UpdateSheepName(key, docs)
				existing.docs = docs
			}

		} else {
			// New shard — summon at hex centre.
			s.cmd.SummonSheep(key, zd.center.X, s.origin.Y+1, zd.center.Z,
				minecraft.ColorID(state), shard.IsPrimary(), docs)
			s.shards[key] = &shardEntry{
				key:     key,
				zone:    zone,
				state:   state,
				primary: shard.IsPrimary(),
				docs:    docs,
			}
			created++
		}
	}

	// Remove shards that vanished from OpenSearch.
	for key, e := range s.shards {
		if _, ok := newShardMap[key]; !ok {
			s.cmd.KillSheep(key)
			delete(s.shards, key)
			_ = e
			removed++
		}
	}

	return
}

// ---------- Zone lifecycle ---------------------------------------------------

func (s *Syncer) addZone(name string) {
	idx := s.nextIdx
	s.nextIdx++

	center := Pos{
		X: s.origin.X + idx*hexSpacing,
		Y: s.origin.Y,
		Z: s.origin.Z,
	}

	floor := hexFloorBlocks(center.X, center.Z, hexRadius)
	zd := &zoneData{center: center, floorBlocks: floor, diskPct: -1}
	s.zones[name] = zd
	s.zoneOrder = append(s.zoneOrder, name)
	s.activeZones[name] = true

	s.buildHex(center, name)
	s.paintDiskFloor(name, 0)

	log.Infof("[world] zone added: %s (col %d)", name, idx)
}

func (s *Syncer) removeZone(name string) {
	zd := s.zones[name]

	// Kill all sheep in this zone and send them flying to UNASSIGNED.
	unassigned := s.zones["UNASSIGNED"]
	for key, e := range s.shards {
		if e.zone == name {
			s.cmd.FlyTo(
				key,
				zd.center.X, zd.center.Z,
				unassigned.center.X, unassigned.center.Z,
				s.origin.Y,
			)
			e.zone = "UNASSIGNED"
		}
	}

	// Gray out the floor to show the node is offline.
	for _, b := range zd.floorBlocks {
		s.cmd.SetBlock(b[0], s.origin.Y, b[1], "cobblestone")
	}
	s.cmd.PlaceSign(zd.center.X, s.origin.Y+1, zd.center.Z-hexRadius-1, "[offline]")
	zd.diskPct = -1

	delete(s.activeZones, name)
	log.Infof("[world] zone removed: %s", name)
}

func (s *Syncer) restoreZone(name string) {
	zd := s.zones[name]
	s.activeZones[name] = true
	s.paintDiskFloor(name, 0)
	s.cmd.PlaceSign(zd.center.X, s.origin.Y+1, zd.center.Z-hexRadius-1, zoneLabel(name))
	log.Infof("[world] zone restored: %s", name)
}

// ---------- Hex construction -------------------------------------------------

// buildHex places the fence ring, a grass floor placeholder, and the zone sign.
func (s *Syncer) buildHex(center Pos, name string) {
	y := center.Y

	// Fence perimeter at circumradius R+1 (just outside the floor).
	for _, b := range hexPerimeterBlocks(center.X, center.Z, hexRadius+1) {
		s.cmd.SetBlock(b[0], y+1, b[1], "oak_fence")
	}

	// Sign above the south edge.
	s.cmd.PlaceSign(center.X, y+1, center.Z-hexRadius-1, zoneLabel(name))
}

// paintDiskFloor recolors the hex floor to show disk usage.
// Red blocks = used space, green blocks = free space.
// Blocks are filled from south (Z-) to north (Z+) so the red band "grows" upward.
func (s *Syncer) paintDiskFloor(zone string, usedPct float64) {
	zd := s.zones[zone]
	blocks := zd.floorBlocks
	n := len(blocks)
	redCount := int(math.Round(float64(n) * usedPct / 100.0))

	for i, b := range blocks {
		block := "green_concrete"
		if i < redCount {
			block = "red_concrete"
		}
		s.cmd.SetBlock(b[0], s.origin.Y, b[1], block)
	}
	zd.diskPct = usedPct
}

// ---------- Hex geometry -----------------------------------------------------

// isInFlatHex returns true if (dx, dz) is inside a flat-top regular hexagon
// with circumradius R centred at the origin.
//
// Constraints for flat-top hex:
//
//	|dx|           ≤ R
//	|dz|           ≤ R·√3/2
//	|dx|·√3/2 + |dz|·½  ≤ R·√3/2
func isInFlatHex(dx, dz, R int) bool {
	adx := math.Abs(float64(dx))
	adz := math.Abs(float64(dz))
	fR := float64(R)
	h := fR * math.Sqrt(3) / 2 // inradius
	return adx <= fR && adz <= h && adx*math.Sqrt(3)/2+adz*0.5 <= h
}

// hexFloorBlocks returns all (x, z) pairs on the floor of a flat-top hex with
// circumradius R, sorted south-to-north then west-to-east (for disk fill order).
func hexFloorBlocks(cx, cz, R int) [][2]int {
	var blocks [][2]int
	for dx := -R; dx <= R; dx++ {
		for dz := -R; dz <= R; dz++ {
			if isInFlatHex(dx, dz, R) {
				blocks = append(blocks, [2]int{cx + dx, cz + dz})
			}
		}
	}
	sort.Slice(blocks, func(i, j int) bool {
		if blocks[i][1] != blocks[j][1] {
			return blocks[i][1] < blocks[j][1] // south first (smaller Z)
		}
		return blocks[i][0] < blocks[j][0]
	})
	return blocks
}

// hexPerimeterBlocks returns all blocks on the boundary of a flat-top hex,
// traced by walking each of the 6 edges with Bresenham's line algorithm.
func hexPerimeterBlocks(cx, cz, R int) [][2]int {
	// 6 vertices of flat-top hex at angles 0°, 60°, ..., 300°
	verts := make([][2]int, 6)
	for i := 0; i < 6; i++ {
		a := math.Pi / 3 * float64(i)
		verts[i] = [2]int{
			cx + int(math.Round(float64(R)*math.Cos(a))),
			cz + int(math.Round(float64(R)*math.Sin(a))),
		}
	}

	seen := make(map[[2]int]bool)
	var out [][2]int
	for i := 0; i < 6; i++ {
		for _, b := range bresenhamXZ(verts[i], verts[(i+1)%6]) {
			if !seen[b] {
				seen[b] = true
				out = append(out, b)
			}
		}
	}
	return out
}

// bresenhamXZ traces a line between two (x,z) points using Bresenham's algorithm.
func bresenhamXZ(a, b [2]int) [][2]int {
	x0, z0 := a[0], a[1]
	x1, z1 := b[0], b[1]

	dx := abs(x1 - x0)
	dz := abs(z1 - z0)
	sx := sign(x1 - x0)
	sz := sign(z1 - z0)
	err := dx - dz

	var points [][2]int
	for {
		points = append(points, [2]int{x0, z0})
		if x0 == x1 && z0 == z1 {
			break
		}
		e2 := 2 * err
		if e2 > -dz {
			err -= dz
			x0 += sx
		}
		if e2 < dx {
			err += dx
			z0 += sz
		}
	}
	return points
}

// ---------- Shard map --------------------------------------------------------

func buildShardMap(shards []ops.Shard) map[string]ops.Shard {
	counter := make(map[string]int)
	result := make(map[string]ops.Shard)
	for _, s := range shards {
		base := s.Index + ":" + s.Shard + ":" + s.Prirep
		n := counter[base]
		counter[base]++
		result[fmt.Sprintf("%s:%d", base, n)] = s
	}
	return result
}

// ---------- Tiny helpers -----------------------------------------------------

func zoneLabel(name string) string {
	if len(name) > 15 {
		return name[:15]
	}
	return name
}

func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}

func sign(x int) int {
	if x > 0 {
		return 1
	}
	if x < 0 {
		return -1
	}
	return 0
}
