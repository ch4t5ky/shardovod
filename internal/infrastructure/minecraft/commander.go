package minecraft

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/gorcon/rcon"
	log "github.com/sirupsen/logrus"
)

const (
	commandsPerSecond = 40
	queueSize         = 8192
	dialTimeout       = 5 * time.Second
	redialDelay       = 3 * time.Second

	// FlyTo animation
	flySteps     = 14
	flyLift      = 18 // max blocks above ground at arc peak
	flyStepDelay = 120 * time.Millisecond
)

// Commander sends commands to a Java 1.16.5 server over RCON.
// Commands are queued and dispatched at a safe rate.
type Commander struct {
	addr     string
	password string
	conn     *rcon.Conn
	queue    chan string
	done     chan struct{}
}

func NewCommander(addr, password string) *Commander {
	c := &Commander{
		addr:     addr,
		password: password,
		queue:    make(chan string, queueSize),
		done:     make(chan struct{}),
	}
	go c.dispatch()
	return c
}

func (c *Commander) Close() {
	close(c.done)
	if c.conn != nil {
		c.conn.Close()
	}
}

func (c *Commander) dispatch() {
	ticker := time.NewTicker(time.Second / commandsPerSecond)
	defer ticker.Stop()
	for {
		select {
		case <-c.done:
			return
		case cmd := <-c.queue:
			<-ticker.C
			c.execWithRetry(cmd)
		}
	}
}

func (c *Commander) execWithRetry(cmd string) {
	for {
		if c.conn == nil {
			if err := c.connect(); err != nil {
				log.Errorf("[mc] connect error: %v, retrying in %s", err, redialDelay)
				time.Sleep(redialDelay)
				continue
			}
		}
		if _, err := c.conn.Execute(cmd); err != nil {
			log.Errorf("[mc] execute error: %v, reconnecting", err)
			c.conn.Close()
			c.conn = nil
			continue
		}
		return
	}
}

func (c *Commander) connect() error {
	conn, err := rcon.Dial(c.addr, c.password, rcon.SetDialTimeout(dialTimeout))
	if err != nil {
		return err
	}
	c.conn = conn
	log.Infof("[mc] rcon connected to %s", c.addr)
	return nil
}

func (c *Commander) send(cmd string) {
	select {
	case c.queue <- cmd:
	default:
		log.Warn("[mc] queue full, dropping command")
	}
}

// ---------- World commands ---------------------------------------------------

func (c *Commander) SetBlock(x, y, z int, block string) {
	c.send(fmt.Sprintf("setblock %d %d %d minecraft:%s", x, y, z, block))
}

func (c *Commander) Fill(x1, y1, z1, x2, y2, z2 int, block string) {
	c.send(fmt.Sprintf("fill %d %d %d %d %d %d minecraft:%s", x1, y1, z1, x2, y2, z2, block))
}

func (c *Commander) PlaceSign(x, y, z int, text string) {
	c.send(fmt.Sprintf("setblock %d %d %d minecraft:oak_sign", x, y, z))
	c.send(fmt.Sprintf(`data merge block %d %d %d {Text1:'{"text":"%s"}'}`, x, y, z, text))
}

func (c *Commander) Say(msg string) {
	c.send(fmt.Sprintf("say %s", msg))
}

// ---------- Sheep commands ---------------------------------------------------

// SummonSheep spawns a shard sheep.
//   - AI enabled (NoAI removed) — sheep can wander inside the hex pen
//   - Invulnerable — players cannot kill it
//   - Age 0 → adult (primary), Age -24000 → permanent baby (replica)
//   - Tags: ["shardovod", "<shard-tag>"] for targeted selectors
//   - CustomName shows shard info + doc count
func (c *Commander) SummonSheep(key string, x, y, z, colorID int, isPrimary bool, docs int64) {
	age := 0
	if !isPrimary {
		age = -24000
	}
	nbt := fmt.Sprintf(
		`{CustomName:'{"text":"%s"}',CustomNameVisible:1b,Invulnerable:1b,Color:%db,Age:%d,Tags:["shardovod","%s"]}`,
		FormatShardName(key, docs), colorID, age, ShardTag(key),
	)
	c.send(fmt.Sprintf("summon minecraft:sheep %d %d %d %s", x, y, z, nbt))
}

func (c *Commander) KillSheep(key string) {
	c.send(fmt.Sprintf(
		"kill @e[type=minecraft:sheep,tag=%s,tag=shardovod]",
		ShardTag(key),
	))
}

// RecolorSheep changes a sheep's wool colour in-place via NBT merge.
func (c *Commander) RecolorSheep(key string, colorID int) {
	c.send(fmt.Sprintf(
		"data merge entity @e[type=minecraft:sheep,tag=%s,tag=shardovod,limit=1] {Color:%db}",
		ShardTag(key), colorID,
	))
}

// UpdateSheepName refreshes the floating label without killing/resummoning.
func (c *Commander) UpdateSheepName(key string, docs int64) {
	c.send(fmt.Sprintf(
		`data merge entity @e[type=minecraft:sheep,tag=%s,tag=shardovod,limit=1] {CustomName:'{"text":"%s"}'}`,
		ShardTag(key), FormatShardName(key, docs),
	))
}

// FlyTo animates a sheep flying from one position to another along a parabolic
// arc. Runs in a background goroutine so it does not block the sync loop.
//
// The sheep is first snapped to (fromX, groundY+1, fromZ) so the arc origin is
// predictable, then 14 intermediate positions are sent at 120 ms intervals.
func (c *Commander) FlyTo(key string, fromX, fromZ, toX, toZ, groundY int) {
	tag := ShardTag(key)
	go func() {
		// Snap to the start point so arc begins from a known location.
		c.send(fmt.Sprintf(
			"tp @e[type=minecraft:sheep,tag=%s,tag=shardovod] %d %d %d",
			tag, fromX, groundY+1, fromZ,
		))
		time.Sleep(flyStepDelay)

		for i := 1; i <= flySteps; i++ {
			t := float64(i) / float64(flySteps)

			// Linear interpolation in XZ.
			x := fromX + int(math.Round(float64(toX-fromX)*t))
			z := fromZ + int(math.Round(float64(toZ-fromZ)*t))

			// Parabolic arc in Y: sin(t*π) peaks at t=0.5.
			lift := int(math.Round(math.Sin(t*math.Pi) * flyLift))
			y := groundY + 1 + lift

			c.send(fmt.Sprintf(
				"tp @e[type=minecraft:sheep,tag=%s,tag=shardovod] %d %d %d",
				tag, x, y, z,
			))
			time.Sleep(flyStepDelay)
		}
	}()
}

// ---------- Tag & display helpers --------------------------------------------

// ShardTag returns a Minecraft-safe entity tag derived from the shard key.
// Special characters are replaced so the tag works in @e[tag=…] selectors.
func ShardTag(key string) string {
	r := strings.NewReplacer(":", "_", " ", "_", ".", "_", "/", "_")
	safe := r.Replace(key)
	if len(safe) > 48 {
		safe = safe[:48]
	}
	return "sv_" + safe
}

// FormatShardName builds the CustomName shown above the sheep:
// "index/shard/p (1.2k)"
func FormatShardName(key string, docs int64) string {
	parts := strings.Split(key, ":")
	short := key
	if len(parts) >= 3 {
		short = parts[0] + "/" + parts[1] + "/" + parts[2]
	}
	if len(short) > 20 {
		short = short[:20]
	}
	return short + " (" + FormatDocs(docs) + ")"
}

// FormatDocs formats a raw doc count into a short human-readable string.
func FormatDocs(docs int64) string {
	switch {
	case docs >= 1_000_000:
		return fmt.Sprintf("%.1fM", float64(docs)/1_000_000)
	case docs >= 1_000:
		return fmt.Sprintf("%.1fk", float64(docs)/1_000)
	default:
		return fmt.Sprintf("%d", docs)
	}
}

// ---------- Color helpers ----------------------------------------------------

// ColorID maps shard state to Minecraft sheep wool colour NBT value.
func ColorID(state string) int {
	switch state {
	case "STARTED":
		return 5 // lime
	case "INITIALIZING":
		return 1 // orange
	case "RELOCATING":
		return 4 // yellow
	case "UNASSIGNED":
		return 15 // black
	default:
		return 14 // red
	}
}
