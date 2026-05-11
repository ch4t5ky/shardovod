package minecraft

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ch4t5ky/shardovod/internal/domain/minecraft"
	"github.com/gorcon/rcon"
	log "github.com/sirupsen/logrus"
)

const (
	commandsPerSecond = 40
	queueSize         = 8192
	dialTimeout       = 5 * time.Second
	redialDelay       = 3 * time.Second
)

type Commander struct {
	addr     string
	password string
	mu       sync.Mutex
	conn     *rcon.Conn
	queue    chan string
	done     chan struct{}
	wg       sync.WaitGroup
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

// Close drains the queue, then shuts down the dispatcher and connection.
func (c *Commander) Close() {
	c.wg.Wait() // block until all enqueued commands are executed
	close(c.done)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
}

func (c *Commander) dispatch() {
	ticker := time.NewTicker(time.Second / commandsPerSecond)
	defer ticker.Stop()
	for {
		select {
		case <-c.done:
			return
		case <-ticker.C: // wait for rate-limit tick FIRST
			select {
			case cmd := <-c.queue:
				c.execWithRetry(cmd)
			default:
				// nothing queued this tick — continue
			}
		}
	}
}

func (c *Commander) execWithRetry(cmd string) {
	defer c.wg.Done()
	for {
		c.mu.Lock()
		if c.conn == nil {
			if err := c.connect(); err != nil {
				c.mu.Unlock()
				log.Errorf("[mc] connect error: %v, retrying in %s", err, redialDelay)
				time.Sleep(redialDelay)
				continue
			}
		}
		_, err := c.conn.Execute(cmd)
		c.mu.Unlock()
		if err != nil {
			log.Errorf("[mc] execute error: %v, reconnecting", err)
			c.mu.Lock()
			c.conn.Close()
			c.conn = nil
			c.mu.Unlock()
			continue
		}
		return
	}
}

func (c *Commander) executeSync(cmd string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for {
		if c.conn == nil {
			if err := c.connect(); err != nil {
				log.Errorf("[mc] connect error: %v, retrying in %s", err, redialDelay)
				c.mu.Unlock()
				time.Sleep(redialDelay)
				c.mu.Lock()
				continue
			}
		}
		resp, err := c.conn.Execute(cmd)
		if err != nil {
			log.Errorf("[mc] execute error: %v, reconnecting", err)
			c.conn.Close()
			c.conn = nil
			continue
		}
		return resp, nil
	}
}

func (c *Commander) connect() error {
	conn, err := rcon.Dial(c.addr, c.password, rcon.SetDialTimeout(dialTimeout))
	if err != nil {
		return fmt.Errorf("failed to connect to %s: %w", c.addr, err)
	}
	c.conn = conn
	log.Infof("[mc] rcon connected to %s", c.addr)
	return nil
}

func (c *Commander) send(cmd string) {
	c.wg.Add(1) // register before enqueue
	select {
	case c.queue <- cmd:
	default:
		c.wg.Done() // dropped — release immediately
		log.Warn("[mc] queue full, dropping command")
	}
}

func (c *Commander) SetBlock(x, y, z int, block string) {
	c.send(fmt.Sprintf("setblock %d %d %d minecraft:%s", x, y, z, block))
}

func (c *Commander) Say(msg string) {
	c.send(fmt.Sprintf("say %s", msg))
}

func (c *Commander) SummonSheep(key string, x, y, z, colorID int, name string) {
	age := 0

	nbt := fmt.Sprintf(
		`{CustomName:'{"text":"%s"}',CustomNameVisible:1b,Invulnerable:1b,Color:%db,Age:%d,Tags:["shardovod","%s"]}`,
		name, colorID, age, CleanTag(key),
	)
	c.send(fmt.Sprintf("summon minecraft:sheep %d %d %d %s", x, y, z, nbt))
}

func CleanTag(key string) string {
	r := strings.NewReplacer(":", "_", " ", "_", ".", "_", "/", "_")
	safe := r.Replace(key)
	if len(safe) > 48 {
		safe = safe[:48]
	}
	return "sv_" + safe
}

// SpawnSheep implements [application.minecraftCommander].
func (c *Commander) SpawnSheep(ctx context.Context, sheep *minecraft.Sheep) {
	age := 0

	nbt := fmt.Sprintf(
		`{CustomName:'{"text":"%s"}',CustomNameVisible:1b,Invulnerable:1b,Color:%db,Age:%d,Tags:["shardovod","%s"]}`,
		sheep.SheepID, sheep.Color, age, CleanTag(sheep.SheepID),
	)
	c.send(fmt.Sprintf("summon minecraft:sheep %d %d %d %s", sheep.Position.X, sheep.Position.Y, sheep.Position.Z, nbt))
}

func (c *Commander) KillSheep(ctx context.Context, sheepID string) {
	c.send(fmt.Sprintf(
		"kill @e[type=minecraft:sheep,tag=%s,tag=shardovod]",
		CleanTag(sheepID),
	))
}

func (c *Commander) UpdateSheepColor(ctx context.Context, sheepID string, color minecraft.Color) {
	c.send(fmt.Sprintf(
		"data merge entity @e[type=minecraft:sheep,tag=%s,tag=shardovod,limit=1] {Color:%db}",
		CleanTag(sheepID), int(color),
	))
}

func (c *Commander) SheepExistsByName(ctx context.Context, name string) (bool, error) {
	resp, err := c.executeSync(
		fmt.Sprintf(`execute if entity @e[type=minecraft:sheep,name="%s"]`, name),
	)
	if err != nil {
		return false, err
	}
	return strings.Contains(resp, "Test passed"), nil
}

func (c *Commander) KillAllSheeps(ctx context.Context) {
	c.send("kill @e[type=minecraft:sheep,tag=shardovod]")
}

func (c *Commander) BuildPen(ctx context.Context, pen *minecraft.Pen) {
	min := pen.Bounds.Min
	max := pen.Bounds.Max

	centerZ := (min.Z + max.Z) / 2

	// передняя и задняя стенки (по X, длина PenWidth=10)
	for x := min.X; x <= max.X; x++ {
		c.send(fmt.Sprintf("setblock %d %d %d minecraft:oak_fence", x, min.Y, min.Z))
		c.send(fmt.Sprintf("setblock %d %d %d minecraft:oak_fence", x, min.Y, max.Z))
	}

	// боковые стенки (по Z, длина PenDepth=7) — в центре пропуск под знак
	for z := min.Z + 1; z < max.Z; z++ {
		c.send(fmt.Sprintf("setblock %d %d %d minecraft:oak_fence", min.X, min.Y, z))
		c.send(fmt.Sprintf("setblock %d %d %d minecraft:oak_fence", max.X, min.Y, z))
	}

	// знак на левой боковой стенке (min.X), смотрит наружу (на запад, rotation=12)
	c.send(fmt.Sprintf(
		"setblock %d %d %d minecraft:oak_sign[rotation=4]",
		min.X-1, min.Y, centerZ,
	))
	c.send(fmt.Sprintf(
		`data merge block %d %d %d {Text1:'{"text":"%s"}'}`,
		min.X-1, min.Y, centerZ, pen.Name,
	))
}

func (c *Commander) SetPenOffline(ctx context.Context, pen *minecraft.Pen) {
	min := pen.Bounds.Min
	max := pen.Bounds.Max

	// периметр → красное стекло
	for x := min.X; x <= max.X; x++ {
		c.send(fmt.Sprintf("setblock %d %d %d minecraft:red_stained_glass", x, min.Y, min.Z))
		c.send(fmt.Sprintf("setblock %d %d %d minecraft:red_stained_glass", x, min.Y, max.Z))
	}
	for z := min.Z + 1; z < max.Z; z++ {
		c.send(fmt.Sprintf("setblock %d %d %d minecraft:red_stained_glass", min.X, min.Y, z))
		c.send(fmt.Sprintf("setblock %d %d %d minecraft:red_stained_glass", max.X, min.Y, z))
	}

	centerZ := (min.Z + max.Z) / 2
	// знак на левой боковой стенке (min.X), смотрит наружу (на запад, rotation=12)
	c.send(fmt.Sprintf(
		"setblock %d %d %d minecraft:oak_sign[rotation=4]",
		min.X-1, min.Y, centerZ,
	))
	c.send(fmt.Sprintf(
		`data merge block %d %d %d {Text1:'{"text":"%s"}'}`,
		min.X-1, min.Y, centerZ, "offline",
	))
}

func (c *Commander) GetSheepLocation(sheepID string) (minecraft.Location, error) {
	tag := CleanTag(sheepID)
	sel := fmt.Sprintf("@e[tag=%s,limit=1]", tag)

	resp, err := c.executeSync(fmt.Sprintf("data get entity %s Pos", sel))
	if err != nil {
		return minecraft.Location{}, err
	}

	// resp: "... has the following entity data: [Xd, Yd, Zd]"
	var x, y, z float64
	_, err = fmt.Sscanf(
		resp[strings.Index(resp, "[")+1:strings.Index(resp, "]")],
		"%fd, %fd, %fd", &x, &y, &z,
	)
	if err != nil {
		return minecraft.Location{}, fmt.Errorf("parse pos: %w", err)
	}
	return minecraft.Location{X: int(x), Y: int(y), Z: int(z)}, nil
}

func (c *Commander) MoveSheep(ctx context.Context, sheepID string, to minecraft.Location) {
	tag := CleanTag(sheepID)
	sel := fmt.Sprintf("@e[tag=%s,limit=1]", tag)

	from, err := c.GetSheepLocation(sheepID)
	if err != nil {
		log.Errorf("[mc] can't get sheep position: %v", err)
		return
	}
	flyY := from.Y + 4

	wait := func(d time.Duration) bool {
		select {
		case <-time.After(d):
			return true
		case <-ctx.Done():
			return false
		}
	}

	go func() {
		const (
			stepDelay = 200 * time.Millisecond
			steps     = 10
		)

		// Фаза 1: плавно вверх
		for i := 1; i <= steps; i++ {
			y := from.Y + (flyY-from.Y)*i/steps
			c.send(fmt.Sprintf("tp %s %d %d %d", sel, from.X, y, from.Z))
			if !wait(stepDelay) {
				return
			}
		}

		// Фаза 2: плавно горизонтально
		for i := 1; i <= steps; i++ {
			x := from.X + (to.X-from.X)*i/steps
			z := from.Z + (to.Z-from.Z)*i/steps
			c.send(fmt.Sprintf("tp %s %d %d %d", sel, x, flyY, z))
			if !wait(stepDelay) {
				return
			}
		}

		// Фаза 3: плавно вниз
		for i := 1; i <= steps; i++ {
			y := flyY + (to.Y-flyY)*i/steps
			c.send(fmt.Sprintf("tp %s %d %d %d", sel, to.X, y, to.Z))
			if !wait(stepDelay) {
				return
			}
		}
	}()
}

func (c *Commander) DestroyAllPens(ctx context.Context, bounds minecraft.Bounds) {
	min := bounds.Min
	max := bounds.Max
	c.send(fmt.Sprintf("fill %d %d %d %d %d %d air",
		min.X, min.Y, min.Z,
		max.X, min.Y, max.Z,
	))
}
