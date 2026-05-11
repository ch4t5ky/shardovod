package minecraft

type PenStatus string

const (
	PenStatusActive  PenStatus = "active"
	PenStatusOffline PenStatus = "offline"
)

const (
	PenWidth  = 10
	PenDepth  = 7
	PenGap    = 1
	PenStride = PenDepth + PenGap // 8 — шаг между загонами по Z
)

type Location struct {
	X, Y, Z int
}

type Bounds struct {
	Min, Max Location
}

func (b Bounds) Contains(loc Location) bool {
	return loc.X >= b.Min.X && loc.X <= b.Max.X &&
		loc.Y >= b.Min.Y && loc.Y <= b.Max.Y &&
		loc.Z >= b.Min.Z && loc.Z <= b.Max.Z
}

type Pen struct {
	PenID    string
	Name     string
	Bounds   Bounds
	Status   PenStatus
	SheepIDs []string
}

func NewPen(penID, name string, bounds Bounds) *Pen {
	return &Pen{
		PenID:    penID,
		Name:     name,
		Bounds:   bounds,
		Status:   PenStatusActive,
		SheepIDs: []string{},
	}
}

// NewPenAt создаёт загон по базовой точке и порядковому номеру вдоль оси Z
func NewPenAt(penID, name string, base Location, index int) *Pen {
	minZ := base.Z + index*PenStride
	return NewPen(penID, name, Bounds{
		Min: Location{X: base.X, Y: base.Y, Z: minZ},
		Max: Location{X: base.X + PenWidth - 1, Y: base.Y, Z: minZ + PenDepth - 1},
	})
}

// SpawnLocation — центр по X, середина по Z
func (p *Pen) SpawnLocation() Location {
	return Location{
		X: (p.Bounds.Min.X + p.Bounds.Max.X) / 2,
		Y: p.Bounds.Min.Y,
		Z: (p.Bounds.Min.Z + p.Bounds.Max.Z) / 2,
	}
}

func (p *Pen) AddSheep(sheepID string) {
	p.SheepIDs = append(p.SheepIDs, sheepID)
}

func (p *Pen) RemoveSheep(sheepID string) {
	filtered := p.SheepIDs[:0]
	for _, id := range p.SheepIDs {
		if id != sheepID {
			filtered = append(filtered, id)
		}
	}
	p.SheepIDs = filtered
}
