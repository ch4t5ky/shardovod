package minecraft

type Hologram struct {
	ID       string
	Location Location
}

func NewHologram(id string, loc Location) *Hologram {
	return &Hologram{
		ID:       id,
		Location: loc,
	}
}
