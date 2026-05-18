package minecraft

type Hologram struct {
	Name     string
	Location Location
}

func NewHologram(name string, loc Location) *Hologram {
	return &Hologram{
		Name:     name,
		Location: loc,
	}
}
