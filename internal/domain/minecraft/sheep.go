package minecraft

type Sheep struct {
	SheepID  string
	PenID    string
	Name     string
	Position Location
	Color    Color
}

func NewSheep(sheepID, penID, name string, pos Location) *Sheep {
	return &Sheep{
		SheepID:  sheepID,
		PenID:    penID,
		Name:     name,
		Position: pos,
		Color:    ColorBlack, // unassigned по умолчанию
	}
}
