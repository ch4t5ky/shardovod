package opensearch

import "strings"

type IndexHealth string

const (
	IndexHealthGreen  IndexHealth = "green"
	IndexHealthYellow IndexHealth = "yellow"
	IndexHealthRed    IndexHealth = "red"
)

type Index struct {
	Id        string
	Name      string
	Health    IndexHealth
	Size      string
	DocsCount int
}

func NewIndex(id, name string, health IndexHealth, docsCount int, size string) *Index {
	return &Index{
		Id:        id,
		Name:      name,
		Health:    health,
		DocsCount: docsCount,
		Size:      size,
	}
}

func (i *Index) IsSystem() bool {
	return len(i.Name) > 0 &&
		i.Name[0] == '.' &&
		!strings.HasPrefix(i.Name, ".ds-")
}
