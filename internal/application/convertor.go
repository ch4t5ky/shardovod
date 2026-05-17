package application

import (
	minecraft "github.com/ch4t5ky/shardovod/internal/domain/minecraft"
	opensearch "github.com/ch4t5ky/shardovod/internal/domain/opensearch"
)

func SheepColor(state opensearch.ShardState, isPrimary bool) minecraft.Color {
	switch state {
	case opensearch.ShardStateStarted:
		if isPrimary {
			return minecraft.ColorWhite
		}
		return minecraft.ColorLime
	case opensearch.ShardStateInitializing:
		return minecraft.ColorOrange
	case opensearch.ShardStateRelocating:
		return minecraft.ColorYellow
	case opensearch.ShardStateUnassigned:
		return minecraft.ColorBlack
	default:
		return minecraft.ColorRed
	}
}

func healthColor(h opensearch.IndexHealth) string {
	switch h {
	case opensearch.IndexHealthGreen:
		return "&#00FF00"
	case opensearch.IndexHealthYellow:
		return "&#FFAA00"
	case opensearch.IndexHealthRed:
		return "&#FF3333"
	default:
		return "&#AAAAAA"
	}
}
