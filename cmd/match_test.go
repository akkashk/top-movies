package cmd

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_matching(t *testing.T) {
	mdFeatures := make(moviesMetadataFeatures)
	mdFeatures["0"] = &movieMetadataFeatures{
		title: "film title",
	}

	out := make(chan string, 1)
	var wg sync.WaitGroup
	wg.Add(1)
	mdFeatures.mostRelevant(
		&wikiEntry{
			title: "film title",
		},
		out,
		&wg,
	)
	id := <-out
	require.Equal(t, "0", id)
}
