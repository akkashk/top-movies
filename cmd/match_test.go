package cmd

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_matching(t *testing.T) {
	mdFeatures := &moviesMetadataFeatures{
		data: map[string]*movieMetadataFeatures{},
		trie: newTrie(),
	}
	mdFeatures.data["0"] = &movieMetadataFeatures{
		title: "film title",
	}
	mdFeatures.trie.put([]rune("film title"), "0")

	ids := mdFeatures.mostRelevant(
		&wikiEntry{
			title: "film title",
		},
	)
	require.Contains(t, ids, "0")
}
