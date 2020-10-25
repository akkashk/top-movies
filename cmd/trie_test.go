package cmd

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_trie(t *testing.T) {
	trie := newTrie()
	trie.put([]rune("film"), "0")

	ids := trie.walk([]rune("films"))
	require.Contains(t, ids, "0")
}
