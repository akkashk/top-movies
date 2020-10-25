package cmd

type trieNode struct {
	children map[rune]*trieNode
	values   []string
}

func newTrie() *trieNode {
	return &trieNode{
		children: make(map[rune]*trieNode),
		values:   make([]string, 0),
	}
}

func (t *trieNode) put(key []rune, val string) {
	currentNode := t
	for _, k := range key {
		if currentNode.children[k] == nil {
			currentNode.children[k] = newTrie()
		}
		currentNode = currentNode.children[k]

	}
	currentNode.values = append(currentNode.values, val)
}

func (t *trieNode) walk(key []rune) []string {
	currentNode := t
	ids := []string{}
	for _, k := range key {
		child := currentNode.children[k]
		if child == nil {
			return ids
		}

		ids = append(ids, child.values...)
		currentNode = child
	}

	return ids
}
