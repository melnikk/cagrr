package cluster

// Node represents a node in cluster
type Node struct {
	Host string
	Port int
}

// Ring represents several node combined in ring
type Ring struct {
	nodes []Node
}

// Token represents primary key range
type Token struct {
	id int64
}

// Fragment of Token range for repair
type Fragment struct {
	Start  int64
	Finish int64
}

// Tokens initializes array of node tokens
func (n Node) Tokens() []Token {
	result := make([]Token, 256)
	return result
}

// Ranges is a set of ranges in Token
func (t Token) Ranges(steps int) []Fragment {
	result := make([]Fragment, steps)
	return result
}

// Repair fragment of node range
func (f Fragment) Repair() {
}
