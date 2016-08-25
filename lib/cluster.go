package cluster

import (
	"fmt"
	"os"
	"os/exec"
	"syscall"
)

const (
	numTokens = 256
)

// Node represents a node in cluster
type Node struct {
	Host string
	Port int
}

// Ring represents several node combined in ring
type Ring struct {
	Nodes []Node
}

// Token represents primary key range
type Token struct {
	Node *Node
	ID   int64
	Next *Token
}

// Fragment of Token range for repair
type Fragment struct {
	Token  *Token
	Start  int64
	Finish int64
}

// Tokens initializes array of node tokens
func (n Node) Tokens() []Token {
	result := make([]Token, numTokens)
	return result
}

// Fragments is a set of ranges in Token
func (t Token) Fragments(steps int) []Fragment {
	result := make([]Fragment, steps)
	return result
}

// Repair fragment of node range
func (f Fragment) Repair(keyspace string) {
	binary, lookErr := exec.LookPath("nodetool")
	if lookErr != nil {
		panic(lookErr)
	}
	node := f.Token.Node
	env := os.Environ()
	args := []string{
		"nodetool",
		"-h", node.Host,
		"-p", fmt.Sprintf("%d", node.Port),
		"repair", keyspace,
		"-pr",
		"-st", fmt.Sprintf("%d", f.Start),
		"-et", fmt.Sprintf("%d", f.Finish)}
	execErr := syscall.Exec(binary, args, env)
	if execErr != nil {
		panic(execErr)
	}
}
