package cluster

import (
	"fmt"
	"log"
	"os/exec"
	"strconv"

	s "strings"
)

const (
	numFields = 8
	numTokens = 256
)

var binary string

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
	Next int64
}

// Fragment of Token range for repair
type Fragment struct {
	Token  *Token
	Start  int64
	Finish int64
}

// Tokens initializes array of node tokens
func (n Node) Tokens() []Token {
	var result []Token
	args := []string{
		"-h", n.Host,
		"-p", fmt.Sprintf("%d", n.Port),
		"ring"}
	res, err := nodetool(args)
	if err != nil {
		panic(err)
	}
	lines := s.Split(res, "\n")
	var prev int64
	for _, str := range lines {
		fields := s.Fields(str)
		if len(fields) == numFields {
			i, _ := strconv.ParseInt(fields[7], 10, 64)
			next := int64(i)
			if prev == 0 {
				prev = i
				continue
			}
			token := Token{Node: &n, ID: prev, Next: next}

			result = append(result, token)
			prev = next
		}
	}

	return result
}

// Fragments is a set of ranges in Token
func (t Token) Fragments(steps int) []Fragment {
	var result []Fragment
	step := (t.Next - t.ID) / int64(steps)
	for i := t.ID; i < t.Next; i += step {
		frag := Fragment{Token: &t, Start: i, Finish: i + step}
		result = append(result, frag)
	}
	return result
}

// Repair fragment of node range
func (f Fragment) Repair(keyspace string) (string, error) {
	node := f.Token.Node
	args := []string{
		"-h", node.Host,
		"-p", fmt.Sprintf("%d", node.Port),
		"repair", keyspace,
		"-st", fmt.Sprintf("%d", f.Start),
		"-et", fmt.Sprintf("%d", f.Finish)}

	return nodetool(args)
}

func nodetool(args []string) (string, error) {
	out, err := exec.Command("nodetool", s.Join(args, " ")).Output()
	if err != nil {
		fmt.Println("Alarma:", string(out))
		log.Fatal(err)
	}
	return string(out), err
}
