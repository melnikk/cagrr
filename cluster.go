package cagrr

import (
	"fmt"
	"os/exec"
	"strconv"

	s "strings"
)

const (
	numFields = 8
)

type int64arr []int64

func (a int64arr) Len() int           { return len(a) }
func (a int64arr) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a int64arr) Less(i, j int) bool { return a[i] < a[j] }

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
	ID     int
	Token  *Token
	Start  int64
	Finish int64
}

// RepairResult keeps information about Fragment repair status
type RepairResult struct {
	Frag    *Fragment
	Message string
	Error   error
}

// Tokens initializes array of node tokens
func (n Node) Tokens() (map[int64]Token, []int64) {
	result := make(map[int64]Token)
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
	var keys []int64
	for _, str := range lines {
		fields := s.Fields(str)
		if len(fields) == numFields {
			i, err := strconv.ParseInt(fields[7], 10, 64)
			if err != nil {
				continue
			}
			keys = append(keys, i)
			next := i
			if prev == 0 {
				prev = i
				continue
			}
			token := Token{Node: &n, ID: prev, Next: next}

			result[prev] = token
			prev = next
		}
	}
	return result, keys
}

// Fragments is a set of ranges in Token
func (t Token) Fragments(steps int) []Fragment {
	var result []Fragment
	step := (t.Next - t.ID) / int64(steps)
	for i := t.ID; i < t.Next; i += step {
		var finish int64
		switch {
		case i+step > t.Next:
			finish = t.Next
		default:
			finish = i + step
		}
		frag := Fragment{Token: &t, Start: i, Finish: finish}
		result = append(result, frag)
	}
	return result
}

// Repair fragment of node range
func (f Fragment) Repair(keyspace string) (RepairResult, error) {
	node := f.Token.Node
	args := []string{
		"-h", node.Host,
		"-p", fmt.Sprintf("%d", node.Port),
		"repair", keyspace,
		"-st", fmt.Sprintf("%d", f.Start),
		"-et", fmt.Sprintf("%d", f.Finish)}

	str, err := nodetool(args)

	result := RepairResult{
		Frag:    &f,
		Message: str,
		Error:   err,
	}
	return result, err
}

func nodetool(args []string) (string, error) {
	out, err := exec.Command("nodetool", s.Join(args, " ")).Output()
	return string(out), err
}
