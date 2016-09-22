package cagrr

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/big"
	"net/http"
	"os/exec"

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
	Host string
}

// Fragment of Token range for repair
type Fragment struct {
	ID     int
	Token  *Token
	Start  int64
	Finish int64
}

// Repair is a Unit of repair work
type Repair struct {
	ID       int64             `json:"id"`
	Keyspace string            `json:"keyspace"`
	Cause    string            `json:"cause"`
	Owner    string            `json:"owner"`
	Options  map[string]string `json:"options"`
	Callback string            `json:"callback"`
	Message  string            `json:"message"`
}

// RepairStatus keeps status of repair
type RepairStatus struct {
	ID      int64  `json:"id"`
	Message string `json:"message"`
	Error   bool   `json:"error"`
	Type    string `json:"type"`
	Count   int    `json:"count"`
	Total   int    `json:"total"`
}

func (n Node) Ranges(slices int) ([]string, error) {
	url := fmt.Sprintf("http://%s:%d/ring/%d", n.Host, n.Port, slices)
	r, _ := http.Get(url)
	response, _ := ioutil.ReadAll(r.Body)
	var ranges []string

	err := json.Unmarshal(response, &ranges)
	return ranges, err
}

// Tokens initializes array of node tokens
func (n Node) Tokens() (map[int64]Token, []int64) {
	url := fmt.Sprintf("http://%s:%d/ring", n.Host, n.Port)
	r, _ := http.Get(url)
	response, _ := ioutil.ReadAll(r.Body)
	var ring map[int64]string
	var result = make(map[int64]Token)

	err := json.Unmarshal(response, &ring)
	var prev int64
	var keys []int64
	if err != nil {
		for i, host := range ring {
			keys = append(keys, i)
			next := i
			if prev == 0 {
				prev = i
				continue
			}
			token := Token{Node: &n, ID: prev, Next: next, Host: host}

			result[prev] = token
			prev = next
		}
	}
	return result, keys
}

// Fragments is a set of ranges in Token
func (t Token) Fragments(steps int) []Fragment {
	var min *big.Int
	var max *big.Int
	var size *big.Int
	one := big.NewInt(1)
	two := big.NewInt(2)
	power := big.NewInt(63)

	min = min.Exp(two, power, nil)
	min = min.Neg(min)

	max = max.Exp(two, power, nil)
	max = max.Sub(max, one)

	size = size.Sub(max, min)
	size = size.Add(size, one)
	//BigInteger RANGE_MIN = new BigInteger("2").pow(63).negate();
	//RANGE_MAX = new BigInteger("2").pow(63).subtract(BigInteger.ONE);
	//RANGE_SIZE = RANGE_MAX.subtract(RANGE_MIN).add(BigInteger.ONE);

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
		frag := Fragment{Token: &t, Start: i, Finish: finish - 1}
		result = append(result, frag)
	}
	return result
}

// Repair fragment of node range
func (f Fragment) Repair(keyspace string, cause string, owner string, callback string) (Repair, error) {

	options := &Repair{
		Keyspace: keyspace,
		Cause:    cause,
		Owner:    owner,
		Callback: callback,
		Options: map[string]string{
			"parallelism": "parallel",
			"ranges":      fmt.Sprintf("%d:%d", f.Start, f.Finish-1),
		},
	}

	url := fmt.Sprintf("http://%s:%d/repair", f.Token.Node.Host, f.Token.Node.Port)

	buf, _ := json.Marshal(options)
	body := bytes.NewBuffer(buf)
	r, err := http.Post(url, "application/json", body)
	var repair Repair
	if err == nil {
		response, _ := ioutil.ReadAll(r.Body)
		err = json.Unmarshal(response, &repair)
	}

	return repair, err
}

func nodetool(args []string) (string, error) {
	out, err := exec.Command("nodetool", s.Join(args, " ")).Output()
	return string(out), err
}
