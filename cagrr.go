package cagrr

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

const (
	numFields = 8
)

type int64arr []int64

func (a int64arr) Len() int           { return len(a) }
func (a int64arr) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a int64arr) Less(i, j int) bool { return a[i] < a[j] }

// Ring represents several node combined in ring
type Ring struct {
	Host   string
	Port   int
	Tokens []Token `json:"tokens"`
}

// Token represents primary key range
type Token struct {
	ID     string     `json:"key"`
	Ranges []Fragment `json:"ranges"`
}

// Fragment of Token range for repair
type Fragment struct {
	ID    int
	Start string `json:"start"`
	End   string `json:"end"`
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
	ID       int64  `json:"id"`
	Message  string `json:"message"`
	Error    bool   `json:"error"`
	Type     string `json:"type"`
	Count    int    `json:"count"`
	Total    int    `json:"total"`
	Session  string `json:"session"`
	Command  int    `json:"command"`
	Start    string `json:"start"`
	Finish   string `json:"finish"`
	Keyspace string `json:"keyspace"`
	Options  string `json:"options"`
	Duration int    `json:"duration"`
}

// Get the Ring
func (r Ring) Get(slices int) (Ring, error) {
	url := fmt.Sprintf("http://%s:%d/ring/%d", r.Host, r.Port, slices)
	res, _ := http.Get(url)
	response, _ := ioutil.ReadAll(res.Body)
	err := json.Unmarshal(response, &r)
	return r, err
}

// Repair ring
func (r Ring) Repair(keyspace string, from int, cause string, owner string, callback string) ([]Repair, []error) {
	count := r.Count()
	repairs := make([]Repair, count)
	errors := make([]error, count)
	for _, token := range r.Tokens {
		for _, frag := range token.Ranges {
			if frag.ID > from {
				repair, err := frag.Repair(r, keyspace, cause, owner, callback)
				repairs = append(repairs, repair)
				errors = append(errors, err)
			}
		}
	}
	return repairs, errors
}

// Count fragments of ring
func (r Ring) Count() int {
	count := 0
	for _, token := range r.Tokens {
		for range token.Ranges {
			count++
		}
	}
	return count
}

// Repair fragment
func (f Fragment) Repair(ring Ring, keyspace string, cause string, owner string, callback string) (Repair, error) {
	options := &Repair{
		Keyspace: keyspace,
		Cause:    cause,
		Owner:    owner,
		Callback: callback,
		Options: map[string]string{
			"parallelism": "parallel",
			"ranges":      fmt.Sprintf("%s:%s", f.Start, f.End),
		},
	}

	url := fmt.Sprintf("http://%s:%d/repair", ring.Host, ring.Port)

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
