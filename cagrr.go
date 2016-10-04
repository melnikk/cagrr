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
	Host        string
	Port        int
	Cluster     int     `json:"cluster"`
	Name        string  `json:"name"`
	Partitioner string  `json:"partitioner"`
	Tokens      []Token `json:"tokens"`
}

// Token represents primary key range
type Token struct {
	ID     string     `json:"key"`
	Ranges []Fragment `json:"ranges"`
}

// Fragment of Token range for repair
type Fragment struct {
	ID       int    `json:"id"`
	Endpoint string `json:"endpoint"`
	Start    string `json:"start"`
	End      string `json:"end"`
}

// Repair is a Unit of repair work
type Repair struct {
	ID       int64    `json:"id"`
	Fragment Fragment `json:"fragment"`
	Callback string   `json:"callback"`
	Keyspace string   `json:"keyspace"`
}

// RepairStatus keeps status of repair
type RepairStatus struct {
	Repair   Repair `json:"repair"`
	Command  int    `json:"command"`
	Count    int    `json:"count"`
	Duration int    `json:"duration"`
	Error    bool   `json:"error"`
	Message  string `json:"message"`
	Session  string `json:"session"`
	Total    int    `json:"total"`
	Type     string `json:"type"`
}

// Get the Ring
func (r Ring) get() (Ring, error) {
	url := fmt.Sprintf("http://%s:%d/ring/%d", r.Host, r.Port, r.Cluster)
	res, err := http.Get(url)
	if err == nil {
		response, _ := ioutil.ReadAll(res.Body)
		err = json.Unmarshal(response, &r)
	}
	return r, err
}

// RegisterStatus of repair
func (r Ring) RegisterStatus(status RepairStatus) (RepairStatus, error) {
	//status.Percent = 100
	return status, nil
}

// Repair ring
func (r Ring) Repair(keyspace string, callback string) ([]Repair, error) {
	count := r.Count()
	repairs := make([]Repair, count)
	r, err := r.get()
	if err == nil {
		for _, token := range r.Tokens {
			for _, frag := range token.Ranges {
				repair := frag.Repair(r, keyspace, callback)
				repairs = append(repairs, repair)
			}
		}
		return repairs, nil
	}
	return nil, err
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
func (f Fragment) Repair(ring Ring, keyspace string, callback string) Repair {
	repair := Repair{
		Fragment: f,
		Keyspace: keyspace,
		Callback: callback,
	}

	return repair
}

// Run repair in cluster
func (r *Repair) Run(host string, port, cluster int) error {
	url := fmt.Sprintf("http://%s:%d/repair/%d", host, port, cluster)

	buf, err := json.Marshal(r)
	body := bytes.NewBuffer(buf)
	res, err := http.Post(url, "application/json", body)
	if err == nil {
		response, _ := ioutil.ReadAll(res.Body)
		err = json.Unmarshal(response, r)
	}
	return err
}
