package cagrr

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/skbkontur/cagrr/repair"
)

// Fixer repairs cluster
type Fixer interface {
	Repair(keyspace string, callback string) ([]Repair, error)
}

// Config is a configuration file struct
type Config struct {
	Clusters []ClusterConfig `yaml:"clusters"`
}

// ClusterConfig contains configuration of cluster item
type ClusterConfig struct {
	Host      string   `yaml:"host"`
	Port      int      `yaml:"port"`
	Interval  string   `yaml:"interval"`
	Keyspaces []string `yaml:"keyspaces"`
}

// Ring represents several node combined in ring
type Ring struct {
	Host        string
	Port        int
	Cluster     int     `json:"cluster"`
	Name        string  `json:"name"`
	Partitioner string  `json:"partitioner"`
	Tokens      []Token `json:"tokens"`
	count       int32
	completed   int32
}

// Token represents primary key range
type Token struct {
	ID     string     `json:"key"`
	Ranges []Fragment `json:"ranges"`
}

// Fragment of Token range for repair
type Fragment struct {
	ring     *Ring
	ID       int    `json:"id"`
	Endpoint string `json:"endpoint"`
	Start    string `json:"start"`
	End      string `json:"end"`
}

// Repair is a Unit of repair work
type Repair struct {
	ID       int64    `json:"id"`
	Fragment Fragment `json:"fragment"`
	duration time.Duration
	host     string
	port     int
	cluster  int
	T1       time.Time
	T2       time.Time
	Callback string `json:"callback"`
	Keyspace string `json:"keyspace"`
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
func (r *Ring) Get() (*Ring, error) {
	url := fmt.Sprintf("http://%s:%d/ring/%d", r.Host, r.Port, r.Cluster)
	res, err := http.Get(url)
	if err == nil {
		response, _ := ioutil.ReadAll(res.Body)
		err = json.Unmarshal(response, &r)
	}
	return r, err
}

// Obtain ring
func (r *Ring) Obtain(keyspace, callback string, cluster int) ([]repair.Runner, error) {
	r.Cluster = cluster
	var result []repair.Runner

	r, err := r.Get()
	if err == nil {
		result, err = r.Repair(keyspace, callback)
		return result, err
	}
	return nil, err
}

// Repair ring
func (r *Ring) Repair(keyspace string, callback string) ([]repair.Runner, error) {
	r.completed = 0
	r.count = r.Count()
	repairs := make([]repair.Runner, r.count)
	for _, token := range r.Tokens {
		for _, frag := range token.Ranges {
			frag.ring = r
			repair := frag.Repair(r, keyspace, callback)
			repairs = append(repairs, &repair)
		}
	}
	return repairs, nil
}

// Count fragments of ring
func (r *Ring) Count() int32 {
	atomic.StoreInt32(&r.count, 0)
	for _, token := range r.Tokens {
		atomic.AddInt32(&r.count, int32(len(token.Ranges)))
	}
	return r.count
}

// CompleteRepair updates repair statistics of Ring
func (r *Ring) CompleteRepair(repair Repair) (int32, int32, int32) {
	repair.Complete()
	completed := atomic.AddInt32(&r.completed, 1)
	count := atomic.LoadInt32(&r.count)
	percent := r.Percent()
	return count, completed, percent
}

// Percent calculates percent of current repair
func (r *Ring) Percent() int32 {
	count := atomic.LoadInt32(&r.count)
	complete := atomic.LoadInt32(&r.completed)
	percent := 0
	if count > 0 {
		return complete * 100 / count
	}
	return int32(percent)
}

// Percent calculates percent of current repair
func (f Fragment) Percent() int32 {
	return f.ring.Percent()
}

// Repair fragment
func (f Fragment) Repair(r *Ring, keyspace string, callback string) Repair {
	repair := Repair{
		Fragment: f,
		host:     r.Host,
		port:     r.Port,
		cluster:  r.Cluster,
		Keyspace: keyspace,
		Callback: callback,
	}

	return repair
}

// Complete repair of fragment
func (r *Repair) Complete() {
	r.StopMeasure()
}

// Duration measure time of fragment's repair
func (r *Repair) Duration() time.Duration {
	duration := r.T2.Sub(r.T1)
	return duration
}

// StartMeasure fixes start time of Request
func (r *Repair) StartMeasure() {
	r.T1 = time.Now()
}

// StopMeasure fixes end time of Request
func (r *Repair) StopMeasure() {
	r.T2 = time.Now()
}

// Percent returns percent of fragment
func (r *Repair) Percent() int32 {
	return r.Fragment.Percent()
}

// Run repair in cluster
func (r *Repair) Run() error {
	r.StartMeasure()
	url := fmt.Sprintf("http://%s:%d/repair/%d", r.host, r.port, r.cluster)

	buf, err := json.Marshal(r)
	body := bytes.NewBuffer(buf)
	res, err := http.Post(url, "application/json", body)
	if err == nil {
		response, _ := ioutil.ReadAll(res.Body)
		err = json.Unmarshal(response, &r)
	}
	return err
}
