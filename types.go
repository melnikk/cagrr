package cagrr

import (
	"net/http"
	"time"

	"github.com/boltdb/bolt"
)

// Cluster contains configuration of cluster item
type Cluster struct {
	ID        int
	Name      string      `yaml:"name"`
	Interval  string      `yaml:"interval"`
	Keyspaces []*Keyspace `yaml:"keyspaces"`
	obtainer  Obtainer
	regulator Regulator
	tracker   Tracker
}

// ClusterStats for logging
type ClusterStats struct {
	Cluster         string
	ClusterDuration time.Duration
	LastSuccess     time.Time
}

// Config is a configuration file struct
type Config struct {
	Conn         *Connector `yaml:"conn"`
	BufferLength int        `yaml:"buffer"`
	Clusters     []*Cluster `yaml:"clusters"`
}

// Connector to repair service
type Connector struct {
	Host string
	Port int
}

// Fragment of Token range for repair
type Fragment struct {
	ID       int `json:"id"`
	Endpoint string
	Start    string
	End      string
}

// Keyspace contains keyspace repair schedule description
type Keyspace struct {
	Name   string `yaml:"name"`
	tables []*Table
	total  int
}

// Navigation holds coordinates of next repair
type Navigation struct {
	Cluster  string
	Keyspace string
	Table    string
}

// Repair object
type Repair struct {
	ID       int    `json:"id"`
	Cluster  string `json:"cluster"`
	Keyspace string `json:"keyspace"`
	Table    string `json:"table"`
	Endpoint string `json:"endpoint"`
	Start    string `json:"start"`
	End      string `json:"end"`
}

// RepairStats for logging
type RepairStats struct {
	Cluster           string
	Keyspace          string
	Table             string
	ID                int
	Duration          time.Duration
	TableTotal        int
	TableCompleted    int
	TablePercent      float32
	TableAverage      time.Duration
	TableEstimate     time.Duration
	KeyspaceTotal     int
	KeyspaceCompleted int
	KeyspacePercent   float32
	KeyspaceAverage   time.Duration
	KeyspaceEstimate  time.Duration
	ClusterTotal      int
	ClusterCompleted  int
	ClusterPercent    float32
	ClusterAverage    time.Duration
	ClusterEstimate   time.Duration
}

// RepairStatus keeps status of repair
type RepairStatus struct {
	Repair  Repair
	Message string
	Type    string
}

// Table contains column families to repair
type Table struct {
	Name    string  `yaml:"name"`
	Size    int64   `yaml:"size"`
	Slices  int     `yaml:"slices"`
	Weight  float32 `yaml:"weight"`
	repairs []*Repair
	total   int
}

// Token represents cassandra key range
type Token struct {
	ID     string `json:"id"`
	Ranges []Fragment
}

// TokenSet is a set of Token
type TokenSet []Token

type boltDB struct {
	db *bolt.DB
}

type fixer struct {
	runner RepairRunner
}

type logger struct {
	err    error
	fields map[string]interface{}
}

type queue struct {
	nodes []time.Duration
	size  int
}

type regulator struct {
	queues map[string]DurationQueue
	size   int
}

type server struct {
	callback   string
	clusters   []*Cluster
	jobs       chan<- *Repair
	mux        *http.ServeMux
	navigation *Navigation
	obtainer   Obtainer
	regulator  Regulator
	tracker    Tracker
}

type tracker struct {
	completions map[string]bool
	counts      map[string]int
	db          DB
	durations   map[string]time.Duration
	starts      map[string]time.Time
	totals      map[string]int
}
