package cagrr

import (
	"net/http"
	"time"

	redis "gopkg.in/redis.v5"

	"github.com/hashicorp/consul/api"
)

// Cluster contains configuration of cluster item
type Cluster struct {
	ID        int
	Name      string      `yaml:"name"`
	Interval  string      `yaml:"interval"`
	Keyspaces []*Keyspace `yaml:"keyspaces"`
	Host      string
	Port      int
	done      chan bool
	regulator Regulator
	tracker   Tracker
}

// ClusterStats for logging
type ClusterStats struct {
	Cluster            string
	ClusterDuration    time.Duration
	LastClusterSuccess time.Time
}

// Config is a configuration file struct
type Config struct {
	BufferLength int        `yaml:"buffer"`
	Clusters     []*Cluster `yaml:"clusters"`
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
	Cluster            string
	Keyspace           string
	Table              string
	ID                 int
	Duration           time.Duration
	Rate               time.Duration
	TableTotal         int
	TableCompleted     int
	TablePercent       float32
	TableDuration      time.Duration
	TableAverage       time.Duration
	TableEstimate      time.Duration
	KeyspaceTotal      int
	KeyspaceCompleted  int
	KeyspacePercent    float32
	KeyspaceDuration   time.Duration
	KeyspaceAverage    time.Duration
	KeyspaceEstimate   time.Duration
	ClusterTotal       int
	ClusterCompleted   int
	ClusterPercent     float32
	ClusterDuration    time.Duration
	ClusterAverage     time.Duration
	ClusterEstimate    time.Duration
	LastClusterSuccess time.Time
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

// Track of repair item
type Track struct {
	Completed bool
	Count     int
	Total     int
	Percent   float32
	Duration  time.Duration
	Average   time.Duration
	Estimate  time.Duration
	Rate      time.Duration
	Finished  time.Time
	Started   time.Time
}

type consulDB struct {
	db *api.Client
}

type logger struct {
	err    error
	fields map[string]interface{}
}

type queue struct {
	nodes []time.Duration
	size  int
}

type redisDB struct {
	db *redis.Client
}

type regulator struct {
	queues map[string]DurationQueue
	size   int
}

type server struct {
	callback string
	clusters []*Cluster
	jobs     chan<- *Repair
	mux      *http.ServeMux
	tracker  Tracker
}

type tracker struct {
	db        DB
	regulator Regulator
}
