package cagrr

import (
	"net/http"
	"time"

	"github.com/boltdb/bolt"
)

// Cluster contains configuration of cluster item
type Cluster struct {
	ID        int
	Name      string     `yaml:"name"`
	Interval  string     `yaml:"interval"`
	Keyspaces []Keyspace `yaml:"keyspaces"`
}

// Config is a configuration file struct
type Config struct {
	Conn     Connector `yaml:"conn"`
	Clusters []Cluster `yaml:"clusters"`
}

// Connector connects scheduler to repairer
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
	Name   string  `yaml:"name"`
	Slices int     `yaml:"slices"`
	Tables []Table `yaml:"tables"`
}

// Queue is a basic FIFO queue based on a circular list that resizes as needed.
type Queue struct {
	nodes []*QueueNode
	size  int
	head  int
	tail  int
	count int
}

// QueueNode is Duration struct
type QueueNode struct {
	Value time.Duration
}

// Repair object
type Repair struct {
	ID       int    `json:"id"`
	Start    string `json:"start"`
	End      string `json:"end"`
	Endpoint string `json:"endpoint"`
	Cluster  string `json:"cluster"`
	Keyspace string `json:"keyspace"`
	Table    string `json:"table"`
	Callback string `json:"callback"`
}

// RepairStats for logging
type RepairStats struct {
	Cluster          string
	Keyspace         string
	Table            string
	Completed        int32
	Percent          int32
	Total            int32
	TotalTables      int32
	TotalPercent     int32
	Estimate         string
	EstimateKeyspace string
	EstimateCluster  string
}

// RepairStatus keeps status of repair
type RepairStatus struct {
	Repair  Repair
	Command int32
	Options string
	Message string
	Session string
	Type    string
}

// Table contains column families to repair
type Table struct {
	Name   string `yaml:"name"`
	Slices int    `yaml:"slices"`
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

type regulator struct {
	queue     *Queue
	counter   int
	threshold int
	state     string
	rate      time.Duration
	Timeout   time.Duration
}

type scheduler struct {
	callback  string
	clusters  []Cluster
	db        DB
	jobs      chan<- Repair
	mux       *http.ServeMux
	obtainer  Obtainer
	regulator Regulator
}

type tableStats struct {
	cluster   string
	keyspace  string
	table     string
	repairs   map[int]*tableRepairStats
	started   time.Time
	total     int32
	completed int32
}
type tableRepairKey struct {
	cluster  string
	keyspace string
	table    string
}

type tableRepairStats struct {
	id       int
	Started  time.Time
	Finished time.Time
}
