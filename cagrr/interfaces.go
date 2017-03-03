package cagrr

import "time"

// Closer closes DB connection
type Closer interface {
	Close()
}

// DB implements DB interface
type DB interface {
	CreateKey(keys ...string) string
	ValueReader
	ValueWriter
	Closer
}

// DurationQueue is a fixed size FIFO queue of durations
type DurationQueue interface {
	Push(time.Duration)
	Pop() time.Duration
	Len() int
	Average() time.Duration
}

// Logger logs messages
type Logger interface {
	WithError(err error) Logger
	WithFields(str interface{}) Logger
	Debug(message interface{}) Logger
	Error(message interface{}) Logger
	Fatal(message interface{}) Logger
	Warn(message interface{}) Logger
	Info(message interface{}) Logger
}

// Regulator moderates the process
type Regulator interface {
	LimitRateTo(key string, duration time.Duration) time.Duration
	Limit(key string)
	Rate(key string) time.Duration
}

// Scheduler creates jobs in time
type Scheduler interface {
	RegulateWith(Regulator) Scheduler
	Schedule()
	TrackIn(Tracker) Scheduler
	Until(chan bool) Scheduler
}

// Server serves repair handlers
type Server interface {
	ServeAt(callback string) Server
}

// Tracker keeps progress of repair
type Tracker interface {
	Complete(cluster, keyspace, table string, repair int, err bool) *RepairStats
	HasErrors(keys ...string) bool
	IsCompleted(cluster, keyspace, table string, repair int, threshold time.Duration) bool
	Restart(cluster, keyspace, table string, repair int)
	Skip(cluster, keyspace, table string, repair int)
	Start(cluster, keyspace, table string, repair int)
	StartTable(cluster, keyspace, table string, total int)
	StartKeyspace(cluster, keyspace string, total int)
	StartCluster(cluster string, total int)
	TrackError(cluster, keyspace, table string, id int)
}

// ValueReader reads position data from DB
type ValueReader interface {
	ReadValue(string, string) []byte
}

// ValueWriter writes position to DB
type ValueWriter interface {
	WriteValue(string, string, []byte) error
}
