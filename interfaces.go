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

// Fixer starts repair cycle
type Fixer interface {
	Fix(jobs <-chan *Repair)
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

// Obtainer gets info about fragment from Cajrr
type Obtainer interface {
	ObtainTables(cluster, keyspace string) ([]*Table, error)
	ObtainFragments(cluster, keyspace string, slices int) ([]*Fragment, error)
}

// Regulator moderates the process
type Regulator interface {
	LimitRateTo(key string, duration time.Duration) time.Duration
	Limit(key string)
	Rate(key string) time.Duration
}

// RepairRunner starts fragment repair via Cajrr
type RepairRunner interface {
	RunRepair(repair *Repair) error
}

// Scheduler creates jobs in time
type Scheduler interface {
	ObtainBy(Obtainer) Scheduler
	RegulateWith(Regulator) Scheduler
	Schedule(chan *Repair)
	TrackIn(Tracker) Scheduler
}

// Server serves repair handlers
type Server interface {
	ServeAt(callback string) Server
}

// Tracker keeps progress of repair
type Tracker interface {
	Complete(cluster, keyspace, table string, repair int) *RepairStats
	IsCompleted(cluster, keyspace, table string, repair int) bool
	Restart(cluster, keyspace, table string, repair int)
	Start(cluster, keyspace, table string, repair, tt, kt, ct int)
}

// ValueReader reads position data from DB
type ValueReader interface {
	ReadValue(string, string) []byte
}

// ValueWriter writes position to DB
type ValueWriter interface {
	WriteValue(string, string, []byte) error
}
