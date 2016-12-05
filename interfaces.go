package cagrr

import "time"

// Closer closes DB connection
type Closer interface {
	Close()
}

// DB implements DB interface
type DB interface {
	ValueReader
	ValueWriter
	Closer
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
	Obtain(keyspace, cluster string, slices int) ([]*Fragment, []*Table, error)
}

// Regulator moderates the process
type Regulator interface {
	LimitRateTo(key string, duration time.Duration)
	Limit(key string)
}

// RepairRunner starts fragment repair via Cajrr
type RepairRunner interface {
	RunRepair(repair *Repair) error
}

// Scheduler creates jobs in time
type Scheduler interface {
	ServeAt(callback string) Scheduler
	Schedule(chan *Repair)
}

// ValueReader reads position data from DB
type ValueReader interface {
	ReadValue(string, string) string
	ReadOrCreate(string, string, string) string
}

// ValueWriter writes position to DB
type ValueWriter interface {
	WriteValue(string, string, string) error
}
