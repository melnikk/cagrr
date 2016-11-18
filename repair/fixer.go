package repair

import (
	"fmt"
	"strconv"

	"github.com/skbkontur/cagrr"
	"github.com/skbkontur/cagrr/db"
)

const (
	dbTable = "positions"
)

// Fixer starts repair cycle
type Fixer interface {
	Fix(jobs <-chan Runner)
}

type fixer struct {
	database db.DB
	cluster  *cagrr.ClusterConfig
	keyspace *cagrr.Keyspace
	tables   *cagrr.Table
	position int
}

// NewFixer creates fixer of clusters
func NewFixer(database db.DB, cluster *cagrr.ClusterConfig, keyspace *cagrr.Keyspace, tables *cagrr.Table, total int) Fixer {
	database.WriteValue(dbTable, "init", "bucket")
	result := fixer{database, cluster, keyspace, tables, 0}
	position := result.loadPosition()
	TrackTable(cluster, keyspace, tables, total, position)
	result.position = position
	return &result
}

func (f *fixer) Fix(jobs <-chan Runner) {
	log.WithFields(f).Debug("Starting fix loop")
	n := 0
	for job := range jobs {
		if n++; n <= f.position {
			continue
		}

		err := job.Run(f.tables.Name)
		if err == nil {
			log.WithFields(job).Debug("Repair job started")
		}

		f.savePosition(n)
	}
	f.savePosition(0)
}

func (f *fixer) dbKey() string {
	return fmt.Sprintf("%d_%s_%s", f.cluster.ID, f.keyspace.Name, f.tables.Name)
}

func (f *fixer) loadPosition() int {
	key := f.dbKey()
	val := f.database.ReadValue(dbTable, key)
	i, _ := strconv.Atoi(val)
	return i
}

func (f *fixer) savePosition(n int) {
	key := f.dbKey()
	val := strconv.Itoa(n)
	f.database.WriteValue(dbTable, key, val)
}
