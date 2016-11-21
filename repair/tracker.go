package repair

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/skbkontur/cagrr"
)

type repairKey struct {
	cluster  string
	keyspace string
	tables   string
}

type repairStats struct {
	id       int
	Started  time.Time
	Finished time.Time
}

type tableStats struct {
	cluster   string
	keyspace  string
	tables    string
	repairs   map[int]*repairStats
	started   time.Time
	total     int32
	completed int32
}

// Stats for logging
type Stats struct {
	Cluster   string
	Keyspace  string
	Tables    string
	Total     int32
	Completed int32
	Percent   int32
	Estimate  string
}

var (
	tracks = make(map[repairKey]*tableStats)
)

// TrackTable traces repair calls and progress
func TrackTable(cluster *cagrr.Cluster, keyspace *cagrr.Keyspace, tables *cagrr.Table, total, position int) {
	table := getTrackTable(cluster.Name, keyspace.Name, tables.Name)
	table.started = time.Now()
	// Dump protection
	if total == 0 {
		total = 1
	}
	table.total = int32(total)
	table.completed = int32(position)
}

// TrackRepair traces repair calls and progress
func TrackRepair(cluster string, keyspace, tables string, id int) {
	table := getTrackTable(cluster, keyspace, tables)
	table.start(id)
}

// TrackStatus fixes status of repair
func TrackStatus(status cagrr.RepairStatus) error {
	var err error
	switch status.Type {
	case "COMPLETE":
		table := getTrackTable(status.Cluster, status.Keyspace, status.Tables)
		duration := table.finish(status.ID)
		stats := table.statistics()
		log.WithFields(stats).Info("Fragment completed")

		regulator.LimitRateTo(duration)
	case "ERROR":
		err = errors.New("Error in cajrr")
	}
	return err
}

func getTrackTable(cluster string, keyspace, tables string) *tableStats {
	tableKey := repairKey{cluster, keyspace, tables}
	table, exists := tracks[tableKey]
	if !exists {
		statsmap := make(map[int]*repairStats)
		table = &tableStats{cluster, keyspace, tables, statsmap, time.Now(), 0, 0}
		tracks[tableKey] = table
	}

	return table
}

func (t *tableStats) statistics() Stats {
	completed := atomic.LoadInt32(&t.completed)
	total := atomic.LoadInt32(&t.total)
	percent := completed * 100 / total

	fragmentLeft := float32(total) - float32(completed)

	worktime := time.Now().Sub(t.started)
	timeLeft := float32(0)

	if completed > 0 {
		timeLeft = float32(worktime) * fragmentLeft / float32(completed)
	}
	return Stats{t.cluster, t.keyspace, t.tables, total, completed, percent, fmt.Sprintf("%s", time.Duration(timeLeft))}
}

func (t *tableStats) start(id int) {
	stats := repairStats{
		id:      id,
		Started: time.Now(),
	}
	t.repairs[id] = &stats
}

func (t *tableStats) finish(id int) time.Duration {
	atomic.AddInt32(&t.completed, 1)

	repair := t.repairs[id]
	repair.Finished = time.Now()
	return repair.Finished.Sub(repair.Started)
}
