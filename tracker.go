package cagrr

import (
	"fmt"
	"sync/atomic"
	"time"
)

var (
	tracks = make(map[tableRepairKey]*tableStats)
)

// StartTableTrack inits track of table
func StartTableTrack(cluster string, keyspace, table string, total, position int) {
	stats := getTrackTable(cluster, keyspace, table)
	stats.started = time.Now()
	// Dumb protection
	if total == 0 {
		total = 1
	}
	stats.total = int32(total)
	stats.completed = int32(position)
}

// StartRepairTrack begins trace of repair
func StartRepairTrack(cluster string, keyspace, table string, id int) {
	stats := getTrackTable(cluster, keyspace, table)
	stats.start(id)
}

func getTrackTable(cluster string, keyspace, table string) *tableStats {
	tableKey := tableRepairKey{cluster, keyspace, table}
	stats, exists := tracks[tableKey]
	if !exists {
		statsmap := make(map[int]*tableRepairStats)
		stats = &tableStats{
			cluster:  cluster,
			keyspace: keyspace,
			table:    table,
			repairs:  statsmap,
			started:  time.Now(),
		}
		tracks[tableKey] = stats
	}

	return stats
}

func (t *tableStats) start(id int) {
	stats := tableRepairStats{
		id:      id,
		Started: time.Now(),
	}
	t.repairs[id] = &stats
}

func (t *tableStats) statistics() RepairStats {
	completed := atomic.LoadInt32(&t.completed)
	total := atomic.LoadInt32(&t.total)
	percent := completed * 100 / total

	fragmentLeft := float32(total) - float32(completed)

	worktime := time.Now().Sub(t.started)
	timeLeft := float32(0)

	if completed > 0 {
		timeLeft = float32(worktime) * fragmentLeft / float32(completed)
	}
	return RepairStats{
		Cluster:   t.cluster,
		Keyspace:  t.keyspace,
		Table:     t.table,
		Completed: completed,
		Total:     total,
		Percent:   percent,
		Estimate:  fmt.Sprintf("%s", time.Duration(timeLeft))}
}

func (t *tableStats) finish(id int) time.Duration {
	atomic.AddInt32(&t.completed, 1)

	repair := t.repairs[id]
	repair.Finished = time.Now()
	return repair.Finished.Sub(repair.Started)
}
