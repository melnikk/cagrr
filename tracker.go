package cagrr

import (
	"fmt"
	"sync/atomic"
	"time"
)

var (
	tracks = make(map[tableRepairKey]*tableStats)
)

// StartTableTrack begins trace of repair
func StartTableTrack(cluster, keyspace, table string, total int32) {
	stats := getTrackTable(cluster, keyspace, table)
	stats.total = total
}

// FinishRepairTrack stops tracking and returns mestrics
func FinishRepairTrack(repair Repair) (time.Duration, RepairStats) {
	stats := getTrackTable(repair.Cluster, repair.Keyspace, repair.Table)
	duration := stats.finish(repair.ID)
	logStats := stats.statistics()
	return duration, logStats
}

// StartRepairTrack begins trace of repair
func StartRepairTrack(rep Repair) {
	stats := getTrackTable(rep.Cluster, rep.Keyspace, rep.Table)
	stats.start(rep.ID)
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
			total:    1,
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
