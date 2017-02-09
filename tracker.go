package cagrr

import (
	"fmt"
	"time"
)

// NewTracker created new progress tracker
func NewTracker(db DB) Tracker {

	return &tracker{
		completions: make(map[string]bool),
		counts:      make(map[string]int),
		db:          db,
		durations:   make(map[string]time.Duration),
		starts:      make(map[string]time.Time),
		totals:      make(map[string]int),
	}
}

// Complete repair and returns statistics
func (t *tracker) Complete(cluster, keyspace, table string, id int) *RepairStats {
	ck, kk, tk := t.keys(cluster, keyspace, table)

	rk := t.createKey(cluster, keyspace, table, id)

	start := t.starts[rk]
	now := time.Now()
	duration := now.Sub(start)

	t.complete(rk, duration)
	tt, tc, ta, tp, te := t.complete(tk, duration)
	kt, kc, ka, kp, ke := t.complete(kk, duration)
	ct, cc, ca, cp, ce := t.complete(ck, duration)
	return &RepairStats{
		Cluster:           cluster,
		Keyspace:          keyspace,
		Table:             table,
		ID:                id,
		Duration:          duration,
		TableTotal:        tt,
		TableCompleted:    tc,
		TablePercent:      tp,
		TableAverage:      ta,
		TableEstimate:     te,
		KeyspaceTotal:     kt,
		KeyspaceCompleted: kc,
		KeyspacePercent:   kp,
		KeyspaceAverage:   ka,
		KeyspaceEstimate:  ke,
		ClusterTotal:      ct,
		ClusterCompleted:  cc,
		ClusterPercent:    cp,
		ClusterAverage:    ca,
		ClusterEstimate:   ce,
	}

}

// IsCompleted check fragment completion
func (t *tracker) IsCompleted(cluster, keyspace, table string, id int) bool {
	key := t.createKey(cluster, keyspace, table, id)
	completed := t.completions[key]
	return completed
}

func (t *tracker) Restart(cluster, keyspace, table string, id int) {
	key := t.createKey(cluster, keyspace, table, id)
	t.completions[key] = false
}

func (t *tracker) Start(cluster, keyspace, table string, id int, tt, kt, ct int) {
	ck, kk, tk := t.keys(cluster, keyspace, table)
	rk := t.createKey(cluster, keyspace, table, id)

	t.start(ck, ct)
	t.start(kk, kt)
	t.start(tk, tt)
	t.start(rk, 1)

}

func (t *tracker) average(worktime time.Duration, completed int) time.Duration {
	average := int64(0)
	if completed > 0 {
		average = int64(worktime) / int64(completed)
	}
	return time.Duration(average)
}

func (t *tracker) complete(key string, duration time.Duration) (int, int, time.Duration, float32, time.Duration) {

	t.completions[key] = true
	t.counts[key]++
	t.durations[key] += duration

	completed := t.counts[key]
	total := t.totals[key]
	worktime := t.durations[key]

	average := t.average(worktime, completed)
	estimate := t.estimate(average, total, completed)
	percent := t.percent(total, completed)

	return total, completed, average, percent, estimate
}

func (t *tracker) createKey(vars ...interface{}) string {
	result := "track"
	for _, v := range vars {
		result += fmt.Sprintf("_%s", v)
	}
	return result
}

func (t *tracker) estimate(average time.Duration, total, completed int) time.Duration {
	result := int64(average) * int64(total-completed)
	return time.Duration(result)
}

func (t *tracker) keys(cluster, keyspace, table string) (string, string, string) {
	clusterKey := t.createKey(cluster)
	keyspaceKey := t.createKey(cluster, keyspace)
	tableKey := t.createKey(cluster, keyspace, table)

	return clusterKey, keyspaceKey, tableKey
}

func (t *tracker) percent(total, completed int) float32 {
	return (100 * float32(completed) / float32(total))

}

func (t *tracker) start(key string, total int) {
	_, ex := t.starts[key]
	if !ex {
		t.completions[key] = false
		t.starts[key] = time.Now()
		t.counts[key] = 0
		t.durations[key] = time.Duration(0)
		t.totals[key] = total
	}
}
