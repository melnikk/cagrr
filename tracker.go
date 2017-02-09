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
	}
}

// CompleteFragment makes repair completed and returns duration
func (t *tracker) CompleteFragment(cluster, keyspace, table string, id int) time.Duration {
	ck, kk, tk := t.keys(cluster, keyspace, table)

	rk := t.createKey(cluster, keyspace, table, id)
	duration := t.duration(rk)

	t.complete(rk, duration)
	t.complete(tk, duration)
	t.complete(kk, duration)
	t.complete(ck, duration)

	return duration
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

func (t *tracker) Track(cluster, keyspace, table string, id int, tt, kt, ct int) *RepairStats {
	ck, kk, tk := t.keys(cluster, keyspace, table)
	rk := t.createKey(cluster, keyspace, table, id)

	cc, ca, cp, ce := t.start(ck, ct)
	kc, ka, kp, ke := t.start(kk, kt)
	tc, ta, tp, te := t.start(tk, tt)
	t.start(rk, 1)

	return &RepairStats{
		Cluster:           cluster,
		Keyspace:          keyspace,
		Table:             table,
		ID:                id,
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

func (t *tracker) average(worktime time.Duration, completed int) time.Duration {
	average := int64(0)
	if completed > 0 {
		average = int64(worktime) / int64(completed)
	}
	return time.Duration(average)
}

func (t *tracker) complete(key string, duration time.Duration) int {
	_, ex := t.counts[key]
	if !ex {
		t.counts[key] = 0
		t.durations[key] = 0
	}
	t.completions[key] = true
	t.counts[key]++
	t.durations[key] += duration
	return t.counts[key]
}

func (t *tracker) completed(key string) int {
	c, ex := t.counts[key]
	if !ex {
		t.counts[key] = 0
	}
	return c
}

func (t *tracker) createKey(vars ...interface{}) string {
	result := "track"
	for _, v := range vars {
		result += fmt.Sprintf("_%s", v)
	}
	return result
}

func (t *tracker) duration(key string) time.Duration {
	start := t.startTime(key)
	now := time.Now()
	duration := now.Sub(start)
	return duration
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

func (t *tracker) start(key string, total int) (int, time.Duration, float32, time.Duration) {
	_, ex := t.starts[key]
	if !ex {
		t.starts[key] = time.Now()
	}

	worktime := t.totalDuration(key)
	completed := t.completed(key)
	average := t.average(worktime, completed)
	percent := t.percent(total, completed)
	estimate := t.estimate(average, total, completed)

	return completed, average, percent, estimate
}

func (t *tracker) startTime(key string) time.Time {
	tm, ex := t.starts[key]
	if !ex {
		t.starts[key] = time.Time{}
	}
	return tm
}

func (t *tracker) totalDuration(key string) time.Duration {
	return t.durations[key]
}
