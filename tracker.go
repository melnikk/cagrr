package cagrr

import (
	"strconv"
	"time"
)

const (
	completions = "completions"
	counts      = "counts"
	durations   = "durations"
	starts      = "starts"
	timeFormat  = "2006-01-02 15:04:05 -0700 -07"
	totals      = "totals"
)

// NewTracker created new progress tracker
func NewTracker(db DB, r Regulator) Tracker {

	return &tracker{
		db:        db,
		regulator: r,
	}
}

// Complete repair and returns statistics
func (t *tracker) Complete(cluster, keyspace, table string, id int) *RepairStats {
	ck, kk, tk := t.keys(cluster, keyspace, table)

	rk := t.db.CreateKey(cluster, keyspace, table, strconv.Itoa(id))

	startValue := string(t.db.ReadValue(starts, rk))
	start, _ := time.Parse(timeFormat, startValue)

	now := time.Now()
	duration := now.Sub(start)

	rate := t.regulator.LimitRateTo(cluster, duration)

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
		Rate:              rate,
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
	key := t.db.CreateKey(cluster, keyspace, table, strconv.Itoa(id))
	completed, _ := t.db.ReadOrCreate(completions, key, "false")
	result, _ := strconv.ParseBool(string(completed))
	return result
}

func (t *tracker) Restart(cluster, keyspace, table string, id int) {
	key := t.db.CreateKey(cluster, keyspace, table, strconv.Itoa(id))
	t.db.WriteValue(completions, key, "false")
}

func (t *tracker) Start(cluster, keyspace, table string, id int, tt, kt, ct int) {
	ck, kk, tk := t.keys(cluster, keyspace, table)
	rk := t.db.CreateKey(cluster, keyspace, table, strconv.Itoa(id))

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

	t.db.WriteValue(completions, key, "true")
	completedValue, _ := t.db.ReadOrCreate(counts, key, "0")
	completed, _ := strconv.Atoi(string(completedValue))
	completed++
	t.db.WriteValue(counts, key, strconv.Itoa(completed))

	worktimeValue := t.db.ReadValue(durations, key)
	worktime, _ := time.ParseDuration(string(worktimeValue))
	worktime += duration
	t.db.WriteValue(durations, key, worktime.String())

	total, _ := strconv.Atoi(string(t.db.ReadValue(totals, key)))

	average := t.average(worktime, completed)
	estimate := t.estimate(average, total, completed)
	percent := t.percent(total, completed)

	if percent == 100 {
		t.db.WriteValue(completions, key, "true")
	}

	return total, completed, average, percent, estimate
}

func (t *tracker) estimate(average time.Duration, total, completed int) time.Duration {
	result := int64(average) * int64(total-completed)
	return time.Duration(result)
}

func (t *tracker) keys(cluster, keyspace, table string) (string, string, string) {
	clusterKey := t.db.CreateKey(cluster)
	keyspaceKey := t.db.CreateKey(cluster, keyspace)
	tableKey := t.db.CreateKey(cluster, keyspace, table)

	return clusterKey, keyspaceKey, tableKey
}

func (t *tracker) percent(total, completed int) float32 {
	return (100 * float32(completed) / float32(total))

}

func (t *tracker) start(key string, total int) {
	_, ex := t.db.ReadOrCreate(starts, key, time.Now().String())
	if !ex {
		t.db.WriteValue(completions, key, "false")
		t.db.WriteValue(counts, key, "0")
		t.db.WriteValue(durations, key, "0s")
		t.db.WriteValue(totals, key, strconv.Itoa(total))
	}
}
