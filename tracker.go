package cagrr

import (
	"errors"
	"fmt"
	"time"
)

// NewTracker created new progress tracker
func NewTracker(db DB) Tracker {

	return &tracker{
		completions: make(map[string]bool),
		counts:      make(map[string]int),
		currents:    make(map[string]string),
		db:          db,
		percents:    make(map[string]float32),
		starts:      make(map[string]time.Time),
		successes:   make(map[string]time.Time),
		totals:      make(map[string]int),
	}
}

// CompleteFragment makes repair completed and returns duration
func (t *tracker) CompleteFragment(cluster, keyspace, table string, id int) time.Duration {
	key := t.createKey(cluster, keyspace, table)
	t.complete(key)
	t.calculatePercents(cluster)

	key = t.createKey(cluster, keyspace, table, id)
	t.completions[key] = true
	startTime := t.startTime(key)
	finishTime := time.Now()
	duration := finishTime.Sub(startTime)
	return duration
}

// IsCompleted check fragment completion
func (t *tracker) IsCompleted(cluster, keyspace, table string, id int) bool {
	key := t.createKey(cluster, keyspace, table, id)
	completed := t.completions[key]
	return completed
}

// Estimate durations of repair
func (t *tracker) Estimate(cluster string) (time.Duration, time.Duration, time.Duration) {

	ck, kk, tk := t.keys(cluster)

	ec := t.estimate(ck)
	ek := t.estimate(kk)
	et := t.estimate(tk)

	return ec, ek, et
}

func (t *tracker) FragmentAverage(cluster string) time.Duration {
	_, _, tk := t.keys(cluster)
	worktime := t.duration(tk)
	average := int64(0)
	completed := t.completed(tk)

	if completed > 0 {
		average = int64(worktime) / int64(completed)
	}
	return time.Duration(average)
}

// LastSucces returns Timestamp of last successful repair
func (t *tracker) LastSuccess(cluster string) time.Time {
	key := t.createKey(cluster)
	s, ex := t.successes[key]
	if !ex {
		return time.Time{}
	}
	return s
}

func (t *tracker) Percentage(cluster string) (float32, float32, float32) {

	ck, kk, tk := t.keys(cluster)

	pc := t.percent(ck)
	pk := t.percent(kk)
	pt := t.percent(tk)

	return pc, pk, pt
}

func (t *tracker) Restart(cluster, keyspace, table string, id int) {
	key := t.createKey(cluster, keyspace, table, id)
	t.completions[key] = false
}

/*
// StartCluster sets start time of cluster repair
func (t *tracker) StartCluster(cluster string, length int) {
	key := t.createKey(cluster)
	t.start(key)
	t.length(key, length)
}

// StartKeypace sets start time of keyspace repair
func (t *tracker) StartKeyspace(cluster, keyspace string, length int) {
	key := t.createKey(cluster)
	t.currents[key] = keyspace

	key = t.createKey(cluster, keyspace)
	t.start(key)
	t.length(key, length)
}

// StartTable sets start time of table repair
func (t *tracker) StartTable(cluster, keyspace, table string, length int) {
	key := t.createKey(cluster, keyspace)
	t.currents[key] = table

	key = t.createKey(cluster, keyspace, table)
	t.start(key)
	t.length(key, length)
}
*/
func (t *tracker) Total(cluster string) int {
	ck, _, _ := t.keys(cluster)
	return t.total(ck)
}

func (t *tracker) Track(cluster, keyspace, table string, id int, total int) {
	ck, kk, tk := t.keys(cluster)
	if !t.started(ck) {
		t.start(ck)
		t.length(ck, total)
	}
	if !t.started(kk) {
		t.start(kk)
	}
	if !t.started(tk) {
		t.start(tk)
	}

	key := t.createKey(cluster, keyspace, table, id)
	t.start(key)
}

func (t *tracker) calculatePercent(key string) float32 {
	complete := t.completed(key)
	if complete == 0 {
		return 0
	}
	return (100 * float32(complete) / float32(t.total(key)))
}
func (t *tracker) calculatePercents(cluster string) {
	ck, kk, tk := t.keys(cluster)

	ptk := t.calculatePercent(tk)
	pkk := t.calculatePercent(kk) + ptk/float32(t.total(kk))
	pck := t.calculatePercent(ck) + pkk/float32(t.total(ck))

	t.percents[tk] = ptk
	t.percents[kk] = pkk
	t.percents[ck] = pck

	if t.percent(tk) == 100 {
		t.complete(kk)

		if t.percent(kk) == 100 {
			t.complete(ck)

			if t.percent(ck) == 100 {
				t.success(ck)
			}
		}
	}

}

func (t *tracker) complete(key string) int {

	_, ex := t.counts[key]
	if !ex {
		t.counts[key] = 0
	}
	t.counts[key]++
	return t.counts[key]
}

func (t *tracker) completed(key string) int {
	c, ex := t.counts[key]
	if !ex {
		log.WithError(errors.New(key)).Error("Tracked complete not started") // TODO try to read from DB
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

func (t *tracker) current(key string) string {
	c, ex := t.currents[key]
	if !ex {
		log.WithError(errors.New(key)).Error("Tracked current not started") // TODO try to read from DB
	}
	return c
}

func (t *tracker) duration(key string) time.Duration {
	start := t.startTime(key)
	now := time.Now()
	duration := now.Sub(start)
	return duration
}

func (t *tracker) estimate(key string) time.Duration {
	percent := t.percent(key)
	if percent == 0 {
		return 0
	}
	duration := t.duration(key)
	result := int64(duration) * int64((100-percent)/percent)
	return time.Duration(result)
}

func (t *tracker) keys(cluster string) (string, string, string) {
	clusterKey := t.createKey(cluster)
	keyspace := t.current(clusterKey)
	keyspaceKey := t.createKey(cluster, keyspace)
	table := t.current(keyspaceKey)
	tableKey := t.createKey(cluster, keyspace, table)

	return clusterKey, keyspaceKey, tableKey
}

func (t *tracker) length(key string, length int) {
	t.totals[key] = length
}

func (t *tracker) percent(key string) float32 {
	pc, ex := t.percents[key]
	if !ex {
		log.WithError(errors.New(key)).Error("Tracked percent not started") // TODO try to read from DB
	}
	return pc
}

func (t *tracker) start(key string) {
	t.counts[key] = 0
	t.percents[key] = 0
	t.starts[key] = time.Now()
}

func (t *tracker) started(key string) bool {
	_, ex := t.starts[key]
	return ex
}

func (t *tracker) startTime(key string) time.Time {
	tm, ex := t.starts[key]
	if !ex {
		log.WithError(errors.New(key)).Error("Tracked start not started") // TODO try to read from DB
	}
	return tm
}

func (t *tracker) success(key string) {
	t.successes[key] = time.Now()
}

func (t *tracker) total(key string) int {
	total, ex := t.totals[key]
	if !ex {
		log.WithError(errors.New(key)).Error("Tracked total not started") // TODO try to read from DB
	}
	return total

}
