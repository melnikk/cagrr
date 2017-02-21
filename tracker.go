package cagrr

import (
	"encoding/json"
	"strconv"
	"time"
)

const (
	tableName  = "repairs"
	timeFormat = "2006-01-02 15:04:05 -0700 -07"
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
	ck, kk, tk, rk := t.keys(cluster, keyspace, table, id)

	track := t.readTrack(rk)
	_, _, _, _, _, rd := track.Complete(time.Duration(0))
	rate := t.regulator.LimitRateTo(cluster, rd)
	track.Rate = rate
	t.writeTrack(rk, track)

	track = t.readTrack(tk)
	tt, tc, ta, tp, te, td := track.Complete(rd)
	track.Rate = rate
	t.writeTrack(tk, track)

	track = t.readTrack(kk)
	kt, kc, ka, kp, ke, kd := track.Complete(rd)
	track.Rate = rate
	t.writeTrack(kk, track)

	track = t.readTrack(ck)
	ct, cc, ca, cp, ce, cd := track.Complete(rd)
	track.Rate = rate
	t.writeTrack(ck, track)

	return &RepairStats{
		Cluster:            cluster,
		Keyspace:           keyspace,
		Table:              table,
		ID:                 id,
		Duration:           rd,
		Rate:               rate,
		TableTotal:         tt,
		TableCompleted:     tc,
		TablePercent:       tp,
		TableDuration:      td,
		TableAverage:       ta,
		TableEstimate:      te,
		KeyspaceTotal:      kt,
		KeyspaceCompleted:  kc,
		KeyspacePercent:    kp,
		KeyspaceDuration:   kd,
		KeyspaceAverage:    ka,
		KeyspaceEstimate:   ke,
		ClusterTotal:       ct,
		ClusterCompleted:   cc,
		ClusterPercent:     cp,
		ClusterDuration:    cd,
		ClusterAverage:     ca,
		ClusterEstimate:    ce,
		LastClusterSuccess: track.Finished,
	}

}

// IsCompleted check fragment completion
func (t *tracker) IsCompleted(cluster, keyspace, table string, id int, threshold time.Duration) bool {
	key := t.db.CreateKey(cluster, keyspace, table, strconv.Itoa(id))

	track := t.readTrack(key)

	return track.IsRepaired(threshold)
}

func (t *tracker) Restart(cluster, keyspace, table string, id int) {
	key := t.db.CreateKey(cluster, keyspace, table, strconv.Itoa(id))
	track := t.readTrack(key)
	track.Restart()
	t.writeTrack(key, track)
}

func (t *tracker) Skip(cluster, keyspace, table string, id int) {

	ck, kk, tk, _ := t.keys(cluster, keyspace, table, id)

	ct := t.readTrack(ck)
	kt := t.readTrack(kk)
	tt := t.readTrack(tk)

	ct.Skip()
	kt.Skip()
	tt.Skip()

	t.writeTrack(ck, ct)
	t.writeTrack(kk, kt)
	t.writeTrack(tk, tt)
}

func (t *tracker) Start(cluster, keyspace, table string, id int) {
	key := t.db.CreateKey(cluster, keyspace, table, strconv.Itoa(id))
	t.start(key, 1)
}

func (t *tracker) StartCluster(cluster string, total int) {
	key := t.db.CreateKey(cluster)
	t.start(key, total)
}

func (t *tracker) StartKeyspace(cluster, keyspace string, total int) {
	key := t.db.CreateKey(cluster, keyspace)
	t.start(key, total)
}

func (t *tracker) StartTable(cluster, keyspace, table string, total int) {
	key := t.db.CreateKey(cluster, keyspace, table)
	t.start(key, total)
}

func (t *tracker) readTrack(key string) *Track {
	var track Track
	value := t.db.ReadValue(tableName, key)
	json.Unmarshal(value, &track)
	return &track
}

func (t *tracker) writeTrack(key string, value *Track) {
	val, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	t.db.WriteValue(tableName, key, val)
}

func (t *tracker) keys(cluster, keyspace, table string, row int) (string, string, string, string) {
	clusterKey := t.db.CreateKey(cluster)
	keyspaceKey := t.db.CreateKey(cluster, keyspace)
	tableKey := t.db.CreateKey(cluster, keyspace, table)
	rowKey := t.db.CreateKey(clusterKey, keyspace, table, strconv.Itoa(row))
	return clusterKey, keyspaceKey, tableKey, rowKey
}

func (t *tracker) start(key string, total int) {
	track := t.readTrack(key)
	track.Start(total)
	t.writeTrack(key, track)
}
