package cagrr

import (
	"time"
)

const (
	keyspaceRepairs = "KeyspaceRepairs"
)

// RegisterStart sets start time of keyspace repair
func (k *Keyspace) RegisterStart(tables []*Table) {
	k.percent = 0
	k.Tables = tables

	db := getDatabase()
	key := dbKey("start", k.Name)
	db.WriteValue(keyspaceRepairs, key, time.Now().String())
}

// RegisterFinish sets value of successful keyspace repair
func (k *Keyspace) RegisterFinish() {
	k.percent = 100

	startKey := dbKey("start", k.Name)
	db := getDatabase()
	db.WriteValue(clusterRepairs, startKey, "")
}

func (k *Keyspace) findTable(name string) *Table {
	for i, t := range k.Tables {
		if t.Name == name {
			return k.Tables[i]
		}
	}
	panic("Table not found")
}

func (k *Keyspace) percentage() float32 {
	if k.percent == 100 {
		return k.percent
	}

	result := float32(0)
	for _, t := range k.Tables {
		result += t.percentage()
	}
	length := float32(len(k.Tables))
	if length > 0 {
		result = result / length
	}

	return result
}

func (k *Keyspace) estimate() time.Duration {
	percent := k.percentage()
	if percent == 100 {
		return 0
	}
	db := getDatabase()
	key := dbKey("start", k.Name)
	val := db.ReadValue(keyspaceRepairs, key)
	started, err := time.Parse("2006-01-02 15:04:05.9 -0700 -07", val)
	if err != nil {
		log.WithError(err).Warn("Error parse time of keyspace start")
		return 0
	}
	now := time.Now()
	duration := now.Sub(started)

	result := float32(duration) * float32((100-percent)/percent)
	return time.Duration(result)
}
