package cagrr

import (
	"strings"
	"time"
)

const (
	clusterRepairs        = "ClusterRepairs"
	clusterLastSuccessKey = "LastSuccess"
	clusterStartKey       = "start"
	clusterTimeFormat     = "2006-01-02 15:04:05.9 -0700 -07"
)

// LastSuccesfullRepairTime returns Timestamp of last repair
func (c *Cluster) LastSuccesfullRepairTime() string {
	db := getDatabase()
	key := dbKey(clusterLastSuccessKey, c.Name)
	val := db.ReadOrCreate(clusterRepairs, key, time.Time{}.Format(clusterTimeFormat))
	return val
}

// RegisterStart sets start time of cluster repair
func (c *Cluster) RegisterStart() {
	c.percent = 0
	db := getDatabase()

	key := dbKey(clusterStartKey, c.Name)
	db.WriteValue(clusterRepairs, key, time.Now().Format(clusterTimeFormat))
	db.WriteValue(currentPositions, c.Name, "0")
}

// RegisterFinish sets value of successful whole cluster repair
func (c *Cluster) RegisterFinish() {
	c.percent = 100

	successKey := dbKey(clusterLastSuccessKey, c.Name)
	startKey := dbKey(clusterStartKey, c.Name)

	db := getDatabase()
	db.WriteValue(clusterRepairs, successKey, time.Now().Format(clusterTimeFormat))
	db.WriteValue(clusterRepairs, startKey, "")
	db.WriteValue(savedPositions, c.Name, "0")
}

func (c *Cluster) percentage() float32 {
	if c.percent == 100 {
		return float32(c.percent)
	}
	result := float32(0)
	for _, k := range c.Keyspaces {
		result += k.percentage()
	}
	result = result / float32(len(c.Keyspaces))
	c.percent = result
	return result
}

func (c *Cluster) estimate() time.Duration {
	percent := c.percentage()
	if percent == 100 {
		return 0
	}
	db := getDatabase()
	key := dbKey(clusterStartKey, c.Name)
	val := db.ReadValue(clusterRepairs, key)
	started, err := time.Parse(clusterTimeFormat, val)
	if err != nil {
		log.WithError(err).Warn("Error parse time of cluster start")
		return 0
	}
	now := time.Now()
	duration := now.Sub(started)

	result := float32(duration) * float32((100-percent)/percent)
	return time.Duration(result)
}

func (c *Cluster) findKeyspace(name string) *Keyspace {
	for i, ks := range c.Keyspaces {
		if ks.Name == name {
			return c.Keyspaces[i]
		}
	}
	return nil
}

func dbKey(name, cluster string) string {
	return strings.Replace(name+cluster, " ", "", -1)
}
