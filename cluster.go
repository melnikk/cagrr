package cagrr

import (
	"time"
)

// LastSuccesfullRepairTime returns Timestamp of last repair
func (c *Cluster) LastSuccesfullRepairTime() (time.Time, error) {
	db := getDatabase()
	key := dbKey("lastSuccess", c.Name)
	val := db.ReadValue("cluster-repairs", key)
	return time.Parse("", val)
}

// RegisterStart sets start time of cluster repair
func (c *Cluster) RegisterStart() {
	c.percent = 0
}

// RegisterFinish sets value of successful whole cluster repair
func (c *Cluster) RegisterFinish() {
	c.percent = 100
}

func (c *Cluster) percentage() int32 {
	if c.percent == 100 {
		return c.percent
	}
	result := int32(0)
	for _, k := range c.Keyspaces {
		result += k.percentage()
	}
	result = result / int32(len(c.Keyspaces))
	c.percent = result
	return result
}

func (c *Cluster) estimate() time.Duration {
	result := 0
	for _, k := range c.Keyspaces {
		result += int(k.estimate())
	}
	result = result / len(c.Keyspaces)
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
	return name + cluster
}
