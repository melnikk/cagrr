package cagrr

import (
	"fmt"
	"time"
)

// Schedule cluster repair
func (c *Cluster) Schedule(jobs chan *Repair) {

	for {
		log.WithFields(c).Debug("Starting cluster")
		keyspaces, total := c.obtainKeyspaces()
		c.tracker.StartCluster(c.Name, total)

		for _, k := range keyspaces {
			log.WithFields(k).Debug("Starting keyspace")
			c.tracker.StartKeyspace(c.Name, k.Name, k.Total())

			for _, t := range k.Tables() {
				log.WithFields(t).Debug("Starting table")
				c.tracker.StartTable(c.Name, k.Name, t.Name, t.Total())

				for _, r := range t.Repairs() {

					if c.tracker.IsCompleted(c.Name, k.Name, t.Name, r.ID, c.interval()) {
						log.WithFields(r).Debug("Repair already completed")
						c.tracker.Skip(c.Name, k.Name, t.Name, r.ID)
						continue
					}

					log.WithFields(r).Debug("Scheduling fragment")
					jobs <- r
					c.tracker.Start(c.Name, k.Name, t.Name, r.ID)
					c.regulator.Limit(c.Name)
				}
			}
		}

		c.sleep()
	}
}

// ObtainBy given obtainer
func (c *Cluster) ObtainBy(o Obtainer) Scheduler {
	c.obtainer = o
	return c
}

// RegulateWith given rate limiter
func (c *Cluster) RegulateWith(r Regulator) Scheduler {
	c.regulator = r
	return c
}

// TrackIn given tracker
func (c *Cluster) TrackIn(t Tracker) Scheduler {
	c.tracker = t
	return c
}
func (c *Cluster) interval() time.Duration {
	duration, err := time.ParseDuration(c.Interval)
	if err != nil {
		log.WithFields(c).WithError(err).Warn("Duration parsing error")
		duration = week
	}
	return duration
}

func (c *Cluster) obtainKeyspaces() ([]*Keyspace, int) {
	total := 0
	var result []*Keyspace

	for _, k := range c.Keyspaces {
		tables, err := c.obtainer.ObtainTables(c.Name, k.Name)
		if err != nil {
			log.WithError(err).Warn("Tables obtain error")
			continue
		}
		keyspaceTotal := 0

		for _, t := range tables {
			fragments, err := c.obtainer.ObtainFragments(c.Name, k.Name, t.Slices)
			if err != nil {
				log.WithError(err).Warn("Fragments obtain error")
				continue
			}
			var repairs []*Repair
			for _, f := range fragments {
				r := &Repair{
					ID:       f.ID,
					Start:    f.Start,
					End:      f.End,
					Endpoint: f.Endpoint,
					Cluster:  c.Name,
					Keyspace: k.Name,
					Table:    t.Name,
				}
				repairs = append(repairs, r)
			}
			tableTotal := len(repairs)
			total += tableTotal
			keyspaceTotal += tableTotal

			t.SetRepairs(repairs)
			t.SetTotal(tableTotal)
		}
		k.SetTables(tables)
		k.SetTotal(keyspaceTotal)
		result = append(result, k)
	}
	return result, total
}

func (c *Cluster) sleep() {
	duration := c.interval()
	log.WithFields(c).Debug(fmt.Sprintf("Cluster scheduled. Going to sleep for: %s", duration))
	time.Sleep(duration)
}
