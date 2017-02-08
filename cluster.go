package cagrr

import (
	"fmt"
	"time"
)

// Schedule cluster repair
func (c *Cluster) Schedule(jobs chan *Repair) {
	keyspaces, total := c.obtainKeyspaces()

	for {
		log.WithFields(c).Debug("Starting cluster")

		for _, k := range keyspaces {
			log.WithFields(k).Debug("Starting keyspace")

			for _, t := range k.Tables {
				log.WithFields(t).Debug("Starting table")

				for _, r := range t.Repairs {

					if c.tracker.IsCompleted(c.Name, k.Name, t.Name, r.ID) {
						log.WithFields(r).Debug("Repair already completed")
						continue
					}

					c.regulator.Limit(c.Name)

					log.WithFields(r).Debug("Scheduling fragment")
					jobs <- r

					c.tracker.Track(c.Name, k.Name, t.Name, r.ID, total)
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

func (c *Cluster) obtainKeyspaces() ([]*Keyspace, int) {
	total := 0
	var result []*Keyspace

	for _, k := range c.Keyspaces {
		tables, err := c.obtainer.ObtainTables(c.Name, k.Name)
		if err != nil {
			log.WithError(err).Warn("Tables obtain error")
			continue
		}

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

			t.Repairs = repairs
			tables = append(tables, t)
		}
		result = append(result, k)
	}
	return result, total
}

func (c *Cluster) sleep() {
	duration, err := time.ParseDuration(c.Interval)
	if err != nil {
		log.WithFields(c).WithError(err).Warn("Duration parsing error")
		duration = week
	}
	log.WithFields(c).Debug(fmt.Sprintf("Cluster scheduled. Going to sleep for: %s", duration))
	time.Sleep(duration)
}
