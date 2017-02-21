package cagrr

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"
)

// Fix repairs from channel
func (c *Cluster) Fix(jobs <-chan *Repair) {
	log.WithFields(c).Debug("Starting fix loop")
	for job := range jobs {
		err := c.RunRepair(job)
		if err != nil {
			log.WithError(err).WithFields(job).Warn("Fail to start job")
		}
	}
}

// RegulateWith given rate limiter
func (c *Cluster) RegulateWith(r Regulator) Scheduler {
	c.regulator = r
	return c
}

// RunRepair runs fragment repair
func (c *Cluster) RunRepair(repair *Repair) error {
	url := fmt.Sprintf("http://%s:%d/repair", c.Host, c.Port)

	log.WithFields(repair).Info("Starting repair job")

	buf, _ := json.Marshal(repair)
	body := bytes.NewBuffer(buf)
	res, err := http.Post(url, "application/json", body)
	if res != nil {
		defer res.Body.Close()
		if res.StatusCode != 200 {
			log.WithError(err).WithFields(repair).Error("Fail to run repair")
		}
	}

	return err

}

// Schedule cluster repair
func (c *Cluster) Schedule(jobs chan *Repair) {

	for {
		log.WithFields(c).Debug("Starting cluster")
		keyspaces, total := c.keyspaces()
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

// TrackIn given tracker
func (c *Cluster) TrackIn(t Tracker) Scheduler {
	c.tracker = t
	return c
}

// Until sets chan for done event
func (c *Cluster) Until(done chan bool) Scheduler {
	c.done = done
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

func (c *Cluster) keyspaces() ([]*Keyspace, int) {
	total := 0
	var result []*Keyspace

	for _, k := range c.Keyspaces {
		tables, err := c.tables(k.Name)
		if err != nil {
			log.WithError(err).Warn("Tables obtain error")
			continue
		}
		keyspaceTotal := 0

		for _, t := range tables {
			fragments, err := c.fragments(k.Name, t.Slices)
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

func (c *Cluster) fragments(keyspace string, slices int) ([]*Fragment, error) {
	tokens, err := c.tokens(keyspace, slices)
	if err != nil {
		log.WithError(err).Error("Token obtain error")
		return nil, err
	}

	count := len(tokens) * slices
	frags := make([]*Fragment, 0, count)
	for _, token := range tokens {
		for _, frag := range token.Ranges {
			fragLink := frag
			frags = append(frags, &fragLink)
		}
	}
	return frags, nil
}

// Tables returns list of column family in clusters keyspace
func (c *Cluster) tables(keyspace string) ([]*Table, error) {
	var result []*Table
	url := fmt.Sprintf("http://%s:%d/tables/%s", c.Host, c.Port, keyspace)
	log.Debug(fmt.Sprintf("URL: %s", url))

	resp, err := http.Get(url)
	if err != nil {
		log.WithError(err).Error("Failed to obtain column families")
	}

	if resp != nil {
		defer resp.Body.Close()

		response, _ := ioutil.ReadAll(resp.Body)
		err = json.Unmarshal(response, &result)

		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (c *Cluster) tokens(keyspace string, slices int) (TokenSet, error) {
	var tokens TokenSet
	url := fmt.Sprintf("http://%s:%d/ring/%s/%d", c.Host, c.Port, keyspace, slices)
	res, err := http.Get(url)
	if err != nil {
		log.WithError(err).Error("Failed to obtain ring description")
	}

	if res != nil {
		defer res.Body.Close()
		response, _ := ioutil.ReadAll(res.Body)
		err = json.Unmarshal(response, &tokens)
	}
	return tokens, err
}
