package cagrr

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"
)

const (
	bufferLength = 10
	week         = time.Hour * 160
)

// NewScheduler initializes loops for scheduling repair jobs
func NewScheduler(conn *Connector, clusters []*Cluster) Scheduler {
	s := scheduler{
		navigation: &Navigation{},
		obtainer:   conn,
		clusters:   clusters,
		regulator:  NewRegulator(bufferLength),
	}
	return &s
}
func (s *scheduler) Schedule(jobs chan *Repair) {
	go s.startScheduling(jobs)
}

func (s *scheduler) ServeAt(callback string) Scheduler {
	s.callback = callback
	go s.startServer()
	return s
}

func (s *scheduler) findCluster(name string) *Cluster {
	for i, c := range s.clusters {
		if c.Name == name {
			return s.clusters[i]
		}
	}
	return s.clusters[0]
}

func (s *scheduler) handleNavigate(w http.ResponseWriter, req *http.Request) {
	var nav Navigation
	body, _ := ioutil.ReadAll(req.Body)
	var status RepairStatus
	err := json.Unmarshal(body, &status)
	if err != nil {
		log.WithError(err).Warn(fmt.Sprintf("Invalid navigation received: %s", string(body)))
	}
	s.navigation = &nav
}

func (s *scheduler) handleRepairStatus(w http.ResponseWriter, req *http.Request) {
	body, _ := ioutil.ReadAll(req.Body)
	var status RepairStatus
	err := json.Unmarshal(body, &status)
	if err != nil {
		log.WithError(err).Warn(fmt.Sprintf("Invalid status received: %s", string(body)))
	} else {
		s.trackStatus(status)
	}
}

func (s *scheduler) processComplete(status RepairStatus) {
	log.WithFields(status).Debug("Status received")
	repair := &status.Repair
	cluster := s.findCluster(repair.Cluster)
	keyspace := cluster.findKeyspace(repair.Keyspace)
	table := keyspace.findTable(repair.Table)
	repair = table.findRepair(repair.ID)

	duration := repair.RegisterFinish()
	s.regulator.LimitRateTo(repair.Cluster, duration)

	table.completeFragment(repair.ID)
	logstats := RepairStats{
		Cluster:            cluster.Name,
		Keyspace:           keyspace.Name,
		Table:              table.Name,
		Completed:          table.completed,
		Total:              table.total,
		LastClusterSuccess: cluster.LastSuccesfullRepairTime(),
		Percent:            table.percentage(),
		PercentCluster:     cluster.percentage(),
		PercentKeyspace:    keyspace.percentage(),
		Estimate:           fmt.Sprintf("%s", table.estimate()),
		EstimateCluster:    fmt.Sprintf("%s", cluster.estimate()),
		EstimateKeyspace:   fmt.Sprintf("%s", keyspace.estimate()),
	}
	log.WithFields(logstats).Info("Fragment completed")

}

func (s *scheduler) processFail(status RepairStatus) {
	repair := status.Repair
	repair.RegisterStart()
	s.jobs <- &repair
}

func (s *scheduler) scheduleCluster(cluster *Cluster) {
	log.WithFields(cluster).Debug("Starting schedule cluster")
	cluster.RegisterStart()

	for _, keyspace := range cluster.Keyspaces {
		s.scheduleKeyspace(cluster.Name, keyspace)
	}

	duration, err := time.ParseDuration(cluster.Interval)
	if err != nil {
		log.WithError(err).Warn("Duration parsing error")
		duration = week
	}

	cluster.RegisterFinish()
	log.Debug(fmt.Sprintf("Cluster (%s) scheduling complete. Going to sleep for: %s", cluster.Name, time.Duration(duration)))
	time.Sleep(duration)
}

func (s *scheduler) scheduleKeyspace(cluster string, keyspace *Keyspace) {
	log.Debug(fmt.Sprintf("Starting schedule keyspace: %s", keyspace.Name))
	fragments, tables, err := s.obtainer.Obtain(cluster, keyspace.Name, keyspace.Slices)

	keyspace.RegisterStart(tables)

	if err != nil {
		log.WithError(err).Warn("Ring obtain error")
	}

	for _, cf := range tables {
		s.scheduleTable(cf, fragments)
	}

	keyspace.RegisterFinish()
	log.Debug(fmt.Sprintf("Keyspace (%s) scheduling completed", keyspace.Name))
}

func (s *scheduler) scheduleTable(table *Table, fragments []*Fragment) {
	log.Debug(fmt.Sprintf("Starting schedule table: %s", table.Name))
	table.RegisterStart(int32(len(fragments)))

	callback := fmt.Sprintf("http://%s/status", s.callback)

	for _, frag := range fragments {
		if !frag.needToRepair() {
			log.WithFields(frag).Debug("Fragment already scheduled. Skipping...")
			table.completeFragment(frag.ID)
			continue
		}

		s.regulator.Limit(frag.cluster)

		rep := frag.createRepair(table, callback)
		rep.RegisterStart()
		table.repairs[rep.ID] = rep
		s.jobs <- rep

		frag.savePosition()
	}

	table.RegisterFinish()
	log.Debug(fmt.Sprintf("Table (%s) scheduling finished", table.Name))
}

func (s *scheduler) startServer() {
	for {
		log.Info(fmt.Sprintf("Server listen at %s", s.callback))

		s.mux = http.NewServeMux()
		s.mux.Handle("/status", http.HandlerFunc(s.handleRepairStatus))
		s.mux.Handle("/nav", http.HandlerFunc(s.handleNavigate))
		log.Fatal(http.ListenAndServe(s.callback, s.mux))
	}
}

func (s *scheduler) startScheduling(jobs chan *Repair) {
	s.jobs = jobs

	for _, cluster := range s.clusters {
		go s.scheduleCluster(cluster)
	}
}

func (s *scheduler) trackStatus(status RepairStatus) error {
	var err error
	switch status.Type {
	case "COMPLETE":
		s.processComplete(status)
	case "ERROR":
		err = errors.New("Error in cajrr")
		s.processFail(status)
	}
	return err
}
