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
	week = time.Hour * 160
)

// NewScheduler initializes loops for scheduling repair jobs
func NewScheduler(conn Connector, clusters []Cluster) Scheduler {
	s := scheduler{
		obtainer:  &conn,
		clusters:  clusters,
		regulator: NewRegulator(5),
	}
	return &s
}
func (s *scheduler) Schedule(jobs chan Repair) {
	go s.startScheduling(jobs)
}

func (s *scheduler) ServeAt(callback string) Scheduler {
	go s.startServer(callback)
	return s
}

func (s *scheduler) TrackTo(database DB) Scheduler {
	s.db = database
	return s
}

func (s *scheduler) startServer(callback string) {
	for {
		log.Info(fmt.Sprintf("Server listen at %s", s.callback))

		s.mux = http.NewServeMux()
		s.mux.Handle("/status", http.HandlerFunc(s.repairStatus))
		s.mux.Handle("/nav", http.HandlerFunc(s.navigate))
		log.Fatal(http.ListenAndServe(s.callback, s.mux))
	}
}

func (s *scheduler) startScheduling(jobs chan Repair) {
	s.jobs = jobs
	callback := fmt.Sprintf("http://%s/status", s.callback)

	for _, cluster := range s.clusters {
		log.WithFields(cluster).Debug("Starting schedule cluster")

		for _, keyspace := range cluster.Keyspaces {
			log.Debug(fmt.Sprintf("Starting schedule keyspace: %s", keyspace.Name))

			fragments, tables, err := s.obtainer.Obtain(keyspace.Name, cluster.Name, keyspace.Slices)
			if err != nil {
				log.WithError(err).Warn("Ring obtain error")
			}

			for _, cfName := range tables {
				log.Debug(fmt.Sprintf("Starting schedule column families: %s", cfName))

				for _, frag := range fragments {
					s.regulator.Limit()
					rep := Repair{
						ID:       frag.ID,
						Start:    frag.Start,
						End:      frag.End,
						Endpoint: frag.Endpoint,
						Table:    cfName,
						Callback: callback,
					}
					jobs <- rep
				}
			}

		}
		duration, err := time.ParseDuration(cluster.Interval)
		if err != nil {
			log.WithError(err).Warn("Duration parsing error")
			duration = week
		}
		log.Debug(fmt.Sprintf("Cluster scheduling complete. Going to sleep for: %s", time.Duration(duration)))
		time.Sleep(duration)
	}
}

func (s *scheduler) navigate(w http.ResponseWriter, req *http.Request) {
}

func (s *scheduler) repairStatus(w http.ResponseWriter, req *http.Request) {
	body, _ := ioutil.ReadAll(req.Body)
	var status RepairStatus
	err := json.Unmarshal(body, &status)
	if err != nil {
		log.WithError(err).Warn(fmt.Sprintf("Invalid status received: %s", string(body)))
	}
	s.trackStatus(status)
}

func (s *scheduler) trackStatus(status RepairStatus) error {
	var err error
	var repair = status.Repair
	switch status.Type {
	case "COMPLETE":
		stats := getTrackTable(repair.Cluster, repair.Keyspace, repair.Table)
		duration := stats.finish(status.Repair.ID)
		logStats := stats.statistics()
		log.WithFields(logStats).Info("Fragment completed")

		s.regulator.LimitRateTo(duration)
	case "ERROR":
		err = errors.New("Error in cajrr")
		s.processFail(status)
	}
	return err
}

func (s *scheduler) processFail(status RepairStatus) {
	repair := Repair{}
	s.jobs <- repair
}
