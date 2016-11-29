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
	bufferLength = 500
	week         = time.Hour * 160
)

// NewScheduler initializes loops for scheduling repair jobs
func NewScheduler(conn Connector, clusters []Cluster) Scheduler {
	s := scheduler{
		obtainer:  &conn,
		clusters:  clusters,
		regulator: NewRegulator(bufferLength),
	}
	return &s
}
func (s *scheduler) Schedule(jobs chan Repair) {
	go s.startScheduling(jobs)
}

func (s *scheduler) ServeAt(callback string) Scheduler {
	s.callback = callback
	go s.startServer()
	return s
}

func (s *scheduler) TrackTo(database DB) Scheduler {
	s.db = database
	return s
}

func (s *scheduler) startServer() {
	for {
		log.Info(fmt.Sprintf("Server listen at %s", s.callback))

		s.mux = http.NewServeMux()
		s.mux.Handle("/status", http.HandlerFunc(s.repairStatus))
		s.mux.Handle("/nav", http.HandlerFunc(s.navigate))
		log.Fatal(http.ListenAndServe(s.callback, s.mux))
	}
}

func (s *scheduler) navigate(w http.ResponseWriter, req *http.Request) {
}

func (s *scheduler) processFail(status RepairStatus) {
	repair := Repair{}
	s.jobs <- repair
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
func (s *scheduler) startScheduling(jobs chan Repair) {
	s.jobs = jobs
	callback := fmt.Sprintf("http://%s/status", s.callback)

	for _, cluster := range s.clusters {
		log.WithFields(cluster).Debug("Starting schedule cluster")

		for _, keyspace := range cluster.Keyspaces {
			log.Debug(fmt.Sprintf("Starting schedule keyspace: %s", keyspace.Name))

			fragments, tables, err := s.obtainer.Obtain(cluster.Name, keyspace.Name, keyspace.Slices)
			if err != nil {
				log.WithError(err).Warn("Ring obtain error")
			}

			for _, cfName := range tables {
				log.Debug(fmt.Sprintf("Starting schedule column families: %s", cfName))

				StartTableTrack(cluster.Name, keyspace.Name, cfName, int32(len(fragments)))

				for _, frag := range fragments {
					s.regulator.Limit()
					rep := Repair{
						ID:       frag.ID,
						Start:    frag.Start,
						End:      frag.End,
						Endpoint: frag.Endpoint,
						Cluster:  cluster.Name,
						Keyspace: keyspace.Name,
						Table:    cfName,
						Callback: callback,
					}
					jobs <- rep

					StartRepairTrack(rep)
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
func (s *scheduler) trackStatus(status RepairStatus) error {
	var err error
	var repair = status.Repair
	switch status.Type {
	case "COMPLETE":
		duration, logStats := FinishRepairTrack(repair)
		log.WithFields(logStats).Info("Fragment completed")

		s.regulator.LimitRateTo(duration)
	case "ERROR":
		err = errors.New("Error in cajrr")
		s.processFail(status)
	}
	return err
}