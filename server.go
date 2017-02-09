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

// NewServer initializes loops for scheduling repair jobs
func NewServer(regulator Regulator, tracker Tracker) Server {
	s := server{
		navigation: &Navigation{},
		regulator:  regulator,
		tracker:    tracker,
	}
	return &s
}

func (s *server) ServeAt(callback string) Server {
	s.callback = callback
	go s.startServer()
	return s
}

func (s *server) findCluster(name string) *Cluster {
	for i, c := range s.clusters {
		if c.Name == name {
			return s.clusters[i]
		}
	}
	return s.clusters[0]
}

func (s *server) handleNavigate(w http.ResponseWriter, req *http.Request) {
	var nav Navigation
	body, _ := ioutil.ReadAll(req.Body)
	var status RepairStatus
	err := json.Unmarshal(body, &status)
	if err != nil {
		log.WithError(err).Warn(fmt.Sprintf("Invalid navigation received: %s", string(body)))
	}
	s.navigation = &nav
}

func (s *server) handleRepairStatus(w http.ResponseWriter, req *http.Request) {
	body, _ := ioutil.ReadAll(req.Body)
	var status RepairStatus
	err := json.Unmarshal(body, &status)
	if err != nil {
		log.WithError(err).Warn(fmt.Sprintf("Invalid status received: %s", string(body)))
	} else {
		s.trackStatus(status)
	}
}

func (s *server) processComplete(status RepairStatus) {
	log.WithFields(status).Debug("Status received")
	repair := &status.Repair
	cluster := repair.Cluster
	keyspace := repair.Keyspace
	table := repair.Table
	id := repair.ID

	duration := s.tracker.CompleteFragment(cluster, keyspace, table, id)
	log.WithFields(repair).Info(fmt.Sprintf("Fragment completed in %s", duration))

}

func (s *server) processFail(status RepairStatus) {
	repair := status.Repair
	s.tracker.Restart(repair.Cluster, repair.Keyspace, repair.Table, repair.ID)
	s.jobs <- &repair
}

func (s *server) startServer() {
	for {
		log.Info(fmt.Sprintf("Server listen at %s", s.callback))

		s.mux = http.NewServeMux()
		s.mux.Handle("/status", http.HandlerFunc(s.handleRepairStatus))
		s.mux.Handle("/nav", http.HandlerFunc(s.handleNavigate))
		log.Fatal(http.ListenAndServe(s.callback, s.mux))
	}
}

func (s *server) trackStatus(status RepairStatus) error {
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
