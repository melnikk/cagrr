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
func NewServer(tracker Tracker) Server {
	s := server{
		tracker: tracker,
	}
	return &s
}

func (s *server) ServeAt(callback string) Server {
	s.callback = callback
	go s.startServer()
	return s
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
	repair := &status.Repair
	cluster := repair.Cluster
	keyspace := repair.Keyspace
	table := repair.Table
	id := repair.ID

	stats := s.tracker.Complete(cluster, keyspace, table, id)
	log.WithFields(stats).Info(status.Message)

	if stats.ClusterPercent == 100 {
		duration := int64(stats.ClusterAverage) * int64(stats.ClusterCompleted)
		clusterStats := &ClusterStats{
			Cluster:         stats.Cluster,
			ClusterDuration: time.Duration(duration),
			LastSuccess:     time.Now(),
		}
		log.WithFields(clusterStats).Info("Cluster completed")
	}

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
