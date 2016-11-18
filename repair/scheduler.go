package repair

import (
	"fmt"
	"os"
	"time"

	"github.com/skbkontur/cagrr"
	"github.com/skbkontur/cagrr/db"
	"github.com/skbkontur/cagrr/ops"
)

var ()

// Scheduler creates jobs in time
type Scheduler interface {
	Using(obtainer Obtainer) Scheduler
	ReturnTo(callback string) Scheduler
	Reschedule(fails <-chan Runner) Scheduler
	SetInterval(interval string) Scheduler
	OnCluster(cluster cagrr.ClusterConfig) Scheduler
	SaveTo(db.DB) Scheduler
	Until(chan bool, chan os.Signal)
}

type scheduler struct {
	db        db.DB
	regulator ops.Regulator
	fails     <-chan Runner
	cluster   cagrr.ClusterConfig
	obtainer  Obtainer
	callback  string
	duration  time.Duration
}

// NewScheduler initializes loops for scheduling repair jobs
func NewScheduler(regulator ops.Regulator) Scheduler {
	result := scheduler{}
	result.regulator = regulator
	return &result
}

func (s *scheduler) OnCluster(cluster cagrr.ClusterConfig) Scheduler {
	s.cluster = cluster
	return s
}
func (s *scheduler) SaveTo(database db.DB) Scheduler {
	s.db = database
	return s
}

func (s *scheduler) Using(obtainer Obtainer) Scheduler {
	s.obtainer = obtainer
	return s
}

func (s *scheduler) ReturnTo(callback string) Scheduler {
	s.callback = callback
	return s
}

func (s *scheduler) SetInterval(interval string) Scheduler {
	log.Debug(fmt.Sprintf("Init schedule loop (duration: %s)", interval))

	duration, err := time.ParseDuration(interval)
	if err != nil {
		log.WithError(err).Error("Cannot parse schedule interval")
		os.Exit(1)
	}
	s.duration = duration
	go s.scheduleAll()
	return s
}

func (s *scheduler) scheduleAll() {
	callback := fmt.Sprintf("http://%s/status", s.callback)
	for {
		log.WithFields(s.cluster).Debug("Starting schedule cluster")

		for _, keyspace := range s.cluster.Keyspaces {
			log.Debug(fmt.Sprintf("Starting schedule keyspace: %s", keyspace.Name))

			for _, cf := range keyspace.Tables {
				log.Debug(fmt.Sprintf("Starting schedule column families: %s", cf.Name))
				fragments, err := s.obtainer.Obtain(keyspace.Name, callback, s.cluster.Name, cf.Slices)
				if err != nil {
					log.WithError(err).Error("Ring obtain error")
				}

				fixer := NewFixer(s.db, &s.cluster, &keyspace, &cf, len(fragments))
				jobs := make(chan Runner)
				go fixer.Fix(jobs)

				for _, frag := range fragments {
					s.regulator.Limit()
					jobs <- frag
				}
			}

		}
		log.Info(fmt.Sprintf("Sleeping before new repair (%s)", s.duration))
		time.Sleep(s.duration)
	}
}

func (s *scheduler) Reschedule(fails <-chan Runner) Scheduler {
	s.fails = fails
	return s
}

func (s *scheduler) Until(done chan bool, sig chan os.Signal) {
	for {
		select {
		case fail := <-s.fails:
			s.rescheduleFail(fail)
		case <-done:
			return
		default:
			log.Debug("no activity")
			time.Sleep(time.Second * 15)
		}
	}
}

func (s *scheduler) rescheduleFail(fail Runner) {

}
