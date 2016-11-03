package schedule

import (
	"fmt"
	"os"
	"time"

	"github.com/skbkontur/cagrr"
	"github.com/skbkontur/cagrr/http"
	"github.com/skbkontur/cagrr/ops"
	"github.com/skbkontur/cagrr/repair"
)

var (
	log ops.Logger
	reg ops.Regulator
)

// Scheduler creates jobs in time
type Scheduler interface {
	Using(obtainer http.Obtainer) Scheduler
	OnClusters(clusters []cagrr.ClusterConfig) Scheduler
	ReturnTo(callback string) Scheduler
	Reschedule(fails <-chan http.Status) Scheduler
	To(jobs chan<- repair.Runner) Scheduler
	ScheduleFor(interval string) Scheduler
	Forever()
}

type scheduler struct {
	schedule chan repair.Runner
	fails    <-chan http.Status
	jobs     chan<- repair.Runner
	Obtainer http.Obtainer
	Callback string
	duration time.Duration
	Clusters []cagrr.ClusterConfig
}

// NewScheduler initializes loops for scheduling repair jobs
func NewScheduler(logger ops.Logger, regulator ops.Regulator) Scheduler {
	log = logger
	reg = regulator
	return scheduler{
		schedule: make(chan repair.Runner, 5),
	}
}

func (s scheduler) OnClusters(clusters []cagrr.ClusterConfig) Scheduler {
	s.Clusters = clusters
	return s
}

func (s scheduler) Using(obtainer http.Obtainer) Scheduler {
	s.Obtainer = obtainer
	return s
}

func (s scheduler) ReturnTo(callback string) Scheduler {
	s.Callback = callback
	return s
}

func (s scheduler) ScheduleFor(interval string) Scheduler {
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

func (s scheduler) scheduleAll() {
	callback := fmt.Sprintf("http://%s/status", s.Callback)
	for {
		for cid, cluster := range s.Clusters {
			log.WithFields(cluster).Debug("Starting schedule cluster")

			for _, keyspace := range cluster.Keyspaces {
				log.Debug(fmt.Sprintf("Starting schedule keyspace: %s", keyspace))
				fragments, err := s.Obtainer.Obtain(keyspace, callback, cid)
				if err == nil {
					for _, frag := range fragments {
						if frag != nil {
							reg.Limit()
							log.WithFields(frag).Debug("Fragment planning")
							s.schedule <- frag
						}
					}
				} else {
					log.WithError(err).Error("Ring obtain error")
				}
			}
		}
		log.Info(fmt.Sprintf("Sleeping before new repair (%s)", s.duration))
		time.Sleep(s.duration)
	}
}

func (s scheduler) Reschedule(fails <-chan http.Status) Scheduler {
	s.fails = fails
	return s
}

func (s scheduler) To(jobs chan<- repair.Runner) Scheduler {
	s.jobs = jobs
	return s
}

func (s scheduler) Forever() {
	for {
		select {
		case job := <-s.schedule:
			log.WithFields(job).Debug("received job from schedule")
			s.jobs <- job
		case fail := <-s.fails:
			log.WithFields(fail).Debug("received fail")
			s.jobs <- &fail.Repair
		default:
			log.Debug("no activity")
			time.Sleep(time.Second * 15)
		}
	}
}
