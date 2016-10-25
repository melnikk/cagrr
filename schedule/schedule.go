package schedule

import (
	"fmt"
	"time"

	"github.com/skbkontur/cagrr"
	"github.com/skbkontur/cagrr/http"
	"github.com/skbkontur/cagrr/ops"
	"github.com/skbkontur/cagrr/repair"
)

var (
	log ops.Logger
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
	Duration time.Duration
	Clusters []cagrr.ClusterConfig
}

// CreateScheduler initializes http listener
func CreateScheduler(logger ops.Logger) Scheduler {
	log = logger
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
	log.Debug("Init schedule loop")

	duration, _ := time.ParseDuration(interval)
	s.Duration = duration
	// when RepairStatus arrives then put in Reschedule Queue
	for cid, cluster := range s.Clusters {
		log.WithFields(cluster).Debug("Starting schedule cluster")
		go s.ScheduleCluster(cid, cluster)
	}
	return s
}

func (s scheduler) ScheduleCluster(cid int, cluster cagrr.ClusterConfig) {
	callback := fmt.Sprintf("http://%s/status", s.Callback)
	for _, keyspace := range cluster.Keyspaces {
		log.Debug(fmt.Sprintf("Starting schedule keyspace: %s", keyspace))
		fragments, err := s.Obtainer.Obtain(keyspace, callback, cid)
		if err == nil {
			for _, frag := range fragments {
				log.WithFields(frag).Debug("Fragment planning")
				s.schedule <- frag
			}
		} else {
			log.WithError(err).Error("Ring obtain error")
		}
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
			s.jobs <- fail.Repair
		default:
			log.Debug("no activity")
			time.Sleep(time.Second * 10)
		}
	}
}
