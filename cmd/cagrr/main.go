package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/fatih/structs"
	"github.com/jessevdk/go-flags"
	hook "github.com/melnikk/logrus-rabbitmq-hook"
	"github.com/skbkontur/cagrr"
)

var version = "devel"

var opts struct {
	Host      string `short:"h" long:"host" default:"localhost" description:"Address of CAJRR service" env:"CAJRR_HOST"`
	Port      int    `short:"p" long:"port" default:"8080" description:"CAJRR port" env:"CAJRR_PORT"`
	Index     string `short:"i" long:"index" default:"cagrr-*" description:"Index in Elasticsearch" env:"REPAIR_INDEX"`
	App       string `short:"a" long:"app" default:"cagrr" description:"repair process cause app" env:"REPAIR_CAUSE"`
	Workers   int    `short:"w" long:"workers" default:"1" description:"Number of concurrent workers" env:"REPAIR_WORKERS"`
	Duration  string `short:"d" long:"duration" default:"1w" description:"Interval of full-repair" env:"REPAIR_INTERVAL"`
	Callback  string `short:"c" long:"callback" default:"localhost:8888" description:"host:port string of listen address for repair callbacks" env:"CALLBACK_LISTEN"`
	Verbosity string `short:"v" long:"verbosity" default:"debug" description:"Verbosity of tool, possible values are: panic, fatal, error, waring, debug" env:"REPAIR_VERBOSITY"`
}

var ring cagrr.Ring

func main() {
	jobs := make(chan cagrr.Repair, opts.Workers)
	fails := make(chan cagrr.RepairStatus)
	wins := make(chan cagrr.RepairStatus)
	go listen(jobs, fails, wins)
	go schedule(jobs)
	go repair(jobs)
	go reschedule(fails, jobs)

	for win := range wins {
		report(win)
	}
}

func init() {
	flags.Parse(&opts)

	ring = cagrr.Ring{
		Host: opts.Host,
		Port: opts.Port,
	}

	level, _ := log.ParseLevel(opts.Verbosity)
	log.SetLevel(level)

	url := os.Getenv("LOG_STREAM_URL")
	if url != "" {
		hook := hook.New(opts.Index, url, opts.App, opts.App)
		log.AddHook(hook)
		log.WithFields(log.Fields{
			"url": url,
		}).Info("Started logger amqp hook")
	}
}

func listen(jobs chan<- cagrr.Repair, fails chan<- cagrr.RepairStatus, wins chan<- cagrr.RepairStatus) {
	for {
		log.Infof("Server listen at %s", opts.Callback)

		mux := http.NewServeMux()
		mux.Handle("/status", http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			body, _ := ioutil.ReadAll(req.Body)
			var status cagrr.RepairStatus
			var fail error
			err := json.Unmarshal(body, &status)
			if err == nil {
				status, fail = ring.RegisterStatus(status)
				if fail == nil {
					log.WithFields(log.Fields(structs.Map(status))).Debug("Repair suceeded")
					wins <- status
				} else {
					log.WithFields(log.Fields(structs.Map(status))).Warn("Fragment repair failed")
					fails <- status
				}
			} else {
				log.WithError(err).Warn("Invalid status received")
			}
		}))
		log.Fatal(http.ListenAndServe(opts.Callback, mux))
	}
}

func schedule(jobs chan<- cagrr.Repair) {
	log.Debug("Init schedule loop")

	keyspaces := []string{"testspace"}

	duration, _ := time.ParseDuration(opts.Duration)
	// when RepairStatus arrives then put in Reschedule Queue
	go func() {
		for {
			log.Debugf("Starting complete schedule")
			for _, keyspace := range keyspaces {
				log.Debugf("Entering keyspace: %s", keyspace)
				callback := fmt.Sprintf("http://%s/status", opts.Callback)
				fragments, err := ring.Repair(keyspace, callback)
				if err == nil {
					for _, frag := range fragments {
						log.WithFields(log.Fields(structs.Map(frag))).Debug("Fragment planning")
						jobs <- frag
					}
				} else {
					log.WithError(err).Error("Ring obtain error")
				}
			}

			log.Infof("Scheduling complete. Sleeping for interval %s", opts.Duration)
			time.Sleep(duration)
		}
	}()

}

func repair(jobs chan cagrr.Repair) {
	for {
		job := <-jobs
		err := job.Run(ring.Host, ring.Port, ring.Cluster)
		if err == nil {
			log.WithFields(log.Fields(structs.Map(job))).Debug("Repair started")
		} else {
			log.WithError(err).Warn("Failed to start repair")
		}
	}
}

func reschedule(fails <-chan cagrr.RepairStatus, jobs chan<- cagrr.Repair) {
	for {
		status := <-fails
		jobs <- status.Repair
		log.WithFields(log.Fields(structs.Map(status.Repair))).Info("Repair rescheduled after fail")
	}
}

func report(result cagrr.RepairStatus) {
	log.WithFields(log.Fields(structs.Map(result))).Info("Report received")
}
