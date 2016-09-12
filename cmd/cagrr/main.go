package main

import (
	"net"
	"os"

	log "github.com/Sirupsen/logrus"
	"github.com/cyberdelia/go-metrics-graphite"
	flags "github.com/jessevdk/go-flags"
	"github.com/melnikk/cagrr"
	metrics "github.com/rcrowley/go-metrics"
	"github.com/sohlich/elogrus"
	"gopkg.in/olivere/elastic.v3"
)

var opts struct {
	Host      string `short:"h" long:"host" default:"localhost" description:"Address of a node in cluster" env:"CASSANDRA_HOST"`
	Port      int    `short:"p" long:"port" default:"7199" description:"Connector port on a node" env:"CASSANDRA_PORT"`
	Connector string `short:"c" long:"conn" default:"mx4j" description:"Connection type, possible values: mx4j (default), jolokia, nodetool" env:"CASSANDRA_CONNECTOR"`
	Keyspace  string `short:"k" long:"keyspace" default:"*" description:"Keyspace to repair" env:"REPAIR_KEYSPACE"`
	Steps     int    `short:"s" long:"steps" default:"1" description:"Steps to split token ranges to" env:"REPAIR_STEPS"`
	Workers   int    `short:"w" long:"workers" default:"1" description:"Number of concurrent workers" env:"REPAIR_WORKERS"`
	Verbosity string `short:"v" long:"verbosity" description:"Verbosity of tool, possible values are: panic, fatal, error, waring, debug" env:"REPAIR_VERBOSITY"`
}

// Metrics of repair process
type Metrics struct {
	fragmentCount metrics.Counter
	errorCount    metrics.Counter
	repairTime    metrics.Meter
}

var instr Metrics

func fragmentGenerator(node cagrr.Node, jobs chan<- cagrr.Fragment) int {

	log.WithFields(log.Fields{
		"keyspace": opts.Keyspace,
	}).Info("Fragment generator started")

	tokens, keys := node.Tokens()

	if len(keys) == 0 {
		log.WithFields(log.Fields{
			"keyspace": opts.Keyspace,
		}).Error("Empty token ring")
	}

	counter := 0
	for _, k := range keys {
		t := tokens[k]
		log.WithFields(log.Fields{
			"token": t.ID,
			"next":  t.Next,
		}).Info("Generating fragments from token")

		frags := t.Fragments(opts.Steps)
		for _, f := range frags {
			counter++
			f.ID = counter
			log.WithFields(log.Fields{
				"fid":      f.ID,
				"start":    f.Start,
				"finish":   f.Finish,
				"token":    t.ID,
				"keyspace": opts.Keyspace,
			}).Info("Fragment generated")

			jobs <- f
		}
	}

	close(jobs)
	return counter
}

func repairFragment(wid int, fragments <-chan cagrr.Fragment, results chan<- cagrr.RepairResult) {

	log.WithFields(log.Fields{
		"worker": wid,
	}).Info("Worker started")

	for f := range fragments {
		logFields := log.Fields{
			"fid":      f.ID,
			"worker":   wid,
			"start":    f.Start,
			"finish":   f.Finish,
			"keyspace": opts.Keyspace,
		}
		log.WithFields(logFields).Info("Processing fragment")

		res, err := f.Repair(opts.Keyspace)
		if err != nil {
			log.WithFields(logFields).Error(err)
			instr.errorCount.Inc(1)
		} else {
			instr.fragmentCount.Inc(1)
			instr.repairTime.Mark(1)
		}
		results <- res
	}
}

func processResult(result cagrr.RepairResult) {

	log.WithFields(log.Fields{
		"fid":     result.Frag.ID,
		"message": result.Message,
	}).Info("Processing result")
}

func repair(node cagrr.Node) {

	log.Info("Repairing node")

	w := opts.Workers
	jobs := make(chan cagrr.Fragment, w)
	results := make(chan cagrr.RepairResult, w)

	go fragmentGenerator(node, jobs)

	for w := 1; w <= opts.Workers; w++ {

		log.WithFields(log.Fields{
			"worker": w,
		}).Info("Starting worker")

		go repairFragment(w, jobs, results)
	}

	for res := range results {
		processResult(res)
	}
}

func main() {
	flags.Parse(&opts)

	node := cagrr.Node{Host: opts.Host, Port: opts.Port}

	initLog(node)
	initMetrics()

	repair(node)
}

func initMetrics() {
	addr, _ := net.ResolveTCPAddr("tcp", os.Getenv("GRAPHITE_URL"))
	go graphite.Graphite(metrics.DefaultRegistry, 10e9, "cagrr", addr)
	instr = Metrics{
		fragmentCount: metrics.NewRegisteredCounter("fragmentCount", metrics.DefaultRegistry),
		errorCount:    metrics.NewRegisteredCounter("errorCount", metrics.DefaultRegistry),
		repairTime:    metrics.NewRegisteredMeter("repairTime", metrics.DefaultRegistry),
	}
}

func initLog(node cagrr.Node) {
	url := os.Getenv("ELASTICSEARCH_URL")
	index := "cagrr"

	client, err := elastic.NewClient(elastic.SetURL(url))
	if err != nil {
		log.WithFields(log.Fields{
			"url": url,
		}).Panic(err)
	}
	level, _ := log.ParseLevel(opts.Verbosity)
	hook, err := elogrus.NewElasticHook(client, node.Host, level, index)
	if err != nil {
		log.WithFields(log.Fields{
			"url":   url,
			"index": index,
		}).Panic(err)
	}

	logger := log.New()
	logger.Hooks.Add(hook)
}
