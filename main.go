package cagrr

import (
	"flag"
	"net"
	"os"

	log "github.com/Sirupsen/logrus"
	"github.com/cyberdelia/go-metrics-graphite"
	metrics "github.com/rcrowley/go-metrics"
	"github.com/sohlich/elogrus"
	"gopkg.in/olivere/elastic.v3"
)

var host = flag.String("h", "localhost", "Address of a node in cluster")
var port = flag.Int("p", 7199, "JMX port on a node")
var keyspace = flag.String("k", "all", "Keyspace to repair")
var steps = flag.Int("s", 100, "Steps to split token ranges to")
var workers = flag.Int("w", 1, "Number of concurrent workers")
var verbosity = flag.String("v", "debug", "Verbosity of tool, possible values are: panic, fatal, error, waring, debug")

// Metrics of repair process
type Metrics struct {
	fragmentCount metrics.Counter
	errorCount    metrics.Counter
	repairTime    metrics.Meter
}

var instr Metrics

func fragmentGenerator(node Node, jobs chan<- Fragment) int {

	log.WithFields(log.Fields{
		"keyspace": *keyspace,
	}).Info("Fragment generator started")

	tokens, keys := node.Tokens()

	if len(keys) == 0 {
		log.WithFields(log.Fields{
			"keyspace": *keyspace,
		}).Error("Empty token ring")
	}

	counter := 0
	for _, k := range keys {
		t := tokens[k]
		log.WithFields(log.Fields{
			"token": t.ID,
			"next":  t.Next,
		}).Info("Generating fragments from token")

		frags := t.Fragments(*steps)
		for _, f := range frags {
			counter++
			f.ID = counter
			log.WithFields(log.Fields{
				"fid":      f.ID,
				"start":    f.Start,
				"finish":   f.Finish,
				"token":    t.ID,
				"keyspace": *keyspace,
			}).Info("Fragment generated")

			jobs <- f
		}
	}

	close(jobs)
	return counter
}

func repairFragment(wid int, fragments <-chan Fragment, results chan<- RepairResult) {

	log.WithFields(log.Fields{
		"worker": wid,
	}).Info("Worker started")

	for f := range fragments {
		logFields := log.Fields{
			"fid":      f.ID,
			"worker":   wid,
			"start":    f.Start,
			"finish":   f.Finish,
			"keyspace": *keyspace,
		}
		log.WithFields(logFields).Info("Processing fragment")

		res, err := f.Repair(*keyspace)
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

func processResult(result RepairResult) {

	log.WithFields(log.Fields{
		"fid":     result.Frag.ID,
		"message": result.Message,
	}).Info("Processing result")
}

func repair(node Node) {

	log.Info("Repairing node")

	w := *workers
	jobs := make(chan Fragment, w)
	results := make(chan RepairResult, w)

	go fragmentGenerator(node, jobs)

	for w := 1; w <= *workers; w++ {

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

	flag.Parse()

	node := Node{Host: *host, Port: *port}

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

func initLog(node Node) {
	url := os.Getenv("ELASTICSEARCH_URL")
	index := "cagrr"

	client, err := elastic.NewClient(elastic.SetURL(url))
	if err != nil {
		log.WithFields(log.Fields{
			"url": url,
		}).Panic(err)
	}
	level, _ := log.ParseLevel(*verbosity)
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
