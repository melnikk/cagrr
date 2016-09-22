package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"

	elastic "gopkg.in/olivere/elastic.v3"

	log "github.com/Sirupsen/logrus"
	"github.com/go-kit/kit/metrics/graphite"
	flags "github.com/jessevdk/go-flags"
	metrics "github.com/rcrowley/go-metrics"
	"github.com/skbkontur/cagrr"
	"github.com/sohlich/elogrus"
)

var opts struct {
	Host      string `short:"h" long:"host" default:"localhost" description:"Address of CAJRR service" env:"CAJRR_HOST"`
	Port      int    `short:"p" long:"port" default:"8080" description:"CAJRR port" env:"CAJRR_PORT"`
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
var int count

// check progress
func repairStatus(w http.ResponseWriter, req *http.Request) {
	body, _ := ioutil.ReadAll(req.Body)
	var status cagrr.RepairStatus
	err := json.Unmarshal(body, &status)
	if err == nil {
		percent := status.Count * 100 / status.Total
		fmt.Printf("\r%d/%d=%d%%", status.Count, status.Total, percent)
	} else {
		fmt.Println(err)
	}
}

func runServer() {
	for {
		http.HandleFunc("/status", repairStatus)
		log.Fatal(http.ListenAndServe("localhost:8000", nil))
	}
}

func repair(node cagrr.Node) {

	w := opts.Workers
	jobs := make(chan cagrr.Fragment, w)
	results := make(chan cagrr.RepairResult, w)

	count = jobGenerator(Steps)

	options := &cagrr.Repair{
		Keyspace: "testspace",
		Cause:    "I can",
		Owner:    "miller",
		Callback: "http://localhost:8000/status",
		Options: map[string]string{
			"parallelism": "parallel",
		},
	}

	ranges := getRanges()

	for _, r := range ranges {
		options.Options["ranges"] = r
		buf, _ := json.Marshal(options)
		body := bytes.NewBuffer(buf)

		r, _ := http.Post("http://localhost:8080/repair", "application/json", body)
		response, _ := ioutil.ReadAll(r.Body)
		var repair Repair
		err := json.Unmarshal(response, &repair)
		if err == nil {
			fmt.Println(repair)
		} else {
			fmt.Println("Error: ", err)
		}
	}

}

func main() {
	flags.Parse(&opts)

	go runServer()

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
