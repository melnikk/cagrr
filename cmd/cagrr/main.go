package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/user"
	"sync/atomic"

	log "github.com/Sirupsen/logrus"
	flags "github.com/jessevdk/go-flags"
	hook "github.com/melnikk/logrus-rabbitmq-hook"
	"github.com/skbkontur/cagrr"
)

var opts struct {
	Host      string  `short:"h" long:"host" default:"localhost" description:"Address of CAJRR service" env:"CAJRR_HOST"`
	Port      int     `short:"p" long:"port" default:"8080" description:"CAJRR port" env:"CAJRR_PORT"`
	Keyspace  string  `short:"k" long:"keyspace" default:"*" description:"Keyspace to repair" env:"REPAIR_KEYSPACE"`
	Steps     int     `short:"s" long:"steps" default:"1" description:"Steps to split token ranges to" env:"REPAIR_STEPS"`
	Workers   int     `short:"w" long:"workers" default:"1" description:"Number of concurrent workers" env:"REPAIR_WORKERS"`
	Intensity float32 `short:"i" long:"intensity" default:"1" description:"Intensity of repair" env:"REPAIR_INTENSITY"`
	Verbosity string  `short:"v" long:"verbosity" default:"debug" description:"Verbosity of tool, possible values are: panic, fatal, error, waring, debug" env:"REPAIR_VERBOSITY"`
	App       string  `short:"a" long:"app" default:"cagrr" description:"repair process cause app" env:"REPAIR_CAUSE"`
	Callback  string  `short:"c" long:"callback" default:"localhost:8888" description:"host:port string of listen address for repair callbacks" env:"CALLBACK_LISTEN"`
	From      int     `short:"f" long:"from" default:"0" description:"id of fragment to start repair from"`
}

var count = new(int32)
var repaired = new(int32)

func main() {
	flags.Parse(&opts)
	initLog()
	go server()
	repair()
}

func repair() {
	atomic.AddInt32(repaired, int32(opts.From))

	result := cagrr.Ring{
		Host: opts.Host,
		Port: opts.Port,
	}

	ring, _ := result.Get(opts.Steps)
	atomic.StoreInt32(count, int32(ring.Count()))
	owner, _ := user.Current()
	callback := fmt.Sprintf("http://%s/status", opts.Callback)

	log.WithFields(log.Fields{
		"keyspace": opts.Keyspace,
		"from":     opts.From,
		"host":     opts.Host,
		"cause":    opts.App,
		"owner":    owner.Name,
		"callback": callback,
	}).Info("Start repair ring")
	ring.Repair(opts.Keyspace, opts.From, opts.App, owner.Name, callback)
	defer fmt.Println()
}

func server() {
	for {
		log.Infof("Server listen at %s", opts.Callback)

		http.HandleFunc("/status", repairStatus)
		log.Fatal(http.ListenAndServe(opts.Callback, nil))
	}
}

func repairStatus(w http.ResponseWriter, req *http.Request) {
	body, _ := ioutil.ReadAll(req.Body)
	var status cagrr.RepairStatus
	err := json.Unmarshal(body, &status)
	if err == nil {
		totalFragments := atomic.LoadInt32(count)
		repairedFragments := atomic.LoadInt32(repaired)
		percent := repairedFragments * 100 / totalFragments

		log.WithFields(log.Fields{
			"count":             status.Count,
			"message":           status.Message,
			"session":           status.Session,
			"command":           status.Command,
			"start":             status.Start,
			"finish":            status.Finish,
			"keyspace":          status.Keyspace,
			"options":           status.Options,
			"duration":          status.Duration,
			"isError":           status.Error,
			"total":             status.Total,
			"type":              status.Type,
			"totalFragments":    totalFragments,
			"repairedFragments": repairedFragments,
			"percent":           percent,
		}).Debug("Range status received")

		if status.Type == "COMPLETE" {
			atomic.AddInt32(repaired, 1)
		}
	} else {
		log.Warn(err)
	}
}

func initLog() {
	url := os.Getenv("LOG_STREAM_URL")
	index := fmt.Sprintf("devops-%s", opts.App)

	level, _ := log.ParseLevel(opts.Verbosity)
	fmt.Println(opts.Verbosity, level)
	log.SetLevel(level)

	hook := hook.New(index, url, "devops", "devops")
	log.AddHook(hook)
	log.WithFields(log.Fields{
		"url": url,
	}).Info("Started logger amqp hook")
}
