package main

import (
	"fmt"
	"os"

	"github.com/jessevdk/go-flags"
	"github.com/skbkontur/cagrr"
	"github.com/skbkontur/cagrr/config"
	"github.com/skbkontur/cagrr/http"
	"github.com/skbkontur/cagrr/ops"
	"github.com/skbkontur/cagrr/repair"
	"github.com/skbkontur/cagrr/report"
	"github.com/skbkontur/cagrr/schedule"
)

var version = "devel"

var opts struct {
	Host       string `short:"h" long:"host" default:"localhost" description:"Address of CAJRR service" env:"CAJRR_HOST"`
	Port       int    `short:"p" long:"port" default:"8080" description:"CAJRR port" env:"CAJRR_PORT"`
	Index      string `short:"i" long:"index" default:"cagrr-*" description:"Index in Elasticsearch" env:"REPAIR_INDEX"`
	App        string `short:"a" long:"app" default:"cagrr" description:"repair process cause app" env:"REPAIR_CAUSE"`
	Workers    int    `short:"w" long:"workers" default:"4" description:"Number of concurrent workers" env:"REPAIR_WORKERS"`
	Duration   string `short:"d" long:"duration" default:"1w" description:"Interval of full-repair" env:"REPAIR_INTERVAL"`
	Verbosity  string `short:"v" long:"verbosity" default:"debug" description:"Verbosity of tool, possible values are: panic, fatal, error, waring, debug" env:"REPAIR_VERBOSITY"`
	Callback   string `short:"c" long:"callback" default:"localhost:8888" description:"host:port string of listen address for repair callbacks" env:"CALLBACK_LISTEN"`
	ConfigFile string `long:"config" default:"/etc/cagrr/config.yml" description:"Configuration file name"`
	Version    bool   `long:"version" description:"Show version info and exit"`
}

// in/out streams
var (
	out = os.Stdout
	in  = os.Stdin
)

var configuration config.Config

// ops dependencies
var (
	logger    ops.Logger
	meter     ops.Meter
	regulator ops.Regulator
)

// subject dependencies
var (
	fixer       repair.Fixer
	registrator http.Registrator
	obtainer    http.Obtainer
	server      http.Server
	scheduler   schedule.Scheduler
	reporter    report.Reporter
)

func main() {
	jobs := make(chan repair.Runner, opts.Workers)
	wins := make(chan http.Status, opts.Workers)
	fails := make(chan http.Status, opts.Workers)

	go server.
		At(opts.Callback).
		Through(wins, fails).
		Serve()

	go scheduler.
		OnClusters(configuration.Clusters).
		Using(obtainer).
		ReturnTo(opts.Callback).
		ScheduleFor(opts.Duration).
		Reschedule(fails).To(jobs).
		Forever()

	go fixer.
		Fix(jobs)

	for win := range wins {
		reporter.Report(win)
	}
}

func init() {
	flags.Parse(&opts)
	checkVersion()
	// pinfold
	logger = ops.CreateLogger(opts.Verbosity, opts.Index, opts.App)

	var err error
	configuration, err = config.CreateReader().Read(opts.ConfigFile)
	if err != nil {
		logger.WithError(err).Error("Error when reading configuration")
	}

	meter = ops.CreateMeter(&logger)
	server = http.CreateServer(logger)
	scheduler = schedule.CreateScheduler(logger)
	fixer = repair.CreateFixer(logger)
	reporter = report.CreateReporter(logger)
	ring := cagrr.Ring{
		Host: opts.Host,
		Port: opts.Port,
	}
	obtainer = http.Obtainer(ring)

}

func checkVersion() {
	if opts.Version {
		fmt.Fprintln(out, version)
		os.Exit(0)
	}
}
