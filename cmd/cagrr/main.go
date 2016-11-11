package main

import (
	"fmt"
	"os"

	nethttp "net/http"
	_ "net/http/pprof"

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
	Host       string `short:"h" long:"host" default:"localhost" description:"Address of CAJRR service"`
	Port       int    `short:"p" long:"port" default:"8080" description:"CAJRR port"`
	Workers    int    `short:"w" long:"workers" default:"4" description:"Number of concurrent workers"`
	Duration   string `short:"d" long:"duration" default:"160h" description:"Interval between repairs"`
	Verbosity  string `short:"v" long:"verbosity" default:"debug" description:"Verbosity of tool, possible values are: panic, fatal, error, waring, debug"`
	Callback   string `short:"c" long:"callback" default:"localhost:8888" description:"host:port string of listen address for repair callbacks"`
	LogFile    string `short:"l" long:"log" default:"stdout" description:"Log file name"`
	ConfigFile string `long:"config" default:"/etc/cagrr/config.yml" description:"Configuration file name"`
	Version    bool   `long:"version" description:"Show version info and exit"`
}

const (
	bufferLength = 5
)

// in/out streams
var (
	out = os.Stdout
	in  = os.Stdin
)

var configuration config.Config

// ops dependencies
var (
	logger    ops.Logger
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
		LimitRateWith(regulator).
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

	logger = ops.NewLogger(opts.Verbosity, opts.LogFile)
	if opts.Verbosity == "debug" {
		go startProfiling()
	}

	var err error
	configuration, err = config.CreateReader().Read(opts.ConfigFile)
	if err != nil {
		logger.WithError(err).Error("Error when reading configuration")
	}

	ring := cagrr.Ring{
		Host: opts.Host,
		Port: opts.Port,
	}
	regulator = ops.NewRegulator(logger, bufferLength)
	server = http.CreateServer(logger).WithCompleter(&ring)

	scheduler = schedule.NewScheduler(logger, regulator)
	fixer = repair.CreateFixer(logger)
	reporter = report.CreateReporter(logger)
	obtainer = http.Obtainer(&ring)

}

func startProfiling() {
	logger.Info(nethttp.ListenAndServe("localhost:6060", nil))
}

func checkVersion() {
	if opts.Version {
		fmt.Fprintln(out, version)
		os.Exit(0)
	}
}
