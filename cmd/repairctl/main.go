package main

import (
	"fmt"
	"os"

	nethttp "net/http"
	_ "net/http/pprof"

	"github.com/jessevdk/go-flags"
	"github.com/skbkontur/cagrr"
)

var version = "devel"

var opts struct {
	Verbosity  string `short:"v" long:"verbosity" default:"debug" description:"Verbosity of tool, possible values are: panic, fatal, error, waring, debug"`
	Callback   string `short:"c" long:"callback" default:"localhost:8888" description:"host:port string of listen address for repair callbacks"`
	LogFile    string `short:"l" long:"log" default:"stdout" description:"Log file name"`
	ConfigFile string `long:"config" default:"/etc/cagrr/config.yml" description:"Configuration file name"`
	Version    bool   `long:"version" description:"Show version info and exit"`
}

// in/out streams
var (
	out = os.Stdout
	in  = os.Stdin
)

// subject dependencies
var (
	repairs = make(chan cagrr.Repair)
	config  *cagrr.Config
	logger  cagrr.Logger
)

func main() {
	database := cagrr.NewDb("/tmp/cagrr.db")
	scheduler := cagrr.NewScheduler(config.Conn, config.Clusters)
	fixer := cagrr.NewFixer(&config.Conn)

	defer database.Close()

	scheduler.
		TrackTo(database).
		ServeAt(opts.Callback).
		Schedule(repairs)

	fixer.
		Fix(repairs)

}

func init() {
	flags.Parse(&opts)
	checkVersion()

	logger = cagrr.NewLogger(opts.Verbosity, opts.LogFile)
	if opts.Verbosity == "debug" {
		go startProfiling()
	}

	var err error
	config, err = cagrr.ReadConfiguration(opts.ConfigFile)
	if err != nil {
		logger.WithError(err).Error("Error when reading configuration")
		os.Exit(1)
	}

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
