package main

import (
	"fmt"
	"io"
	"os"

	nethttp "net/http"
	_ "net/http/pprof"

	"github.com/jessevdk/go-flags"
	"github.com/skbkontur/cagrr"
)

var version = "devel"

var opts struct {
	Verbosity     string `short:"v" long:"verbosity" default:"debug" description:"Verbosity of tool, possible values are: panic, fatal, error, waring, debug"`
	ListenAddress string `short:"a" long:"listen" default:"localhost:8888" description:"host:port string of listen address for repair callbacks"`
	LogFile       string `short:"l" long:"log" default:"stdout" description:"Log file name"`
	ConfigFile    string `short:"c" long:"config" default:"/etc/cagrr/config.yml" description:"Configuration file name"`
	Version       bool   `long:"version" description:"Show version info and exit"`
}

// in/out streams
var (
	in  io.Reader = os.Stdin
	out io.Writer = os.Stdout
)

// subject dependencies
var (
	logger  cagrr.Logger
	repairs chan *cagrr.Repair
)

func main() {
	config, err := cagrr.ReadConfiguration(opts.ConfigFile)
	if err != nil {
		logger.WithError(err).Error("Error when reading configuration")
		os.Exit(1)
	}

	repairs = make(chan *cagrr.Repair, len(config.Clusters)*2)
	//bolt := cagrr.NewBoltDb("/tmp/cagrr.db")
	consul := cagrr.NewConsulDb()
	//redis := cagrr.NewRedisDb("localhost:6379")
	database := consul
	regulator := cagrr.NewRegulator(config.BufferLength)
	tracker := cagrr.NewTracker(consul, regulator)
	server := cagrr.NewServer(tracker)
	fixer := cagrr.NewFixer(config.Conn)

	defer database.Close()

	server.ServeAt(opts.ListenAddress)

	for _, cluster := range config.Clusters {
		go cluster.
			ObtainBy(config.Conn).
			RegulateWith(regulator).
			TrackIn(tracker).
			Schedule(repairs)
	}

	fixer.Fix(repairs)

}

func init() {
	flags.Parse(&opts)
	checkVersion()

	logger = cagrr.NewLogger(opts.Verbosity, opts.LogFile)

	if opts.Verbosity == "debug" {
		go startProfiling()
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
