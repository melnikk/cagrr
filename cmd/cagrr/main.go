package main

import (
	"fmt"
	"os"

	nethttp "net/http"
	_ "net/http/pprof"

	"github.com/jessevdk/go-flags"
	"github.com/skbkontur/cagrr"
	"github.com/skbkontur/cagrr/config"
	"github.com/skbkontur/cagrr/db"
	"github.com/skbkontur/cagrr/http"
	"github.com/skbkontur/cagrr/ops"
	"github.com/skbkontur/cagrr/repair"
)

var version = "devel"

var opts struct {
	Host       string `short:"h" long:"host" default:"localhost" description:"Address of CAJRR service"`
	Port       int    `short:"p" long:"port" default:"8080" description:"CAJRR port"`
	Workers    int    `short:"w" long:"workers" default:"1" description:"Number of concurrent workers"`
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
	database db.DB
	fixer    repair.Fixer
	obtainer repair.Obtainer
	server   http.Server
)

func main() {
	done := make(chan bool)
	sig := make(chan os.Signal, 1)

	defer database.Close()

	go server.
		At(opts.Callback).
		Serve()

	for cid, cluster := range configuration.Clusters {
		cluster.ID = cid
		go func(cluster cagrr.Cluster) {
			scheduler := repair.NewScheduler(regulator)
			scheduler.
				Using(obtainer).
				SaveTo(database).
				ReturnTo(opts.Callback).
				SetInterval(opts.Duration).
				OnCluster(cluster).
				Until(done, sig)
		}(cluster)
	}
	<-sig
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

	db.SetLogger(logger)
	database = db.NewDb("/tmp/cagrr.db")

	regulator = ops.NewRegulator(bufferLength)
	repair.SetLogger(logger)
	repair.SetRegulator(regulator)

	server = http.CreateServer(logger)

	obtainer = repair.NewObtainer(configuration.Host, configuration.Port)

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
