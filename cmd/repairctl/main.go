package main

import (
	"fmt"
	"io"
	"os"

	_ "net/http/pprof"

	"github.com/jessevdk/go-flags"
)

var version = "devel"

var opts struct {
	Port    string `short:"p" long:"port" default:"8888" description:"Port of Cagrr service"`
	Version bool   `long:"version" description:"Show version info and exit"`
}

// in/out streams
var (
	out io.Writer = os.Stdout
	in  io.Reader = os.Stdin
)

func main() {
}

func init() {
	flags.Parse(&opts)
	checkVersion()
}

func checkVersion() {
	if opts.Version {
		fmt.Fprintln(out, version)
		os.Exit(0)
	}
}
