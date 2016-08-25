package main

import (
	"flag"
	"fmt"

	"github.com/melnikk/cagrr/lib"
)

var host = flag.String("h", "localhost", "Address of a node in cluster")
var port = flag.Int("p", 9042, "JMX port on a node")
var keyspace = flag.String("k", "all", "Keyspace to repair")
var steps = flag.Int("s", 100, "Steps to split token ranges to")
var workers = flag.Int("w", 1, "Number of concurrent workers")

func fragmentGenerator(node cluster.Node, jobs chan<- cluster.Fragment) {
	tokens := node.Tokens()
	for _, t := range tokens {
		frags := t.Fragments(*steps)
		for _, f := range frags {
			fmt.Println("generated fragment ", fmt.Sprintf("[%d:%d]", f.Start, f.Finish))
			jobs <- f
		}
	}
}

func repairFragment(wid int, fragments <-chan cluster.Fragment) {
	for f := range fragments {
		fmt.Println(
			"worker", wid,
			"processing fragment", fmt.Sprintf("[%d:%d]", f.Start, f.Finish),
			"with keyspace", *keyspace)
		f.Repair(*keyspace)
	}
}

func repair(node cluster.Node) {
	fmt.Println("Repairing node", node.Host)
	jobs := make(chan cluster.Fragment, *workers)

	go fragmentGenerator(node, jobs)

	for w := 1; w <= *workers; w++ {
		go repairFragment(w, jobs)
	}
}

func main() {
	flag.Parse()
	node := cluster.Node{Host: *host, Port: *port}
	repair(node)
}
