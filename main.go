package main

import (
	"flag"
	"fmt"

	"github.com/melnikk/cagrr/lib"
)

var host = flag.String("h", "localhost", "Address of a node in cluster")
var port = flag.Int("p", 7199, "JMX port on a node")
var keyspace = flag.String("k", "all", "Keyspace to repair")
var steps = flag.Int("s", 100, "Steps to split token ranges to")
var workers = flag.Int("w", 1, "Number of concurrent workers")

func fragmentGenerator(node cluster.Node, jobs chan<- cluster.Fragment, results chan<- string) {
	fmt.Println("Entering Fragment generator")
	tokens := node.Tokens()
	fmt.Println(tokens)
	for _, t := range tokens {
		frags := t.Fragments(*steps)
		for _, f := range frags {
			fmt.Println("generated fragment ", fmt.Sprintf("[%d:%d]", f.Start, f.Finish))
			jobs <- f
		}
	}

	close(jobs)
	results <- "ok"
}

func repairFragment(wid int, fragments <-chan cluster.Fragment, results chan<- string) {
	fmt.Println("Starting worker", wid)
	for f := range fragments {
		fmt.Println(
			"worker", wid,
			"processing fragment", fmt.Sprintf("[%d:%d]", f.Start, f.Finish),
			"with keyspace", *keyspace)
		str, err := f.Repair(*keyspace)
		if err != nil {
			fmt.Println("error")
			panic(err)
		}
		results <- str
	}
}

func processResult(rid int, result string) {
	fmt.Println(fmt.Sprintf("Result [%d]: %s", rid, result))
}

func repair(node cluster.Node) {
	fmt.Println("Repairing node", node.Host)
	jobs := make(chan cluster.Fragment, *workers)
	results := make(chan string, *workers)

	fmt.Println("Starting goroutines")
	go fragmentGenerator(node, jobs, results)

	for w := 1; w <= *workers; w++ {
		go repairFragment(w, jobs, results)
	}

	counter := 0
	for res := range results {
		processResult(counter, res)
		counter++
	}
}

func main() {
	flag.Parse()
	node := cluster.Node{Host: *host, Port: *port}
	repair(node)
}
