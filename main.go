package main

import (
	"flag"
	"fmt"
	"time"
)

var host = flag.String("h", "localhost", "Address of a node in cluster")
var port = flag.Int("p", 9042, "JMX port on a node")
var keyspace = flag.String("k", "all", "Keyspace to repair")
var steps = flag.Int("s", 100, "Steps to split token ranges to")
var workers = flag.Int("w", 1, "Number of concurrent workers")

type repairRange struct {
	id     string
	start  int64
	finish int64
	result chan bool
}

func fragmentGenerator(jobs chan<- repairRange) {

}

func repairFragment(id int, segments <-chan repairRange, results chan<- repairRange) {
	for j := range segments {
		fmt.Println("worker", id, "processing job", j)
		time.Sleep(time.Second)
		j.result <- true
		results <- j
	}
}

func repair() {
	jobs := make(chan repairRange, *workers)
	results := make(chan repairRange, *workers)
	for w := 1; w <= *workers; w++ {
		go repairFragment(w, jobs, results)
	}
	go fragmentGenerator(jobs)

	close(jobs)
	for a := 1; a <= 9; a++ {
		<-results
	}
}

func main() {
	flag.Parse()
	repair()
}
