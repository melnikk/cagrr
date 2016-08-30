package main

import (
	"flag"
	"fmt"
	"github.com/Sirupsen/logrus"
    	"github.com/sohlich/elogrus"
    	"gopkg.in/olivere/elastic.v3"
	//graphite "github.com/go-kit/kit/metrics/graphite"
	"github.com/melnikk/cagrr/lib"
	"os"
)

var host = flag.String("h", "localhost", "Address of a node in cluster")
var port = flag.Int("p", 7199, "JMX port on a node")
var keyspace = flag.String("k", "all", "Keyspace to repair")
var steps = flag.Int("s", 100, "Steps to split token ranges to")
var workers = flag.Int("w", 1, "Number of concurrent workers")



var counter int = 0
var log *logrus.Logger

func fragmentGenerator(node cluster.Node, jobs chan<- cluster.Fragment, results chan<- string) {
	log.Infoln("Entering Fragment generator")
	tokens, keys := node.Tokens()

	for k := range keys {
		t := tokens[int64(k)]
		frags := t.Fragments(*steps)
		for _, f := range frags {
			log.Infoln("generated fragment ", fmt.Sprintf("[%d:%d]", f.Start, f.Finish))
			jobs <- f
		}
	}
}

func repairFragment(wid int, fragments <-chan cluster.Fragment, results chan<- string) {
	log.Infoln("Starting worker", wid)
	for f := range fragments {
		log.Infoln(
			"worker", wid,
			"processing fragment", fmt.Sprintf("[%d:%d]", f.Start, f.Finish),
			"with keyspace", *keyspace)
		str, err := f.Repair(*keyspace)
		if err != nil {
			log.Panic(err)
			panic(err)
		}
		counter ++
		results <- str
	}
}

func processResult(rid int, result string) {
	log.Infoln(fmt.Sprintf("Result [%d]: %s", rid, result))
}

func repair(node cluster.Node) {
	log.Infoln("Repairing node", node.Host)
	w := *workers
	jobs := make(chan cluster.Fragment, w)
	results := make(chan string, w)

	log.Infoln("Starting goroutines")

	for w := 1; w <= *workers; w++ {
		go repairFragment(w, jobs, results)
	}

	go fragmentGenerator(node, jobs, results)

	for i :=0; i<counter; i++  {
		 <- results
		//processResult(counter, res)
	}
}

func main() {
	flag.Parse()

	log := logrus.New()
    	client, err := elastic.NewClient(elastic.SetURL(os.Getenv("ELASTICSEARCH_URL")))
    	if err != nil {
        	log.Panic(err)
    	}
    	hook, err := elogrus.NewElasticHook(client, "localhost", logrus.DebugLevel, "mylog")
    	if err != nil {
        	log.Panic(err)
    	}
    	log.Hooks.Add(hook)

    	log.WithFields(logrus.Fields{
        	"name": "joe",
        	"age":  42,
    	}).Error("Hello world!")


	node := cluster.Node{Host: *host, Port: *port}
	repair(node)
}
