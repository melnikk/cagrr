package main

import (
	"flag"
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
var log *logrus.Logger = logrus.New()

func fragmentGenerator(node cluster.Node, jobs chan<- cluster.Fragment) {

	log.WithFields(logrus.Fields{
		"keyspace": *keyspace,
		}).Info("Fragment generator started")

	tokens, keys := node.Tokens()

	if len(keys)==0 {
		log.WithFields(logrus.Fields{
			"keyspace": *keyspace,
			}).Error("Empty token ring")
	}

	for _, k := range keys {
		t := tokens[k]
		log.WithFields(logrus.Fields{
			"token": t.ID,
			"next": t.Next,
			}).Info("Generating fragments from token")

		frags := t.Fragments(*steps)
		for _, f := range frags {
			counter ++
			f.ID = counter
			log.WithFields(logrus.Fields{
				"fid": f.ID,
				"start": f.Start,
				"finish": f.Finish,
				"token": t.ID,
				"keyspace": *keyspace,
				}).Info("Fragment generated")

			jobs <- f
		}
	}


	close(jobs)
}

func repairFragment(wid int, fragments <-chan cluster.Fragment, results chan<- cluster.RepairResult) {

	log.WithFields(logrus.Fields{
        	"worker": wid,
    		}).Info("Worker started")

	for f := range fragments {
		logFields := logrus.Fields{
			"fid": f.ID,
			"worker": wid,
			"start": f.Start,
			"finish": f.Finish,
			"keyspace": *keyspace,
		}
		log.WithFields(logFields).Info("Processing fragment")

		res, err := f.Repair(*keyspace)
		if err != nil {
			log.WithFields(logFields).Error(err)
		}
		results <- res
	}
}

func processResult(rid int, result cluster.RepairResult) {

	log.WithFields(logrus.Fields{
		"fid": result.Frag.ID,
        	"result": rid,
        	"message":  result.Message,
    		}).Info("Processing result")
}

func repair(node cluster.Node) {

	log.Info("Repairing node")

	w := *workers
	jobs := make(chan cluster.Fragment, w)
	results := make(chan cluster.RepairResult, w)



	for w := 1; w <= *workers; w++ {

		log.WithFields(logrus.Fields{
			"worker": w,
			}).Info("Starting worker")

		go repairFragment(w, jobs, results)
	}


	fragmentGenerator(node, jobs)

	for res := range results  {
		processResult(counter, res)
	}
}

func main() {

	flag.Parse()

	node := cluster.Node{Host: *host, Port: *port}
	url := os.Getenv("ELASTICSEARCH_URL")
	index := "cagrr"

    	client, err := elastic.NewClient(elastic.SetURL(url))
    	if err != nil {
        	log.WithFields(logrus.Fields{
			"url":	url,
			}).Panic(err)
    	}
    	hook, err := elogrus.NewElasticHook(client, node.Host, logrus.DebugLevel, index)
    	if err != nil {
        	log.WithFields(logrus.Fields{
			"url":	url,
			"index": index,
			}).Panic(err)
    	}
    	log.Hooks.Add(hook)

	repair(node)
}
