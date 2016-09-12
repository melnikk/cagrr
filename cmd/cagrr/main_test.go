// +build integration

package main

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/docker/engine-api/client"
	"github.com/docker/engine-api/types"
	"github.com/gocql/gocql"
	"golang.org/x/net/context"
)

const (
	table  = "testtable"
	userid = "testuser"

	testKeyspace  = "testspace"
	originalValue = "original"
	newValue      = "new"
	tries         = 1000
	protoVersion  = 3
)

var (
	writeHosts = []string{"172.16.238.10", "172.16.238.11", "172.16.238.12"}
	readHosts  = []string{"172.16.238.10", "172.16.238.11", "172.16.238.12"}
)

func operateContainer(t *testing.T, containerName string, operation string) {

	defaultHeaders := map[string]string{"User-Agent": "engine-api-cli-1.0"}
	cli, err := client.NewClient("unix:///var/run/docker.sock", "v1.24", nil, defaultHeaders)
	if err != nil {
		t.Fatal(err)
	}

	switch operation {
	case "start":
		options := types.ContainerStartOptions{}
		err = cli.ContainerStart(context.Background(), containerName, options)
	case "stop":
		timeout, _ := time.ParseDuration("15s")
		err = cli.ContainerStop(context.Background(), containerName, &timeout)
	}

	if err != nil {
		t.Fatal(err, operation)
	} else {
		t.Logf("Container %s, operation %s successfully completed", containerName, operation)
	}

}

func makeHoles(t *testing.T) gocql.UUID {

	cluster := gocql.NewCluster(writeHosts...)
	cluster.ProtoVersion = protoVersion
	cluster.Consistency = gocql.One

	session, _ := cluster.CreateSession()
	defer session.Close()

	// Check keyspace and table existance

	if err := session.Query(
		fmt.Sprintf(`CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 }`, testKeyspace)).Exec(); err != nil {
		t.Fatal(err)
	}

	if err := session.Query(
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.%s (id UUID, userid text, firstname text, PRIMARY KEY(id)) WITH read_repair_chance = 0 AND dclocal_read_repair_chance = 0`, testKeyspace, table)).Exec(); err != nil {
		t.Fatal(err)
	}

	if err := session.Query(
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS ON %s.%s (firstname)`, testKeyspace, table)).Exec(); err != nil {
		t.Fatal(err)
	}

	// Write to cluster originalValue
	uuid := gocql.TimeUUID()
	if err := session.Query(
		fmt.Sprintf(`INSERT INTO %s.%s (id, userid, firstname) VALUES (?, ?, ?)`, testKeyspace, table),
		uuid, userid, originalValue).Consistency(gocql.One).Exec(); err != nil {
		t.Fatal(err)
	} else {
		t.Logf(`Inserted value %s with UUID=%s`, originalValue, uuid)
	}

	// Stop random deadNode
	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)
	node := 1 + r1.Intn(2)
	nodeName := fmt.Sprintf(`cagrr_cassandra%d_1`, node)
	operateContainer(t, nodeName, `stop`)
	time.Sleep(time.Second * 5)
	t.Logf("Node %s stopped", nodeName)

	// Write to cluster newValue
	if err := session.Query(
		fmt.Sprintf(`UPDATE %s.%s SET firstname=? WHERE id=?`, testKeyspace, table),
		newValue, uuid).Consistency(gocql.One).Exec(); err != nil {
		t.Fatal(err)
	} else {
		t.Logf(`Value updated to %s with UUID=%s`, newValue, uuid)
	}

	// Start deadNode
	operateContainer(t, nodeName, `start`)
	time.Sleep(time.Second * 20)
	t.Logf("Node %s started", nodeName)
	return uuid
}

// Test holes and makers
func TestHoles(t *testing.T) {

	uuid := makeHoles(t)

	prob := searchHole(t, uuid)
	if prob > 0 {
		t.Logf("Hole found! Probability: %.2f", prob)
	} else {
		t.Error("There is no holes")
	}
}

func searchHole(t *testing.T, uuid gocql.UUID) float32 {

	// Read from cluster and try to find new value

	cluster := gocql.NewCluster(readHosts...)
	cluster.ProtoVersion = protoVersion
	cluster.Consistency = gocql.One

	session, _ := cluster.CreateSession()
	defer session.Close()

	count := 0
	results := make(chan bool, 3)

	t.Logf("Checking holes")

	for i := 1; i <= tries; i++ {
		go readResult(session, uuid, i, results)
	}
	for k := 1; k <= tries; k++ {
		res := <-results
		if res == true {
			count++
		}
	}
	return float32(count) / float32(tries)
}

func readResult(session *gocql.Session, uuid gocql.UUID, i int, results chan<- bool) {

	q := fmt.Sprintf("select id, userid, firstname from %s.%s where firstname=? and id=?", testKeyspace, table)
	var id gocql.UUID
	var userid string
	var value string
	if err := session.Query(q, originalValue, uuid).Consistency(gocql.One).Scan(&id, &userid, &value); err != nil {
		results <- false
	} else {
		results <- true
	}
}
