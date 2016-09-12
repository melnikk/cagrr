cassandra-go-range-repair
=========================
[![Build Status](https://travis-ci.org/melnikk/cagrr.svg?branch=master)](https://travis-ci.org/melnikk/cagrr)
[![Go Report Card](https://goreportcard.com/badge/github.com/melnikk/cagrr)](https://goreportcard.com/report/github.com/melnikk/cagrr)

Cassandra partial range repair in Go

Anti-entropy Cassandra cluster tool

Prerequisites
-------------
1. `nodetool` installed (on your host)
2. *OR* `mx4j` interface activated (on cassandra node)
3. *OR* `jolokia` agent installed (on cassandra node)

You may control connection type via `-c` flag, default connector is `mx4j`.

Setup test environment
----------------------

Initialize test Cassandra 2.2 cluster (you need ansible, docker and docker-compose installed):

```
make setup
```

Run tests:

```
make test   # Make a "hole" and check existence
```
```
make check  # Only check, no write/restart cycle
```

Repair your cluster:

```
make run
```

Analyze your logs in [Kibana](https://github.com/elastic/kibana) interface available at:
```
http://172.16.237.50:5601
```

Check your metrics in [Grafana](https://github.com/grafana/grafana) interface available at:
```
http://172.16.237.30:3000
```

##Troubleshooting


1. Elasticsearch didn't start with `vm.max_map_count` error:

	Run on your host machine:
	```
	sudo sysctl -w vm.max_map_count=262144
	```
