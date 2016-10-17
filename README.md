Cassandra Go Range Repair tool
==============================
[![Build Status](https://travis-ci.org/skbkontur/cagrr.svg?branch=master)](https://travis-ci.org/skbkontur/cagrr)
[![Go Report Card](https://goreportcard.com/badge/github.com/skbkontur/cagrr)](https://goreportcard.com/report/github.com/skbkontur/cagrr)

Anti-entropy Cassandra cluster tool. It uses [repair service](https://github.com/skbkontur/cajrr) written in Java.

Prerequisites
-------------
You need [cajrr](https://github.com/skbkontur/cajrr) up and running.

Run tests:

```
make integration   # Make a "hole" and check existence
```
```
make check         # Only check, no write/restart cycle
```

Repair your cluster:

```
cagrr -k keyspace
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
