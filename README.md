cassandra-go-range-repair
=========================
Cassandra partial range repair in Go

Anti-entropy Cassandra cluster tool

Prerequisites
-------------
1. `nodetool` installed
2. 

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