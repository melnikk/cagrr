# cassandra-go-range-repair
Cassandra partial range repair in Go

Anti-entropy Cassandra cluster tool

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
