default: run

clean:
	@docker-compose down -v --remove-orphans

init:
	@docker-compose up -d

setup: init
	@ansible-playbook -i inventory provision.yml && \
	docker-compose restart cassandra1 cassandra2 cassandra3


holes: init
	@./tests/stub.py make

test: holes

check: init
	@./tests/stub.py

build:
	@go build -a

run:
	ELASTICSEARCH_URL=http://172.16.237.20:9200 GRAPHITE_URL=http://172.16.237.4:2300 go run main.go -h 172.16.238.10 -k fedikeyspace -w 4 -s 10
