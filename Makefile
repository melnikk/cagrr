default: run

clean:
	@docker-compose down

init:
	@docker-compose up -d

setup: init
	ansible-playbook -i inventory provision.yml && \
	docker-compose restart


holes: init
	@./tests/stub.py make

test: holes

check: init
	@./tests/stub.py

run:
	@./src/stitch.py 172.17.0.2
