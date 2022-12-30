.PHONY: up
up: ## start local containers
	docker-compose -f docker/docker-compose.yaml pull
	docker-compose -f docker/docker-compose.yaml up -d

.PHONY: down
down: ## shuts down local docker containers
	docker-compose -f docker/docker-compose.yaml down --remove-orphans
	@rm -rf docker/data

.PHONY: test
test:
	go test -race . --tags=tikv