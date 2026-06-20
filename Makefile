# market-stream — thin ergonomic wrapper. The sibling spark-k8s repo deliberately uses
# explicit tofu/ansible CLI; here the laptop docker-compose stack is the primary path,
# so a couple of one-liners earn their keep. Everything below is also documented as raw
# commands in the README, so the Makefile is convenience, not a hidden dependency.

COMPOSE := docker compose -f local/docker-compose.yml --env-file .env
REGISTRY ?= ghcr.io/shaked98
TAG ?= 3.5.3-iceberg1.7.1
ANSIBLE_INV ?= inventory/hosts.ini

.DEFAULT_GOAL := help
.PHONY: help local-up local-down local-logs local-smoke test validate \
        images push deploy teardown

help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	  | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

# ── Local (laptop, zero cloud cost) ─────────────────────────────────────────────
local-up: ## Bring the full local pipeline up (redpanda, minio, lakekeeper, trino, producer, spark, web)
	@test -f .env || cp .env.example .env
	$(COMPOSE) up -d --build

local-down: ## Tear the local stack down (add ARGS=-v to wipe volumes)
	$(COMPOSE) down $(ARGS)

local-logs: ## Tail logs from the local stack
	$(COMPOSE) logs -f --tail=100

local-smoke: ## Run the end-to-end smoke test against the local stack
	TRINO_HOST=localhost TRINO_PORT=8080 tests/smoke-test.sh

# ── Quality gates ───────────────────────────────────────────────────────────────
test: ## Run Python unit tests (producer + streaming transforms)
	python -m pytest -q

validate: ## Offline validation (YAML/manifests parse, secrets encrypted, schemas valid)
	tests/validate.sh

# ── Images ──────────────────────────────────────────────────────────────────────
images: ## Build the producer / streaming / web images
	docker build -t $(REGISTRY)/market-producer:latest producer
	docker build -t $(REGISTRY)/spark-market-stream:$(TAG) streaming
	docker build -t $(REGISTRY)/market-stream-api:latest web/api

push: images ## Push the images to the registry (requires docker login)
	docker push $(REGISTRY)/market-producer:latest
	docker push $(REGISTRY)/spark-market-stream:$(TAG)
	docker push $(REGISTRY)/market-stream-api:latest

# ── Cluster (deploys onto the existing spark-k8s cluster) ────────────────────────
deploy: ## Deploy market-stream workloads onto the spark-k8s cluster
	cd ansible && ANSIBLE_CONFIG="$$PWD/ansible.cfg" \
	  ansible-playbook -i $(ANSIBLE_INV) site.yml

teardown: ## Remove ONLY market-stream workloads (leaves the shared cluster intact)
	cd ansible && ANSIBLE_CONFIG="$$PWD/ansible.cfg" \
	  ansible-playbook -i $(ANSIBLE_INV) site.yml --tags teardown
