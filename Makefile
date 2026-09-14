# Everything runs inside the compose network, so the only host dependency is
# Docker. `make demo` is the guided tour.

SHELL := /bin/bash
.DEFAULT_GOAL := help

COMPOSE := docker compose
# The CLI runs inside the compute worker: it needs Temporal, MinIO and the
# warehouse volume, all of which are reachable by service name in there and
# would otherwise need host port mapping and a local Python environment.
RUN  := $(COMPOSE) exec -T worker-compute python -m duckflow.cli
RUNI := $(COMPOSE) exec worker-compute python -m duckflow.cli

DATE   ?= 2026-08-01
MODE   ?= auto
METERS ?= 20000

.PHONY: help
help: ## Show this help
	@echo "Temporal + DuckDB reference pipeline"
	@echo
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
	  | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'
	@echo
	@echo "Variables: DATE=$(DATE) MODE=$(MODE) METERS=$(METERS)"
	@echo
	@echo "UIs:  Temporal http://localhost:8234   MinIO http://localhost:9201"
	@echo "      (minioadmin / minioadmin)"

# ---------------------------------------------------------------- lifecycle

.PHONY: build
build: ## Build the worker image
	$(COMPOSE) build

.PHONY: up
up: ## Start the whole stack and wait for it to be ready
	$(COMPOSE) up -d
	@echo "waiting for workers to register..."
	@for i in $$(seq 1 60); do \
	  if $(COMPOSE) logs worker-compute 2>/dev/null | grep -q "worker ready" \
	  && $(COMPOSE) logs worker-writer  2>/dev/null | grep -q "worker ready"; then \
	    echo "stack ready."; break; \
	  fi; sleep 2; \
	done
	@$(MAKE) --no-print-directory ps

.PHONY: down
down: ## Stop the stack, keep the lake and the warehouse
	$(COMPOSE) down

.PHONY: clean
clean: ## Stop the stack and DELETE the lake, warehouse and Temporal history
	$(COMPOSE) down -v --remove-orphans

.PHONY: ps
ps: ## Show service status
	@$(COMPOSE) ps --format '{{.Service}}\t{{.State}}\t{{.Status}}'

.PHONY: logs
logs: ## Tail both workers
	$(COMPOSE) logs -f worker-compute worker-writer

.PHONY: restart
restart: ## Reload worker code (src/ is bind-mounted, so no rebuild needed)
	$(COMPOSE) restart worker-compute worker-writer
	@sleep 8 && echo "workers reloaded."

.PHONY: shell
shell: ## Interactive shell in the compute worker
	$(COMPOSE) exec worker-compute bash

.PHONY: scale
scale: ## Add compute workers: make scale N=4  (never scale the writer)
	$(COMPOSE) up -d --scale worker-compute=$(or $(N),3)
	@echo "compute workers: $(or $(N),3). worker-writer stays at 1 -- see README 4."

# ------------------------------------------------------------------- runs

.PHONY: run
run: ## Run the pipeline (DATE/MODE/METERS)
	$(RUNI) run --date $(DATE) --mode $(MODE) --meters $(METERS) --no-approval --wait

.PHONY: run-lean
run-lean: ## Run with a 512MB memory limit everywhere, so the big steps spill
	$(RUNI) run --date $(DATE) --mode lean --meters $(METERS) --no-approval --wait

.PHONY: run-roomy
run-roomy: ## Run with 1GB and 4 threads everywhere
	$(RUNI) run --date $(DATE) --mode roomy --meters $(METERS) --no-approval --wait

.PHONY: run-auto
run-auto: ## Run with AUTO sizing so each step is probed and sized at runtime
	$(RUNI) run --date $(DATE) --mode auto --meters $(METERS) \
	  --threshold-mb 16 --no-approval --wait

.PHONY: run-approval
run-approval: ## Run with the human publish gate (then: make approve ID=...)
	$(RUNI) run --date $(DATE) --mode auto --meters $(METERS) \
	  --approval-timeout 0 --id approval-demo
	@echo; echo "parked awaiting approval. release it with:"
	@echo "  make approve ID=approval-demo"

.PHONY: approve
approve: ## Send approve_publish: make approve ID=<workflow-id>
	$(RUNI) approve $(ID) --actor "$(USER)"

.PHONY: watch
watch: ## Live progress query: make watch ID=<workflow-id>
	$(RUNI) watch $(ID) --follow

.PHONY: pause
pause: ## Pause at the next phase boundary: make pause ID=<workflow-id>
	$(RUNI) pause $(ID)

.PHONY: resume
resume: ## Resume a paused run: make resume ID=<workflow-id>
	$(RUNI) resume $(ID)

.PHONY: extend-sla
extend-sla: ## Workflow update: make extend-sla ID=<id> STEP=silver_enrich SECS=120
	$(RUNI) extend-sla $(ID) $(or $(STEP),silver_enrich) $(or $(SECS),120)

.PHONY: backfill
backfill: ## Backfill a range: make backfill START=2026-09-01 END=2026-09-05
	$(RUNI) backfill --start $(or $(START),2026-09-01) --end $(or $(END),2026-09-05) \
	  --meters 500 --parallel 2 --checkpoint-every 3 --wait

.PHONY: schedule
schedule: ## Create the daily Temporal Schedule (paused)
	$(RUNI) schedule --cron "0 2 * * *" --mode auto

# ---------------------------------------------------------- failure demos

.PHONY: demo-dq-failure
demo-dq-failure: ## A negative tariff trips a non-retryable gate -> saga rolls the warehouse back
	-$(RUNI) run --date 2026-10-01 --meters 500 --inject bad_tariff --no-approval --wait
	@echo; echo "the run failed and compensated. Failing checks:"
	@$(RUN) report --date 2026-10-01 --limit 3 2>/dev/null | sed -n '/quality checks/,/publish log/p'

.PHONY: demo-warn
demo-warn: ## A WARN-level check fails and the run continues anyway
	$(RUNI) run --date 2026-10-03 --meters 500 --inject voltage_storm --no-approval --wait
	@$(RUN) report --date 2026-10-03 --limit 3 2>/dev/null | sed -n '/quality checks/,/publish log/p'

.PHONY: demo-chaos
demo-chaos: ## Force a step to fail -> 3 retries -> compensation
	-$(RUNI) run --date 2026-10-02 --meters 500 --fail-step silver_enrich \
	  --no-approval --wait
	@echo; echo "inspect the 3 attempts in the Temporal UI:"
	@echo "  http://localhost:8234/namespaces/default/workflows"

.PHONY: demo-contention
demo-contention: ## Show what DuckDB refuses across processes, and conflicts on within one
	$(RUNI) contention

.PHONY: demo-durability
demo-durability: ## Park a run, stop BOTH workers, bring them back, watch it resume
	@bash scripts/kill_demo.sh

.PHONY: demo-rollback
demo-rollback: ## Publish a day, publish a different one, fail after commit, watch it restore
	@echo "-- run 1: 20,000 meters, publishes cleanly --"
	$(RUNI) run --date 2026-10-05 --meters 20000 --no-approval --wait
	@$(RUN) query "select region, kwh from gold.daily_region_consumption where business_date = date '2026-10-05' order by region"
	@echo
	@echo "-- run 2: 4,000 meters, publishes, THEN fails --"
	-$(RUNI) run --date 2026-10-05 --meters 4000 --no-approval --fail-after-publish --wait
	@echo
	@echo "-- the warehouse, after compensation: run 1's numbers are back --"
	@$(RUN) query "select region, kwh from gold.daily_region_consumption where business_date = date '2026-10-05' order by region"
	@$(RUN) query "select run_id, table_name, rows_written, previous_run_id, compensated from meta.publish_log where business_date = date '2026-10-05' order by published_at"

# ------------------------------------------------------------- observability

.PHONY: report
report: ## Observability report from the meta.* tables
	$(RUNI) report --limit 10

.PHONY: steps
steps: ## Print the declared DAG, SLAs and quality gates
	$(RUNI) steps

.PHONY: lineage
lineage: ## Lineage graph of the most recent run
	$(RUNI) lineage

.PHONY: gold
gold: ## Peek at the gold tables
	$(RUNI) gold

.PHONY: warehouse
warehouse: ## Row counts of every warehouse table
	$(RUNI) warehouse

.PHONY: query
query: ## Read-only SQL: make query SQL="select ..."
	$(RUNI) query "$(SQL)"

.PHONY: lake
lake: ## List what the object store actually holds, by layer and dataset
	$(RUNI) lake

# ------------------------------------------------------------------- testing

.PHONY: test
test: ## 45 tests: catalog, warehouse, and doc cross-references
	$(COMPOSE) exec -T worker-compute sh -c \
	  "pip show pytest >/dev/null 2>&1 || pip install -q pytest==8.4.2 anyio==4.15.1"
	$(COMPOSE) exec -T worker-compute python -m pytest /opt/duckflow/tests -q \
	  --ignore=/opt/duckflow/tests/test_pipeline_workflow.py

.PHONY: test-workflow
test-workflow: ## Orchestration tests on a throwaway Temporal (1st run downloads it)
	$(COMPOSE) exec -T worker-compute sh -c \
	  "pip show pytest >/dev/null 2>&1 || pip install -q pytest==8.4.2 anyio==4.15.1"
	@echo "note: the first run in a fresh container downloads the Temporal dev"
	@echo "      server (~100 MB) before any test starts. Later runs are ~30s."
	$(COMPOSE) exec -T worker-compute python -m pytest \
	  /opt/duckflow/tests/test_pipeline_workflow.py -q

# ---------------------------------------------------------------- guided demo

.PHONY: demo
demo: ## Guided end-to-end tour (~6 min)
	@bash scripts/demo.sh
