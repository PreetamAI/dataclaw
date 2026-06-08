BACKEND_RUN = sh -c 'if command -v uv >/dev/null 2>&1; then uv run python -m pytest "$$@"; else .venv/bin/python -m pytest "$$@"; fi' --
BACKEND_RUFF = sh -c 'if command -v uv >/dev/null 2>&1; then uv run ruff check app tests; else .venv/bin/ruff check app tests; fi'
ACME_CONNECTORS = notion github confluence bigquery snowflake databricks redshift fivetran postgres mysql sql_server trino airflow dbt prefect dagster airbyte sqlite

.PHONY: help install dev backend frontend worker chroma test test-backend test-frontend test-e2e test-integration test-integration-connector test-integration-full integration-up integration-down integration-e2e integration-seed acme-fixtures acme-clean-reports acme-seed acme-coverage acme-compile acme-chat acme-agents acme-evals-loop acme-report acme-full lint matrix-check verify build bundle-frontend quickstart wheel clean

help:
	@echo "DataClaw — make targets"
	@echo ""
	@echo "  install            install backend (uv) + frontend (npm) deps"
	@echo "  dev                run backend + frontend + worker + chroma in foreground"
	@echo "  backend            run only the backend (uvicorn, port 8000)"
	@echo "  frontend           run only the frontend (vite, port 5173)"
	@echo "  worker             run only the background worker"
	@echo "  chroma             run only ChromaDB (Docker, port 8001)"
	@echo "  test               run all unit/contract tests"
	@echo "  test-backend       run backend pytest"
	@echo "  test-frontend      run frontend vitest + typecheck"
	@echo "  test-e2e           run end-to-end tests (no Docker required)"
	@echo "  test-integration   bring up the integration compose and run live adapter tests"
	@echo "  test-integration-connector CONNECTOR=slug run one live Phase H connector case"
	@echo "  test-integration-full release/nightly full connector matrix gate"
	@echo "  integration-up     start the integration compose (Postgres, MySQL, Chroma, fixture APIs)"
	@echo "  integration-down   stop and remove the integration compose"
	@echo "  integration-e2e    up -> backend chat-agent integration pytest -> down"
	@echo "  integration-seed   re-run SQL seed scripts in running integration DBs"
	@echo "  acme-fixtures      regenerate Acme MCP coverage fixtures from catalog"
	@echo "  acme-clean-reports remove local Acme JSON report artifacts"
	@echo "  acme-seed          write Acme seed manifest; BOOT_CONTAINERS=1 starts Docker services"
	@echo "  acme-coverage      validate or run Acme MCP coverage; CONNECTOR=slug narrows selection"
	@echo "  acme-compile       run Acme compile + retrieval E2E tests"
	@echo "  acme-chat          run one Acme chat scenario; SCENARIO=1_docs"
	@echo "  acme-agents        run Acme background-agent E2E tests"
	@echo "  acme-full          Acme seed + coverage + chat + agents + report"
	@echo "  lint               ruff (backend) + tsc --noEmit (frontend)"
	@echo "  matrix-check       generated connector matrix + public repo hygiene"
	@echo "  verify             lint + tests + matrix-check"
	@echo "  build              tsc + vite build (frontend)"
	@echo "  bundle-frontend    build frontend, copy into backend/app/static"
	@echo "  wheel              bundle frontend + build pip-installable wheel"
	@echo "  quickstart         install + bundle + init + launch"
	@echo "  clean              remove caches, dist, sqlite demo files"

install:
	cd backend && uv sync --all-extras
	cd frontend && npm ci

dev:
	cd backend && uv run dataclaw migrate
	@trap 'kill %1 %2 %3 %4 2>/dev/null || true' EXIT INT TERM; \
	$(MAKE) chroma & \
	$(MAKE) worker & \
	$(MAKE) backend & \
	$(MAKE) frontend & \
	wait

backend:
	cd backend && uv run uvicorn app.main:app --reload --host 127.0.0.1 --port 8000

frontend:
	cd frontend && npm run dev -- --host 127.0.0.1 --port 5173

worker:
	cd backend && uv run python -m app.worker.main

chroma:
	docker compose up chroma

test: test-backend test-frontend

test-backend:
	cd backend && $(BACKEND_RUN) -q

test-frontend:
	cd frontend && npm test && npm run typecheck

test-e2e:
	cd backend && $(BACKEND_RUN) tests/e2e -v --runslow

integration-up:
	docker compose -f tests/integration/docker-compose.yml up -d --wait --force-recreate --build

integration-down:
	docker compose -f tests/integration/docker-compose.yml down

test-integration: integration-up
	cd backend && RUN_CONNECTOR_INTEGRATION=1 $(BACKEND_RUN) tests/integration -m integration -v

test-integration-connector:
	@set -eu; \
	if [ -z "$${CONNECTOR:-}" ]; then \
		echo "CONNECTOR is required, e.g. make test-integration-connector CONNECTOR=postgres"; \
		exit 2; \
	fi; \
	if [ -z "$${OPENAI_API_KEY:-}" ]; then \
		echo "OPENAI_API_KEY is required for make test-integration-connector"; \
		exit 2; \
	fi; \
	case "$${CONNECTOR}" in \
		postgres) CONNECTOR_SERVICE=postgres ;; \
		mysql) CONNECTOR_SERVICE=mysql ;; \
		sql_server) CONNECTOR_SERVICE=sql_server ;; \
		trino) CONNECTOR_SERVICE=trino ;; \
		bigquery) CONNECTOR_SERVICE=bigquery ;; \
		airflow) CONNECTOR_SERVICE=airflow ;; \
		dbt) CONNECTOR_SERVICE=fixture-api ;; \
		dagster) CONNECTOR_SERVICE=dagster ;; \
		prefect) CONNECTOR_SERVICE=prefect ;; \
		airbyte) CONNECTOR_SERVICE=fixture-api ;; \
		github) CONNECTOR_SERVICE=fixture-api ;; \
		confluence) CONNECTOR_SERVICE=fixture-api ;; \
		notion) CONNECTOR_SERVICE=fixture-api ;; \
		*) echo "Unknown Phase H connector '$${CONNECTOR}'"; exit 2 ;; \
	esac; \
	ROOT="$$(pwd)"; \
	COMPOSE_FILE="$${ROOT}/tests/integration/docker-compose.yml"; \
	PROJECT="dataclaw-$${CONNECTOR}"; \
	trap 'docker compose -p '"$$PROJECT"' -f '"$$COMPOSE_FILE"' down --remove-orphans' EXIT; \
	if [ "$${CONNECTOR_SERVICE}" = "fixture-api" ] || [ "$${CONNECTOR_SERVICE}" = "dagster" ]; then docker compose -p "$${PROJECT}" -f "$${COMPOSE_FILE}" build "$${CONNECTOR_SERVICE}"; fi; \
	docker compose -p "$${PROJECT}" -f "$${COMPOSE_FILE}" up -d --wait chroma "$${CONNECTOR_SERVICE}"; \
	if [ "$${CONNECTOR}" = "sql_server" ]; then python3 tests/integration/seed/run.py --only sql_server; fi; \
	if [ "$${CONNECTOR}" = "trino" ]; then \
		for attempt in $$(seq 1 12); do \
			if docker compose -p "$${PROJECT}" -f "$${COMPOSE_FILE}" exec -T trino trino --server http://localhost:8080 --execute "select 1" >/dev/null 2>&1; then break; fi; \
			if [ "$${attempt}" = "12" ]; then echo "Trino did not become query-ready"; exit 1; fi; \
			sleep 5; \
		done; \
		docker compose -p "$${PROJECT}" -f "$${COMPOSE_FILE}" exec -T trino trino --server http://localhost:8080 < tests/integration/seed/sql/trino/01_seed.sql; \
	fi; \
	if [ "$${CONNECTOR}" = "bigquery" ]; then python3 tests/integration/seed/run.py --only bigquery; fi; \
	cd "$${ROOT}/backend" && OPENAI_API_KEY="$${OPENAI_API_KEY}" RUN_CONNECTOR_INTEGRATION=1 DATACLAW_AIRBYTE_API_URL="$${DATACLAW_AIRBYTE_API_URL:-http://127.0.0.1:18084}" DATACLAW_FULL_STACK_RELEASE_GATE=1 DATACLAW_FULL_STACK_CONNECTOR="$${CONNECTOR}" $(BACKEND_RUN) tests/integration/e2e/test_full_stack.py -q

test-integration-full:
	@set -eu; \
	if [ -z "$${OPENAI_API_KEY:-}" ]; then \
		echo "OPENAI_API_KEY is required for make test-integration-full"; \
		exit 2; \
	fi; \
	if [ ! -f backend/tests/integration/e2e/test_full_stack.py ]; then \
		echo "backend/tests/integration/e2e/test_full_stack.py is not implemented yet; run make test-integration for the current integration suite."; \
		exit 2; \
	fi; \
	trap '$(MAKE) -C "$(CURDIR)" integration-down' EXIT; \
	$(MAKE) integration-up; \
	$(MAKE) integration-seed; \
	cd backend && OPENAI_API_KEY="$${OPENAI_API_KEY}" RUN_CONNECTOR_INTEGRATION=1 DATACLAW_FULL_STACK_RELEASE_GATE=1 $(BACKEND_RUN) tests/integration -m integration -v

integration-e2e:
	@set -eu; \
	EXPLICIT_DATACLAW_API_URL="$${DATACLAW_API_URL:-}"; \
	if [ -f .env ]; then set -a; . ./.env; set +a; fi; \
	if [ -n "$$EXPLICIT_DATACLAW_API_URL" ]; then \
		export DATACLAW_API_URL="$$EXPLICIT_DATACLAW_API_URL"; \
	else \
		unset DATACLAW_API_URL; \
	fi; \
	API_PID=""; \
	cleanup() { \
		if [ -n "$$API_PID" ]; then kill "$$API_PID" >/dev/null 2>&1 || true; fi; \
		$(MAKE) -C "$(CURDIR)" integration-down; \
	}; \
	trap cleanup EXIT; \
	docker compose -f tests/integration/docker-compose.yml up -d --wait --force-recreate --build chroma postgres fixture-api; \
	if [ -z "$${DATACLAW_API_URL:-}" ]; then \
		API_URL="http://127.0.0.1:18000"; \
		rm -f /tmp/dataclaw_e2e.sqlite /tmp/dataclaw_demo_e2e.sqlite; \
		rm -rf /tmp/dataclaw_e2e_wiki; \
		cd backend; \
		DATABASE_URL=sqlite+aiosqlite:////tmp/dataclaw_e2e.sqlite \
		DEMO_DATABASE_URL=sqlite+aiosqlite:////tmp/dataclaw_demo_e2e.sqlite \
		DEMO_MODE=true \
		DATACLAW_TEST_AUTO_CREATE_SCHEMA=true \
		WIKI_ROOT=/tmp/dataclaw_e2e_wiki \
		CHROMA_URL=http://localhost:18001 \
		MASTER_KEY=test-master-key-please-change \
		SESSION_SECRET=test-session-secret-please-change \
		uv run python -m uvicorn app.main:app --host 127.0.0.1 --port 18000 >/tmp/dataclaw_integration_e2e_api.log 2>&1 & \
		API_PID=$$!; \
		cd ..; \
		for _ in $$(seq 1 60); do curl -fsS "$$API_URL/health" >/dev/null && break; sleep 1; done; \
		curl -fsS "$$API_URL/health" >/dev/null; \
		export DATACLAW_API_URL="$$API_URL"; \
	fi; \
	cd backend && \
		DATABASE_URL=sqlite+aiosqlite:////tmp/dataclaw_e2e.sqlite \
		DEMO_DATABASE_URL=sqlite+aiosqlite:////tmp/dataclaw_demo_e2e.sqlite \
		DEMO_MODE=true \
		DATACLAW_TEST_AUTO_CREATE_SCHEMA=true \
		WIKI_ROOT=/tmp/dataclaw_e2e_wiki \
		CHROMA_URL=http://localhost:18001 \
		MASTER_KEY=test-master-key-please-change \
		SESSION_SECRET=test-session-secret-please-change \
		RUN_CONNECTOR_INTEGRATION=1 \
		RUN_OPENAI_E2E=$${RUN_OPENAI_E2E:-0} \
		$(BACKEND_RUN) tests/integration/test_e2e_knowledge.py -v

integration-seed:
	cd backend && uv run --project . python ../tests/integration/seed/run.py

acme-fixtures:
	backend/.venv/bin/python tests/integration/acme/coverage/generate_fixtures.py

acme-clean-reports:
	rm -f coverage-*.json coverage-live-*.json chat-*.json compile-retrieval.json agents.json
	rm -rf artifacts

acme-seed:
	@set -eu; \
	args=""; \
	if [ "$${BOOT_CONTAINERS:-0}" = "1" ]; then args="$$args --boot-containers"; fi; \
	if [ "$${CONTAINERS_ONLY:-0}" = "1" ]; then args="$$args --containers-only"; fi; \
	if [ "$${SAAS_ONLY:-0}" = "1" ]; then args="$$args --saas-only"; fi; \
	if [ -n "$${CONNECTOR:-}" ]; then args="$$args --container $$CONNECTOR"; fi; \
	if [ "$${REQUIRE_LIVE:-0}" = "1" ]; then args="$$args --require-live"; fi; \
	backend/.venv/bin/python tests/integration/acme/seed/seed_acme.py $$args

acme-coverage:
	@set -eu; \
	if [ -z "$${CONNECTOR:-}" ]; then \
		for connector in $(ACME_CONNECTORS); do \
			$(MAKE) acme-coverage CONNECTOR="$$connector"; \
		done; \
		exit 0; \
	fi; \
	report_connector="$${CONNECTOR}"; \
	cd backend && ACME_COVERAGE_RESULTS_FILE="$${ACME_COVERAGE_RESULTS_FILE:-../coverage-live-$$report_connector.json}" \
		ACME_COVERAGE_CONNECTOR="$$report_connector" RUN_ACME_MCP_COVERAGE=$${RUN_ACME_MCP_COVERAGE:-1} .venv/bin/python -m pytest ../tests/integration/acme/coverage -v \
		--json-report --json-report-file="../coverage-$$report_connector.json"

acme-chat:
	@set -eu; \
	if [ -z "$${SCENARIO:-}" ]; then echo "usage: make acme-chat SCENARIO=1_docs"; exit 2; fi; \
	cd backend && RUN_ACME_E2E=$${RUN_ACME_E2E:-1} .venv/bin/python -m pytest ../tests/integration/acme/e2e/test_chat_scenario_$${SCENARIO}.py -v \
		--json-report --json-report-file="../chat-$${SCENARIO}.json"

acme-agents:
	cd backend && RUN_ACME_E2E=$${RUN_ACME_E2E:-1} .venv/bin/python -m pytest ../tests/integration/acme/e2e/test_background_agents.py -v \
		--json-report --json-report-file="../agents.json"

acme-evals-loop:
	cd backend && RUN_ACME_E2E=$${RUN_ACME_E2E:-1} .venv/bin/python -m pytest ../tests/integration/acme/e2e/test_evals_loop.py -v \
		--json-report --json-report-file="../evals-loop.json"

acme-compile:
	cd backend && RUN_ACME_E2E=$${RUN_ACME_E2E:-1} .venv/bin/python -m pytest ../tests/integration/acme/e2e/test_compile_retrieval.py -v \
		--json-report --json-report-file="../compile-retrieval.json"

acme-report:
	@set -eu; \
	if [ "$${REQUIRE_LIVE:-0}" = "1" ]; then \
		backend/.venv/bin/python tests/integration/acme/report/aggregate.py; \
		backend/.venv/bin/python tests/integration/acme/report/check_all_green.py; \
	else \
		ACME_REPORT_DOCS_DIR=artifacts/acme-reports backend/.venv/bin/python tests/integration/acme/report/aggregate.py; \
		echo "Skipping Acme all-green enforcement for local/no-creds report. Use REQUIRE_LIVE=1 for the release gate."; \
	fi

acme-full:
	$(MAKE) acme-clean-reports
	@if [ "$${REQUIRE_LIVE:-0}" = "1" ]; then \
		$(MAKE) acme-seed BOOT_CONTAINERS=1 REQUIRE_LIVE=1; \
		$(MAKE) acme-coverage RUN_ACME_MCP_COVERAGE=1; \
		$(MAKE) acme-compile RUN_ACME_E2E=1; \
		$(MAKE) acme-chat SCENARIO=1_docs RUN_ACME_E2E=1; \
		$(MAKE) acme-chat SCENARIO=2_bq RUN_ACME_E2E=1; \
		$(MAKE) acme-chat SCENARIO=3_prefect RUN_ACME_E2E=1; \
		$(MAKE) acme-chat SCENARIO=4_messy RUN_ACME_E2E=1; \
		$(MAKE) acme-chat SCENARIO=5_write RUN_ACME_E2E=1; \
		$(MAKE) acme-agents RUN_ACME_E2E=1; \
		$(MAKE) acme-evals-loop RUN_ACME_E2E=1; \
		$(MAKE) acme-report REQUIRE_LIVE=1; \
	else \
		$(MAKE) acme-seed SAAS_ONLY=1; \
		$(MAKE) acme-coverage RUN_ACME_MCP_COVERAGE=0; \
		$(MAKE) acme-compile RUN_ACME_E2E=0; \
		$(MAKE) acme-chat SCENARIO=1_docs RUN_ACME_E2E=0; \
		$(MAKE) acme-chat SCENARIO=2_bq RUN_ACME_E2E=0; \
		$(MAKE) acme-chat SCENARIO=3_prefect RUN_ACME_E2E=0; \
		$(MAKE) acme-chat SCENARIO=4_messy RUN_ACME_E2E=0; \
		$(MAKE) acme-chat SCENARIO=5_write RUN_ACME_E2E=0; \
		$(MAKE) acme-agents RUN_ACME_E2E=0; \
		$(MAKE) acme-evals-loop RUN_ACME_E2E=0; \
		$(MAKE) acme-report; \
	fi

lint:
	cd backend && $(BACKEND_RUFF)
	cd frontend && npm run typecheck

matrix-check:
	@tmp="$$(mktemp)"; \
	cp docs/CONNECTOR_MATRIX.md "$$tmp"; \
	backend/.venv/bin/python scripts/generate_connector_matrix.py; \
	cmp -s "$$tmp" docs/CONNECTOR_MATRIX.md || { \
		rm -f "$$tmp"; \
		echo "docs/CONNECTOR_MATRIX.md is out of sync; commit the regenerated file."; \
		exit 1; \
	}; \
	rm -f "$$tmp"
	test ! -e docs/internal
	test ! -e api_tokens.txt
	@bad="$$(git ls-files | awk '/(^|\/)(\.env|api_tokens\.txt|goal\.md)$$|(^|\/)docs\/internal\// { print }' | while IFS= read -r path; do \
		if [ -e "$$path" ]; then echo "$$path"; fi; \
	done)"; \
	if [ -n "$$bad" ]; then echo "$$bad"; exit 1; fi

verify: lint test matrix-check

build:
	cd frontend && npm run build

bundle-frontend: build
	rm -rf backend/app/static
	mkdir -p backend/app/static
	cp -R frontend/dist/. backend/app/static/

quickstart:
	@$(MAKE) install
	@$(MAKE) bundle-frontend
	cd backend && uv run dataclaw init || true
	cd backend && uv run dataclaw start

wheel: bundle-frontend
	rm -rf backend/build backend/dist backend/*.egg-info
	cd backend && uv run --with build python -m build --wheel
	@echo
	@echo "Wheel built. Install with:  pipx install backend/dist/dataclaw_platform-*.whl"

clean:
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
	find . -type d -name .pytest_cache -prune -exec rm -rf {} +
	find . -type d -name .ruff_cache -prune -exec rm -rf {} +
	rm -rf frontend/dist backend/app/static backend/build backend/dist backend/*.egg-info
	rm -f /tmp/dataclaw_app.sqlite /tmp/dataclaw_demo.sqlite
