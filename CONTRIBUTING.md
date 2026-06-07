# Contributing to DataClaw

DataClaw is early-stage open source. The highest-value contributions are reproducible bug reports, connector test evidence, focused fixes, and docs that match the backend catalog.

## Ways to Contribute

- Report a reproducible bug with logs, version, and steps.
- Request or improve a connector with credential and sync details.
- Fix documentation drift between docs and backend behavior.
- Add tests around connector catalog, MCP tools, sync materialization, or chat behavior.
- Improve local install, Docker, and release automation.

Please do not paste secrets, API keys, customer data, private URLs, or proprietary schemas into issues, pull requests, screenshots, logs, or fixtures.

## Local Setup

Recommended install smoke:

```bash
pipx install dataclaw-platform
dataclaw init
dataclaw start
```

Source development:

```bash
git clone https://github.com/svangapally81/dataclaw.git
cd dataclaw
make install
make dev
```

`make dev` runs the backend API, Vite frontend, worker, and Chroma service for development. The product UI runs at `http://localhost:5173`; the API runs at `http://localhost:8000`.

Docker Compose release-style run:

```bash
docker compose up -d --build
```

The compose stack serves the bundled UI and API on `http://localhost:8000`, runs a separate APScheduler worker, and uses the Chroma service.

## Required PR Gates

Run the focused gate before opening a pull request:

```bash
make verify
```

If your change is narrow and you need a faster loop, run the relevant pieces:

```bash
make lint
make test
make docs-check
make wheel
```

For UI changes, also run the Playwright smoke tests:

```bash
cd frontend
npx playwright test tests/e2e/workspace.spec.ts --project=chromium-desktop
npx playwright test tests/e2e/workspace.spec.ts --project=chromium-mobile
```

For live connector or full-stack changes:

```bash
make test-integration
OPENAI_API_KEY=... make test-integration-connector CONNECTOR=postgres
```

Use `make test-integration-full` only for release/nightly validation because it is slower and requires live service credentials.

## Pull Request Rules

- Keep PRs focused. Avoid mixing connector work, UI redesigns, dependency churn, and docs-only changes unless they are tightly related.
- Add or update tests for behavior changes.
- Update docs when commands, configuration, connector maturity, or API behavior changes.
- Regenerate the connector matrix when catalog/tooling changes.
- Do not commit generated build output, local databases, logs, `.env` files, token dumps, or internal planning notes.
- Explain skipped checks in the PR template.

## Connector Contributions

The connector catalog source of truth is `backend/app/services/connectors/catalog.py`. The generated reference table at `docs/CONNECTOR_MATRIX.md` must match it (`make matrix-check` enforces this in CI).

Connector maturity must stay honest:

- `stable`: live read/write behavior has tests or documented vendor proof, and audit/approval behavior is covered where writes exist.
- `beta`: implemented but fixture-backed, credential-gated, partially covered, or lacking live vendor evidence.
- `planned`: catalog/docs placeholder without production-ready execution.

Adapter test states must also be honest:

- `real`: adapter performs a live network or disk call.
- `demo`: falls back to a bundled demo source when no credentials are supplied.
- `credential_required`: adapter cannot reach the service without user-supplied credentials.
- `not_configured`: a required local dependency or environment value is missing.

Do not silently fake successful connector tests. If SaaS credentials are required, add a credential-gated integration test with a clear skip message naming the required environment variables.

## Database Migrations

Alembic migrations live in `backend/app/alembic/versions/`. After changing models in `backend/app/models/domain.py`, create and review a migration:

```bash
cd backend
alembic revision --autogenerate -m "short description"
```

Autogenerate is a starting point. Review the generated migration before committing.

## Maintainer Review

Maintainers may ask for smaller scope, additional tests, connector evidence, or docs/catalog parity before merging. Security-sensitive changes, migrations, dependency upgrades, release workflow changes, and connector maturity changes require extra review.
