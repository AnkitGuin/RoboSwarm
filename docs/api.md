# HTTP API reference

All endpoints are versioned under `/api/v1`, except `/health`, `/docs` and `/openapi.json`.

| Method | Path | Purpose |
|---|---|---|
| GET | `/health` | DB connectivity and service state |
| GET | `/robots` | Paginated robot list |
| GET | `/robots/{robot_id}` | Robot and session count |
| GET | `/sessions` | Filter by robot/status, paginate |
| GET | `/sessions/{session_id}` | Session detail, optional `robot_id` disambiguator |
| POST | `/ingest/batch` | JSON body: `source`, `records` array; content-idempotent |
| GET | `/ingest/batches/{batch_id}` | Ingestion counts |
| POST | `/pipeline/runs` | JSON body: `batch_id`, optional `force`; synchronous |
| GET | `/pipeline/runs` | Paginated run status list |
| GET | `/pipeline/runs/{run_id}` | Run status/details |
| GET | `/pipeline/runs/{run_id}/report` | Persisted quality report |
| GET | `/episodes` | Latest logical episode revisions, filters and pagination |
| GET | `/episodes/{episode_id}` | Episode metadata and quality metrics |
| GET | `/episodes/{episode_id}/quality` | Explainable quality breakdown |
| GET | `/episodes/{episode_id}/telemetry` | Paginated normalized samples from Parquet |
| GET | `/issues` | Paginated/filtered issues |
| GET | `/overview` | Dashboard overview counts |
| GET | `/analytics/sensors` | DuckDB summary grouped by robot |

Query params `/episodes`: `robot_id`, `session_id`, `task_id`, `quality_status`, `completion_status`, `start`, `end`, `pipeline_run_id`, `limit`, `offset`. `start` and `end` must contain UTC offset (`Z` supported). `limit` between 1–500, except telemetry between 1–5000. `/issues` accepts `run_id`, `batch_id`, `issue_code`, `severity`, `limit`, `offset`. Filters are validated, with a 422 response for invalid values.

**Responses**: lists return `{items, total, limit, offset}`; ingestion returns `{batch_id, source, total, accepted, rejected, duplicates, created_at, reused}`; pipeline start returns `{run_id, status, config_hash, ... , reused}`. Errors use FastAPI's `detail` plus request-level `X-Request-ID`, with structured service/storage errors. Nonfatal record errors are returned in reject counts, not a misleading success for each invalid record. API path traversal is disallowed; clients cannot request server filesystem paths.

**Concurrency note**: `POST /pipeline/runs` performs processing synchronously in the request handler, which may take seconds/minutes for large batches. Use CLI outside the API server for larger files. No authorization in local demo: protect ports before exposing beyond localhost.
