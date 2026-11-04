# RoboStream

**Reproducible robotics telemetry and episode-quality infrastructure (synthetic data prototype).**

RoboStream receives timestamped robot observations and task events, archives raw JSONL input, rejects invalid records with traceable reasons, reconstructs task episodes, computes transparent quality metrics, writes typed Parquet telemetry, and indexes episodes in PostgreSQL. DuckDB supports analytics across Parquet and a Streamlit dashboard provides operational inspection. **It is not a robot controller and has not been validated on real hardware.**

## Why this exists

Robot-learning datasets need more than raw message logs. Telemetry streams can contain non-finite sensor values, missed task-end messages, duplicate IDs, impossible joint positions, clock disorder and missing observations. A downstream dataset consumer needs to know *which records passed validation*, *where the original source is*, *where episode boundaries lie*, and *how each episode's quality label was decided*. This prototype models those engineering steps and preserves evidence of rejected data.

## Architecture

```text
Synthetic generator / JSONL / POST
              |
              v
Ingest + checksum idempotency ----> raw/<sha256>.jsonl (immutable)
              |                                |
              v                                v
Postgres event-ID index              validation + temporal anomaly checks
                                               |
                                       rejected/issues + accepted UTC events
                                               |
                                    deterministic episode segmentation
                                               |
                             quality metrics + versioned Parquet per episode
                                               |
                                  PostgreSQL episode/run metadata
                                               |
                           FastAPI queries + DuckDB Parquet analytics
                                               |
                                Streamlit inspection dashboard
```

The pipeline executes **synchronously** in the API request or local CLI (no distributed queue). Large jobs should be run via CLI. Raw input never gets sorted or rewritten; normalized output is sorted by `(timestamp, sequence_number, event_id)` **within** `(robot_id, session_id)`. A run records arrival-order anomalies before sorting.

### Technology choices

| Role | Tool | Reason |
|---|---|---|
| API/data contracts | FastAPI + Pydantic v2 | Typed inputs and OpenAPI |
| Metadata/event-ID index | PostgreSQL + SQLAlchemy 2 + Alembic | Durable uniqueness and indexed lookups |
| Telemetry | PyArrow Parquet (Zstd) | Compact structured columnar samples |
| Analytics | DuckDB | Query Parquet files without copying samples into SQL |
| Operational UI | Streamlit + Plotly | Lightweight exploratory dashboard |
| Generator/math | Python + NumPy/Pandas-compatible records | Reproducible lightweight local workload |
| Tests | Pytest + HTTPX | Unit, API, integration and failure tests |

## One-command Docker demo

**Prerequisites:** Docker Engine and Compose v2, available ports **8000** and **8501**.

```bash
git clone <your-repository-url> robostream
cd robostream
# For this downloaded ZIP: unzip and cd robostream instead.
make docker-demo
```

The API is at **http://localhost:8000/docs**, dashboard at **http://localhost:8501**. The demo generates four synthetic sessions (two robots) with controlled faults, ingests them, processes episodes, prints the report and executes a DuckDB sensor query. `data/` is bind-mounted and persists generated files across restarts. Repeated demo invocations create distinct generation folders but reuse the same source checksum/batch for identical seed/configuration.

```bash
docker compose logs -f api
docker compose down                  # leaves persistent data and database volume
docker compose down -v               # DANGER: deletes local PostgreSQL volume
```

The `make init-env` target generates a random local PostgreSQL password in `.env` (excluded from version control). Do not expose this unauthenticated prototype on a public network. To use Compose without Make, run `python scripts/init_env.py` first, then `docker compose up --build -d`.

## Local development without Docker

Requires **Python 3.11+** and packages from `pyproject.toml`; use PostgreSQL for full configuration. SQLite is supported **only as a local/test metadata substitute**.

```bash
python -m venv .venv
source .venv/bin/activate           # Windows: .venv\Scripts\activate
python -m pip install -e '.[dev]'
python scripts/init_env.py            # creates .env with random local DB password
# .env.example defaults to local SQLite; edit it to use PostgreSQL if desired.
alembic upgrade head
python scripts/run_demo.py
uvicorn robostream.api.app:app --host 127.0.0.1 --port 8000
# In another terminal:
streamlit run dashboard/app.py
```

### CLI, step by step

```bash
python -m robostream generate --robots 3 --sessions-per-robot 5 \
  --duration-seconds 60 --frequency-hz 20 --seed 42 \
  --scenario degraded --output data/generated/demo42

python -m robostream ingest --file data/generated/demo42/telemetry.jsonl --source synthetic
# Copy batch_id from output:
python -m robostream process --batch-id <batch-id>
# Copy run_id from output:
python -m robostream report --run-id <run-id>
```

Optional `process --config configs/default.yaml --profile configs/robot_profiles.yaml` and `--force` to make a new traceable run, retaining prior processed data. A non-forced reprocess of an already-completed batch/configuration returns the previous run ID. Config hash includes physical profile, sampling thresholds, quality weights and version. Physical profile changes can invalidate/reprocess different records.

### API examples

```bash
curl http://localhost:8000/health
curl http://localhost:8000/api/v1/robots
curl 'http://localhost:8000/api/v1/episodes?quality_status=excellent&limit=20'
curl http://localhost:8000/api/v1/overview
curl http://localhost:8000/api/v1/analytics/sensors
```

For `POST /api/v1/ingest/batch`, send `{ "source": "api", "records": [ { ...event fields... } ] }`. A concrete valid record:

```json
{
  "event_id": "091d121d-28d2-4959-b1cc-a9e8a60bc8a6",
  "robot_id": "robot_001",
  "session_id": "session_001",
  "timestamp": "2026-01-01T00:00:00Z",
  "sequence_number": 0,
  "event_type": "observation",
  "joint_positions": [0.0, 0.1, -0.2, 0.3, 0.1, -0.1],
  "joint_velocities": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
  "end_effector_position": [0.2, 0.0, 0.3],
  "end_effector_orientation": [0.0, 0.0, 0.0, 1.0],
  "gripper_position": 1.0,
  "gripper_state": "open",
  "task_id": "task_001",
  "metadata": {"phase": "pick"}
}
```

Processing: `POST /api/v1/pipeline/runs` with `{"batch_id": "...", "force": false}`. Inspect `GET /api/v1/pipeline/runs/{run_id}/report`. See [API documentation](docs/api.md) and Swagger `/docs` for the full API.

## Validation rules and episode boundaries

Pydantic checks required fields, UUIDs, enum values, timezone-aware timestamps and event-type-specific requirements. Physical rules use `configs/robot_profiles.yaml` (six joints, joint angle ±π, velocity max 3, gripper [0,1], quaternion norm tolerance 0.05). Events violating physical constraints are excluded from processed Parquet, but retained in raw storage with issue details. Invalid JSON and schema errors are rejected at ingestion.

Arrival-order anomalies (`OUT_OF_ORDER_TIMESTAMP`, repeated sequence numbers) are reported across all event types. Processing uses UTC timestamps and a deterministic sort. Episodes never mix robots or sessions. Explicit task starts, task-ID transitions and terminal status events split segments. Any inter-event inactivity exceeding `gap_seconds` splits a segment; episodes missing terminal task status are `incomplete`. Sampling frequency and sampling-interval anomalies use observation events only, since task/action/state markers are not sensor samples and may share their timestamps. Contradictory terminal events are flagged in the temporal issue ledger. More detail: [pipeline policy](docs/pipeline.md).

### Explainable quality score

The engineering heuristic (not a validated measure of robot data usefulness) starts at **100**, subtracts up to 30 for average observation-field missingness, up to 20 for temporal anomalies per record, up to 15 for duplicates per record, up to 30 for invalid readings per record and 20 for an incomplete episode. Scores are clamped to `[0, 100]`. Labels: `excellent` ≥90, `acceptable` ≥75, `poor` ≥50, `rejected` below 50 or no observation samples. Weights and thresholds are configurable. All penalty components, issue counts, flags and source event IDs are stored with each episode. See [quality scoring](docs/quality_scoring.md).

## Data layout

```text
data/
  generated/     # source generator output, includes manifest.json and sessions.json
  raw/           # immutable input copied by checksum as exact JSONL bytes
  rejected/      # failed ingestion records and line numbers
  processed/     # run=<32-char UUID hex>/<episode UUID>.parquet
  reports/       # <run-id>.json with quality counts, scores, durations and config hash
  manifests/     # checked-in miniature example manifest
```

The default one-Parquet-file-per-episode decision improves per-task retrieval and auditability but can create many small files at scale. If datasets grow, add compaction without changing public API. Metadata tables include `robots`, `sessions`, `ingest_batches`, `event_index` (only IDs/source line pointers; not sensor samples), `pipeline_runs`, `episodes` and `issues`. A content-identical file/API batch is idempotent at ingestion. An event ID seen previously is rejected as a duplicate, **not** silently added twice.

## Dashboard

The Streamlit UI offers:

- **Overview**: robot/session/episode totals, rejected counts, latest pipeline run and a DuckDB sensor summary.
- **Robots & sessions**: robot selector, session statuses and related episodes.
- **Episode inspector**: quality reasons, joint trajectories, gripper state, 3-D end-effector position and sampling intervals. Limits plots to a user-selected number of samples.
- **Data quality**: issue distribution, severity filter, missingness and quality-score histogram.
- **Pipeline runs**: metadata and JSON reports.

Empty states and API failures appear as messages rather than unhandled exceptions. The dashboard accesses the API instead of using direct DB credentials.

## Testing and benchmarking

```bash
python -m pytest -q
ruff check .
python -m robostream benchmark --record-counts 10000,100000,500000 --repeats 3 \
  --seed 42 --output data/reports/benchmark.json
```

The dataset bytes are deterministic for a fixed seed; the manifest's `generated_at_utc` reflects wall-clock invocation time and is intentionally not deterministic.

The benchmark measures generation, ingestion throughput, validation/segmentation duration (in run report), Parquet write size/time, DuckDB query latency and end-to-end time; reports machine/OS/Python and medians. The larger workload can take a while on a student laptop: start with `--record-counts 1000,10000 --repeats 1`. **No performance claims are made without a full measured run.** The bundled `data/reports/generation_benchmark.json` (when available) measures only the installed-dependency-free synthetic generator, not the full pipeline.

## Repository guide

- `src/robostream/generator/synthetic.py` — smooth reproducible telemetry and configurable defects.
- `src/robostream/ingestion/service.py` — raw archive, line-level rejects, global event-ID deduplication.
- `src/robostream/validation/` — semantic/physical checks and arrival-order anomaly reporting.
- `src/robostream/segmentation/episodes.py` — deterministic task episode reconstruction.
- `src/robostream/quality/scoring.py` — rule-based explainable quality metrics.
- `src/robostream/processing.py` — orchestrator with run metadata, reports and atomic output writes.
- `src/robostream/database/` and `alembic/` — relational schema and migration.
- `src/robostream/storage/parquet.py` — typed compressed Parquet per episode.
- `src/robostream/analytics/queries.py` — parameterized DuckDB sensor query.
- `src/robostream/api/app.py` and `dashboard/app.py` — REST and operations UI.
- `tests/` — unit, API, integration, and fault/replay tests.
- `docs/` — design, contracts, operational semantics, scoring, API, benchmarks.
- `IMPLEMENTATION_REPORT.md` — verification results, missing external dependencies, limitations.

## Limitations and next steps

This is **synthetic data only**. There is no ROS/ROS 2 adapter, real camera-frame storage, authentication/authorization, encryption-at-rest, production access policy, live event broker, concurrency coordination for simultaneous ingestion, or distributed processing. Processing currently loads accepted records of one batch in memory, so throughput and memory depend on batch size. Sessions spanning separate batches are segmented independently. Analytics contains a representative sensor query, not a full SQL query builder. A single session with multiple sensors or irregular event types may require more sophisticated sampling-rate estimation and synchronized multi-stream clocks. Raw files are not actively garbage-collected. The quality-score weights are illustrative engineering choices, not empirically calibrated thresholds.

For the latest environment-specific checks and measured generator results, see [VALIDATION_REPORT.md](VALIDATION_REPORT.md). For implementation history and earlier verification, see [IMPLEMENTATION_REPORT.md](IMPLEMENTATION_REPORT.md). Never interpret an unverified Docker setup as a tested deployment.
