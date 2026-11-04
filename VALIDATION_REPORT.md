# RoboStream Validation Report

**Run date:** 2026-11-04
**Environment:** Windows 11, Python 3.13.3
**Scope:** Current workspace source. Local SQLite-backed API, ingestion, processing, Parquet, DuckDB, and dashboard were exercised. Docker Compose with PostgreSQL was not verified because Docker is unavailable.

## Executive result

RoboStream is **running and verified locally with SQLite**. The synthetic JSONL generator ran, emitted deterministic data for a fixed seed, and scaled through a 50,000-observation workload. The documented demo completed ingestion, validation, segmentation, quality scoring, Parquet writes, and DuckDB analytics. The FastAPI service and Streamlit dashboard both started and responded locally.

The project virtual environment was installed from `pyproject.toml`. PostgreSQL and the Docker-based deployment remain unverified; this run used SQLite. The benchmark below is generator-only and does not represent end-to-end performance.

## Checks and results

| Check | Result | Evidence / limits |
|---|---|---|
| Python source compilation | Passed | `python -m compileall -q src dashboard scripts tests` |
| Deterministic generation | Passed | Two seed-42 degraded outputs each contained 418 records and had identical SHA-256: `3a03e73114baa04c9bf4ee34b436403e144577ed763b3cbca224f58d32437a81` |
| Full local demo | Passed | 418 input events → 416 ingested → 398 processed → 18 episodes, with real Parquet and DuckDB. See details below. |
| FastAPI | Passed | `/health`, `/api/v1/overview`, `/api/v1/episodes`, `/api/v1/analytics/sensors`, and `/docs` returned HTTP 200. |
| Streamlit dashboard | Passed | `http://127.0.0.1:8501/` returned HTTP 200. |
| Alembic / SQLite | Passed | `alembic upgrade head`; current revision is `0001_initial (head)`. |
| Pytest suite | Passed | **28 passed** in 9.57 seconds. One upstream Starlette/httpx deprecation warning. |
| Ruff | Passed | `ruff check .` → `All checks passed!` |
| Focused checks from the previous review | Passed | 20 Hz observations with task markers at the sample boundary measured 20 Hz without a false duplicate-timestamp issue; analytic midpoint velocity matched the smoothstep derivative. |
| Windows path safety / Parquet path handling | Passed after fixes | Absolute, rooted, drive-qualified, UNC, and parent-relative paths are rejected. Compact per-run Parquet paths avoid the Windows path-length failure found by tests. |
| Docker / PostgreSQL | Not run | `docker` executable is not installed. SQLite was used for local metadata. |

## Generator-only throughput

Each workload used one robot, one session, one task, clean scenario, 20 Hz, and three independent seeds. Generation time includes synthetic record construction, JSONL serialization, file writes, and the generator's checksum pass. Throughput is **total JSONL events per second**, including the two task boundary events. This measures generation only; it is not ingestion or pipeline throughput.

| Observation samples | Total events | Median generation time | Median total events/s | Median JSONL size |
|---:|---:|---:|---:|---:|
| 1,000 | 1,002 | 0.041662 s | 24,050.9 | 547,568 bytes |
| 10,000 | 10,002 | 0.257132 s | 38,898.2 | 5,468,249 bytes |
| 50,000 | 50,002 | 1.184387 s | 42,217.6 | 27,256,718 bytes |

These are local, single-process measurements from one machine and are sensitive to its filesystem, cache, and runtime. They should be labeled **synthetic generator throughput**, not system throughput or a production performance claim.

## Degraded-data core smoke run

Configuration: 2 robots × 2 sessions per robot, 10 seconds/session, 10 Hz, 2 tasks/session, seed 42, degraded scenario, default 1.5% defect probability.

| Observation | Measured result |
|---|---:|
| Input records | 418 |
| Schema rejects | 0 |
| Physical validation issues | 18 |
| Records retained after physical checks | 400 |
| Temporal issue events | 98 |
| Segmented episodes | 18 |
| Episode labels | 8 excellent, 1 acceptable, 1 poor, 8 rejected |
| Mean rule-based quality score | 62.59 / 100 |

The physical issue events were 7 non-finite sensor values, 6 joint-limit violations, and 5 invalid quaternions. Temporal issue events included 17 out-of-order timestamps, 6 sequence regressions, 5 repeated sequence numbers, 30 missing-sequence notices, 30 irregular-sampling notices, 8 repeated observation timestamps, and 2 timestamp gaps. These are issue counts, not necessarily counts of distinct bad records. The quality score is a configurable engineering heuristic, not a scientifically validated measure of demonstration usefulness.

This isolated smoke run was performed before the project virtual environment was installed. It imported the actual Pydantic schemas and validation/segmentation/scoring code; a minimal in-process import stub stood in for the then-missing `pydantic-settings` package to load shared configuration classes. The later full local demo ran with the real installed dependencies and settings implementation.

## Full local demo results

The project dependencies were installed into the ignored `.venv` directory. `alembic upgrade head` created the local SQLite schema, then `python scripts/run_demo.py` completed the real local pipeline.

| Metric | Result |
|---|---:|
| Robots / sessions | 2 / 4 |
| Input events | 418 |
| Accepted / duplicate rejects at ingestion | 416 / 2 |
| Physical processing rejects | 18 |
| Processed Parquet rows | 398 |
| Episodes | 18 (8 excellent, 1 acceptable, 1 poor, 8 rejected) |
| Parquet files written and checked | 18 / 18 |
| Total Parquet size | 162,621 bytes |
| Pipeline elapsed time | 1.280 s |
| Parquet write time | 1.204 s |
| DuckDB rows summarized | 2 robot groups; 197 + 201 = 398 records |
| API and dashboard | HTTP 200 checks passed |

The API and dashboard are currently listening on `127.0.0.1:8000` and `127.0.0.1:8501` in the local environment. PostgreSQL-specific behavior and Compose startup remain untested.

## Runtime dependency availability

The declared Python dependencies are installed in the project `.venv`. Docker is not installed, so Compose/PostgreSQL verification remains outstanding.

To run the already-verified local services again, activate the environment and start:

```powershell
.venv\Scripts\Activate.ps1
alembic upgrade head
uvicorn robostream.api.app:app --host 127.0.0.1 --port 8000
# In a second terminal:
streamlit run dashboard/app.py --server.address 127.0.0.1 --server.port 8501
```

Run checks with `python -m pytest -q` and `ruff check .`. Docker Compose with PostgreSQL is an additional deployment mode and was not tested here.

## Resume-safe wording

If you want to include measured numbers before full-stack verification, use wording that identifies the scope:

> Built a robotics telemetry processing prototype with deterministic synthetic data, rule-based quality scoring, Parquet storage, and DuckDB analytics; verified a local SQLite-backed demo processing 418 events into 398 Parquet records across 18 episodes, and measured synthetic generation at a median 42.2k events/s for 50,000 observations across three local runs.

Do not describe 42.2k events/s as ingestion or processing throughput. The local demo verified Parquet and DuckDB with SQLite metadata; do not claim PostgreSQL or Docker deployment was verified by this report.
