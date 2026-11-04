# Architecture and rationale

The local solution deliberately avoids Kafka, Redis, Spark, Kubernetes, cloud services, GPU frameworks and a distributed task queue. Runtime components are Postgres metadata + immutable raw filesystem + versioned PyArrow Parquet + DuckDB + FastAPI + Streamlit.

**Ingestion** accepts a JSONL file or API batch. It computes a SHA-256 content checksum, copies raw bytes into `data/raw/`, and records the batch in Postgres along with a durable event-ID index and line references. Repeated exact-content ingestion reuses the previous batch ID. Record-ID collisions are rejected and logged. Invalid JSON/schema errors are isolated by line.

**Processing** receives a batch ID, not an untrusted client-side file path. It loads only indexed accepted source lines, applies numerical/physical checks, records arrival-time issues, sorts valid records for deterministic segmentation, calculates per-episode quality metrics, writes compressed Parquet by run and episode, generates a JSON report, then commits episode/index metadata and marks the run complete. A failed run records failure and removes uncommitted derived files, never raw input.

**Read path** uses SQLAlchemy for paginated metadata/issue summaries and PyArrow for episode telemetry. DuckDB reads an explicit validated set of Parquet paths rather than dynamically interpolated caller SQL. Streamlit consumes the API; it does not hold Postgres credentials.

**Consistency tradeoff**: The file system and SQL transaction have no true distributed atomic commit. The implementation writes file artifacts before the final SQL commit and marks success last; a crash between file output and commit can leave an orphan derived file that may be cleaned later. It cannot expose a completed run without both required output artifacts and SQL commit in the normal execution path. A concurrent process can still race an ingested identical file; production would require transaction-level advisory locks or a claim protocol. Source raw files are archived first, separately from downstream writes.

**Data layout**: one Parquet per episode under `processed/<32-character run UUID>/<episode UUID>.parquet`. Keeping paths compact avoids Windows' legacy path-length limit; robot, session, and task identifiers remain available through indexed metadata. One-file-per-episode keeps inspection simple; it is less efficient for datasets with millions of tiny episodes. A compaction job would address this at scale.
