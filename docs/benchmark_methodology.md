# Benchmark methodology

Run `python -m robostream benchmark --record-counts 10000,100000,500000 --repeats 3 --seed 42 --output data/reports/benchmark.json` after installing all dependencies, migrating the database and ensuring sufficient disk space. Optionally start with 1000/10000.

For each dataset size and repeat, create one new deterministic seed derived from size/base-seed/repetition, run the clean generator, ingest JSONL with persistent event-index deduplication, validate+segment+write Parquet, issue a DuckDB sensor-summary query, and record elapsed time measured with `time.perf_counter()`. Report actual (not nominal) generated counts, ingestion records/sec, processing records/sec, Parquet write duration/bytes, DuckDB query latency, end-to-end time and median times by size. The report includes OS, machine and Python version. The pipeline JSON report supplies measured validation and segmentation durations.

The benchmark measures **wall-clock local prototype performance**, not load-tested concurrent serving or real-time guarantees. SQLite versus Postgres, Zstd settings, disk cache, dataset complexity and the single batch/session size strongly influence results. Repetition-specific data seeds ensure each ingest is a new checksum rather than a no-op idempotent replay. Start small on memory-constrained machines; a large batch currently holds validated records in memory.

A generation-only benchmark can run without optional storage dependencies; if included, it must be clearly marked as measuring generation alone. Full end-to-end throughput figures must come from a completed run with both Parquet and DuckDB available. Never treat mocked Parquet writer tests as a storage benchmark.
