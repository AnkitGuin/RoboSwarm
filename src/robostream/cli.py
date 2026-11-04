"""CLI entry points for deterministic generation, ingestion, processing and benchmarks."""
from __future__ import annotations
import argparse
import json
from pathlib import Path

from robostream.config import Settings, load_config, load_profile
from robostream.database.session import session_factory
from robostream.generator.synthetic import GeneratorOptions, generate
from robostream.ingestion.service import ingest_file
from robostream.processing import run_pipeline


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="robostream", description="RoboStream tools")
    commands = parser.add_subparsers(dest="command", required=True)
    gen = commands.add_parser("generate", help="Deterministic synthetic JSONL dataset")
    gen.add_argument("--robots", type=int, default=3)
    gen.add_argument("--sessions-per-robot", type=int, default=2)
    gen.add_argument("--duration-seconds", type=float, default=15)
    gen.add_argument("--frequency-hz", type=float, default=20)
    gen.add_argument("--seed", type=int, default=42)
    gen.add_argument("--scenario", choices=["clean", "degraded"], default="degraded")
    gen.add_argument("--joint-count", type=int, default=6)
    gen.add_argument("--noise", type=float, default=0.005)
    gen.add_argument("--tasks-per-session", type=int, default=2)
    gen.add_argument("--defect-probability", type=float, default=0.015)
    gen.add_argument("--output", type=Path, default=Path("data/generated/demo"))
    ingest = commands.add_parser("ingest", help="Ingest JSONL with audit trail")
    ingest.add_argument("--file", type=Path, required=True)
    ingest.add_argument("--source", default="synthetic")
    process = commands.add_parser("process", help="Validate/segment batch into Parquet")
    process.add_argument("--batch-id", required=True)
    process.add_argument("--force", action="store_true")
    process.add_argument("--config", type=Path)
    process.add_argument("--profile", type=Path)
    report = commands.add_parser("report", help="Display run quality report")
    report.add_argument("--run-id", required=True)
    benchmark = commands.add_parser("benchmark", help="Measure local pipeline")
    benchmark.add_argument("--record-counts", default="1000,10000,100000")
    benchmark.add_argument("--repeats", type=int, default=1)
    benchmark.add_argument("--seed", type=int, default=42)
    benchmark.add_argument("--output", type=Path, default=Path("data/reports/benchmark.json"))
    return parser


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    settings = Settings()
    if args.command == "generate":
        config = GeneratorOptions(robots=args.robots, sessions_per_robot=args.sessions_per_robot,
                                  duration_seconds=args.duration_seconds,
                                  frequency_hz=args.frequency_hz, seed=args.seed,
                                  scenario=args.scenario, joint_count=args.joint_count,
                                  noise=args.noise, tasks_per_session=args.tasks_per_session,
                                  defect_probability=args.defect_probability)
        result = generate(args.output, config)
    elif args.command == "benchmark":
        from robostream.benchmarking.runner import benchmark_pipeline
        result = benchmark_pipeline([int(x) for x in args.record_counts.split(",")],
                                    args.repeats, args.seed, args.output, settings)
    else:
        db_factory = session_factory(settings)
        with db_factory() as db:
            if args.command == "ingest":
                result = ingest_file(args.file, args.source, db, settings)
            elif args.command == "process":
                result = run_pipeline(args.batch_id, db, settings,
                                      load_config(args.config) if args.config else None,
                                      load_profile(args.profile) if args.profile else None,
                                      force=args.force)
            else:
                from robostream.database.models import PipelineRun
                run = db.get(PipelineRun, args.run_id)
                if not run or not run.report_path:
                    raise SystemExit("Run not found or report not ready")
                result = json.loads((settings.data_dir / run.report_path).read_text())
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
