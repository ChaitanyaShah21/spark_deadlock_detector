import argparse
import json
import os
import sys
import time
from dataclasses import asdict

from .detector import create_spark, detect_deadlocks
from .df_engine import detect_deadlocks_df

MAX_PRINTED_CYCLES = 20


def parse_args(argv=None):
    p = argparse.ArgumentParser(
        prog="deadlock-detector",
        description="Detect deadlocks in a resource-allocation log with Apache Spark.",
    )
    p.add_argument("log", help="log file of WAIT/HOLD events")
    p.add_argument("--engine", choices=("rdd", "df"), default="rdd",
                   help="RDD pipeline or DataFrame / Spark SQL pipeline (default: rdd)")
    p.add_argument("-c", "--cores", type=int, default=os.cpu_count(), help="local Spark cores (default: all)")
    p.add_argument("--partitions", type=int, help="RDD partitions (default: 2 x cores)")
    p.add_argument("--max-cycle-length", type=int, default=13, metavar="N",
                   help="longest cycle to search for, in processes (default: 13)")
    p.add_argument("--max-paths", type=int, default=200_000,
                   help="stop the search if more open paths than this remain (default: 200000)")
    p.add_argument("--top", type=int, default=5, help="hotspots to report (default: 5)")
    p.add_argument("--json", metavar="PATH", help="also write the full report as JSON")
    p.add_argument("--driver-memory", default="4g")
    p.add_argument("-v", "--verbose", action="store_true", help="print per-stage progress")
    return p.parse_args(argv)


def print_report(report, total_seconds):
    print("Resource Allocation Graph")
    print(f"  processes  {report.processes}")
    print(f"  resources  {report.resources}")
    print(f"  edges      {report.edges}")

    print(f"\nDeadlocks: {len(report.cycles)} cycle(s)")
    for cycle in report.cycles[:MAX_PRINTED_CYCLES]:
        print(f"  {' -> '.join(cycle.nodes + cycle.nodes[:1])}   (formed {cycle.formed_at})")
    if len(report.cycles) > MAX_PRINTED_CYCLES:
        print(f"  ... {len(report.cycles) - MAX_PRINTED_CYCLES} more (use --json for the full list)")
    if report.truncated:
        print("  WARNING: search stopped at --max-paths; longer cycles may be missing")
    if report.deadlocked_processes:
        print(f"  deadlocked processes ({len(report.deadlocked_processes)}): "
              + " ".join(report.deadlocked_processes[:50])
              + (" ..." if len(report.deadlocked_processes) > 50 else ""))

    if report.deadlocks_per_day:
        print("\nDeadlocks formed per day")
        for day, n in report.deadlocks_per_day:
            print(f"  {day}  {n}")

    print("\nMost contended resources (distinct waiting processes)")
    for r, n in report.top_resources:
        print(f"  {r}  {n}")
    print("\nProcesses waiting on the most resources")
    for p, n in report.top_processes:
        print(f"  {p}  {n}")

    stages = ", ".join(f"{k} {v:.2f}s" for k, v in report.timings.items())
    print(f"\nTime: {total_seconds:.2f}s total ({stages})")


def main(argv=None):
    args = parse_args(argv)
    if not os.path.isfile(args.log):
        sys.exit(f"error: {args.log} not found")

    start = time.perf_counter()
    spark = create_spark(args.cores, args.driver_memory)
    try:
        if args.engine == "df":
            run, target = detect_deadlocks_df, spark
        else:
            run, target = detect_deadlocks, spark.sparkContext
        report = run(
            target,
            args.log,
            partitions=args.partitions or 2 * args.cores,
            max_cycle_processes=args.max_cycle_length,
            max_paths=args.max_paths,
            top=args.top,
            log=print if args.verbose else (lambda *_: None),
        )
    finally:
        spark.stop()

    total = time.perf_counter() - start
    print_report(report, total)
    if args.json:
        with open(args.json, "w") as f:
            json.dump({**asdict(report), "total_seconds": total}, f, indent=2)


if __name__ == "__main__":
    main()
