#!/usr/bin/env python3
"""Time the Spark pipeline or the single-process baseline on a generated log.

Each run is a fresh process, timed by wall clock from outside, so JVM and Spark
start-up are included. Peak memory (baseline only) comes from /usr/bin/time; for
Spark it would only see the Python driver, not the JVM, so it is left blank. Every run is checked against the log's
ground-truth file, and one CSV row is appended per run.

    python scripts/benchmark.py spark data/generated/real_2m.log --cores 4 --runs 3
    python scripts/benchmark.py spark-df data/generated/real_2m.log --cores 4 --runs 3
    python scripts/benchmark.py baseline data/generated/real_2m.log --runs 3
"""

import argparse
import csv
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BIN = Path(sys.executable).parent
FIELDS = ["date", "commit", "engine", "log", "lines", "cores", "run", "status", "wall_s", "peak_mb",
          "build_s", "prune_s", "search_s", "cycles", "correct", "partitions"]


def commit():
    sha = subprocess.run(["git", "rev-parse", "--short", "HEAD"], cwd=ROOT, capture_output=True, text=True).stdout.strip()
    dirty = subprocess.run(["git", "status", "--porcelain", "--untracked-files=no"], cwd=ROOT,
                           capture_output=True, text=True).stdout.strip()
    return sha + ("-dirty" if dirty else "")


def command(engine, log, out, args):
    if engine == "baseline":
        cmd = [str(BIN / "deadlock-detector-baseline"), log, "--json", out]
        # fail with MemoryError instead of driving the machine into swap
        return ["prlimit", f"--as={args.mem_limit_gb << 30}", "--"] + cmd
    return [str(BIN / "deadlock-detector"), log, "--engine", "df" if engine == "spark-df" else "rdd", "-c", str(args.cores), "--max-paths", str(args.max_paths),
            "--partitions", str(args.partitions), "--driver-memory", args.driver_memory, "--json", out]


def stages(engine, result):
    t = result["timings"]
    if engine == "baseline":
        return t["parse"] + t["build_graph"], t["scc"], t["cycle_search"]
    return t["build_rag"], t["prune"], t["cycle_search"]


def check(result, truth):
    cycles = result["cycles"]
    got = sorted((tuple(c["nodes"] if isinstance(c, dict) else c[0]),
                  c["formed_at"] if isinstance(c, dict) else c[1]) for c in cycles)
    want = sorted((tuple(n), f) for n, f in truth["cycle_list"])
    return got == want and not result.get("truncated", False)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("engine", choices=("spark", "spark-df", "baseline"))
    ap.add_argument("log")
    ap.add_argument("--cores", type=int, default=4)
    ap.add_argument("--partitions", type=int, help="shuffle partitions (default: 2 x cores)")
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--max-paths", type=int, default=10_000_000)
    ap.add_argument("--driver-memory", default="6g")
    ap.add_argument("--mem-limit-gb", type=int, default=9, help="address-space cap for the baseline (default: 9)")
    ap.add_argument("--out", default=str(ROOT / "benchmarks" / "results.csv"))
    args = ap.parse_args()

    truth = json.load(open(args.log + ".truth.json"))
    args.partitions = args.partitions or 2 * args.cores
    env = {**os.environ, "JAVA_HOME": os.environ.get("JAVA_HOME", "/usr/lib/jvm/java-17-openjdk-arm64")}
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    new = not Path(args.out).exists()
    sha = commit()

    with open(args.out, "a", newline="") as f:
        writer = csv.DictWriter(f, FIELDS)
        if new:
            writer.writeheader()
        for run in range(1, args.runs + 1):
            with tempfile.TemporaryDirectory() as tmp:
                out, timing = os.path.join(tmp, "result.json"), os.path.join(tmp, "time.txt")
                start = time.perf_counter()
                proc = subprocess.run(["/usr/bin/time", "-f", "%M", "-o", timing] + command(args.engine, args.log, out, args),
                                      env=env, capture_output=True, text=True)
                wall = time.perf_counter() - start
                peak_mb = int(open(timing).read().split()[-1]) / 1024
                row = {"date": time.strftime("%Y-%m-%d %H:%M"), "commit": sha, "engine": args.engine,
                       "log": Path(args.log).name, "lines": truth["lines"],
                       "cores": 1 if args.engine == "baseline" else args.cores, "run": run,
                       "wall_s": f"{wall:.1f}", "peak_mb": f"{peak_mb:.0f}" if args.engine == "baseline" else "",
                       "partitions": "" if args.engine == "baseline" else args.partitions}
                if proc.returncode == 0 and os.path.exists(out):
                    result = json.load(open(out))
                    build, prune, search = stages(args.engine, result)
                    row.update(status="ok", build_s=f"{build:.1f}", prune_s=f"{prune:.1f}", search_s=f"{search:.1f}",
                               cycles=len(result["cycles"]), correct=check(result, truth))
                else:
                    tail = (proc.stderr or proc.stdout).strip().splitlines()[-1:] or ["?"]
                    row.update(status="MemoryError" if "MemoryError" in proc.stderr else f"exit {proc.returncode}")
                    print(f"  failed: {tail[0][:200]}", file=sys.stderr)
            writer.writerow(row)
            f.flush()
            print(" ".join(f"{k}={row.get(k, '')}" for k in FIELDS[2:]), file=sys.stderr)


if __name__ == "__main__":
    main()
