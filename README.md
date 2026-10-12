# Spark Deadlock Detector

[![tests](https://github.com/ChaitanyaShah21/spark_deadlock_detector/actions/workflows/ci.yml/badge.svg)](https://github.com/ChaitanyaShah21/spark_deadlock_detector/actions/workflows/ci.yml)

Detects deadlocks in large resource-allocation logs with Apache Spark (PySpark RDDs).
The log is turned into a Resource Allocation Graph (RAG). The detector then prunes every process that cannot be on a cycle and enumerates the remaining deadlock cycles with iterative distributed joins.

```
$ deadlock-detector data/sample.log
Resource Allocation Graph
  processes  9
  resources  9
  edges      18

Deadlocks: 3 cycle(s)
  P101 -> R002 -> P106 -> R006 -> P101   (formed 2025-10-31 21:07:56)
  P105 -> R003 -> P107 -> R007 -> P105   (formed 2025-11-02 12:07:56)
  P102 -> R005 -> P103 -> R009 -> P108 -> R004 -> P102   (formed 2025-11-02 00:07:58)
  deadlocked processes (7): P101 P102 P103 P105 P106 P107 P108

Deadlocks formed per day
  2025-10-31  1
  2025-11-02  2

Most contended resources (distinct waiting processes)
  R001  2
  ...
```

## Problem

Each log line is a timestamped event:

```
2025-10-31 21:07:53   WAIT   P106   R006     # P106 requests R006       -> edge P106 -> R006
2025-10-31 21:07:54   HOLD   P101   R006     # P101 is granted R006     -> edge R006 -> P101
```

With single-instance resources, the system is deadlocked exactly when the RAG contains a cycle `P -> R -> P -> R -> ... -> P`.
For every cycle the detector reports its processes and resources. It also reports when the cycle formed: the time its last edge first appeared.
Finally it lists the deadlocked processes, a per-day timeline of deadlocks, and contention hotspots.

## How it works

```
log lines ──parse──▶ RAG edges ((src,dst), first_seen)          reduceByKey(min)
                        │
                        ▼
           wait-for edges  Pw ─▶ Ph  (Pw waits on R, Ph holds R)  join on resource
                        │
                        ▼
           prune: repeatedly drop processes with in- or        2 joins / round,
           out-degree 0 in the wait-for graph                   to a fixpoint
                        │
                        ▼
           candidate sub-RAG (only edges on surviving
           wait-for edges)
                        │
                        ▼
           cycle search: frontier of open paths, keyed by       1 join / round,
           last node, joined with adjacency lists               one hop per round
```

1. **Parsing** ([parsing.py](src/deadlock_detector/parsing.py)) accepts tab- or space-separated lines in either `HOLD P R` or `HOLD R P` order. It skips malformed lines.
2. **Pruning** ([graph.py](src/deadlock_detector/graph.py)) uses the fact that a process with no incoming or no outgoing wait-for edge can't be on a cycle.
   Removing such processes can strand others, so the pruning repeats until nothing changes.
   This never removes a deadlocked process. On realistic logs it throws away most of the graph before the expensive search starts.
3. **Cycle search** ([cycles.py](src/deadlock_detector/cycles.py)) seeds one path per `P -> R` edge and grows every path by one hop per round with a distributed join.
   A path only visits processes whose id sorts after its starting process. As a result, each cycle is found exactly once (from its smallest process) in canonical form, and redundant rotations never fill the frontier.
   The search is bounded by `--max-cycle-length`. If the frontier grows past `--max-paths`, the search stops and the report flags the result as possibly incomplete.

## Usage

Requires Python 3.9+ and **Java 8, 11 or 17**. Spark 3.4 does not start on Java 21+.

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e '.[dev]'
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64   # if your default java is newer

deadlock-detector data/sample.log                      # all local cores
deadlock-detector LOG --cores 8 --max-cycle-length 13 --json report.json -v
```

| Option | Default | |
|---|---|---|
| `-c, --cores` | all | local Spark cores |
| `--partitions` | 2 × cores | RDD partitions |
| `--max-cycle-length` | 13 | longest cycle searched for, in processes |
| `--max-paths` | 200000 | stop the search if more open paths remain |
| `--top` | 5 | hotspots to list |
| `--json PATH` | | write the full report (every cycle) as JSON |
| `-v` | | per-stage progress |

## Tests

```bash
pytest
```

The tests cover the parser, hand-built graphs (a chain feeding into a cycle, two cycles sharing a process, the length limit, resource-first lines) and the sample log.
They also compare the Spark output, cycles and formation times included, against an independent brute-force DFS on random graphs.
A further test checks it against the ground-truth file of a realistic generated log.

## Benchmarks

`scripts/generate_benchmark_logs.sh` writes synthetic logs with a known number of 10-process deadlocks (see [generate_log.py](scripts/generate_log.py)).
In those logs every process is deadlocked.
[generate_realistic_log.py](scripts/generate_realistic_log.py) writes a harder and more realistic workload, along with a ground-truth file:
- about 2% of processes are deadlocked;
- the other processes sit in blocked chains and trees that pruning has to remove;
- cycles are 2–10 processes long, and some cycles share processes;
- some chains bridge two deadlocks;
- some WAITs are retried, and some lines are malformed.

End-to-end wall time, including Spark startup, with `local[4]` on an 8-core ARM64 laptop (WSL2, 11 GB RAM).
The "original" column is the first version of this code, before the rewrite described below. Both versions find the same cycles.
Median of 3 runs for each version, measured back to back in one session. Method and raw runs are in [BENCHMARKS.md](BENCHMARKS.md).

| Log lines | Deadlocks | Original | Current | Speedup |
|---:|---:|---:|---:|---:|
| 5,247 | 249 | 72.7 s | 34.7 s | 2.1× |
| 52,497 | 2,499 | 94.3 s | 39.9 s | 2.4× |
| 209,997 | 9,999 | 216.9 s | 51.2 s | 4.2× |

What changed:
- **Exact pruning** replaced a connected-components heuristic. That heuristic stopped after 10 label-propagation rounds and could miss cycle members in long chains.
- **Each cycle is enumerated once, not once per process on it.**
- **Explicit partition counts on every shuffle.** A PySpark `join` without one has as many partitions as both inputs combined, so the old iterative loops grew from 8 to 88 partitions.
- **Each search round's join is cached and reused,** instead of being recomputed for every action.
- **Unused diagnostic jobs removed.**

**Limits.** At these sizes, fixed per-round Spark overhead dominates: the search needs one shuffle round per hop of the longest cycle. Run time therefore grows slowly with input size, and more cores do not help on small logs. The 5k log runs faster on 2 cores than on 8.

## Design

Decisions, rejected alternatives and the planned upgrades are in [ARCHITECTURE.md](ARCHITECTURE.md).

## Layout

```
src/deadlock_detector/   parsing, graph construction + pruning, cycle search, report, CLI
tests/                   unit tests + brute-force cross-check
scripts/                 synthetic log generator, benchmark log script
data/                    small sample logs
```
