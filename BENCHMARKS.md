# Benchmarks — spark-deadlock-detector

Every number carries its method and the commit it was measured at. Estimates are labelled
`ESTIMATE`.

---

## Measurement environment

| | |
|---|---|
| Machine | 8-core ARM64 (aarch64) laptop, 11 GB RAM visible to WSL2 |
| Operating system | Ubuntu on WSL2, Linux 6.6.87 (microsoft-standard-WSL2) |
| Toolchain | Python 3.12.3, PySpark 3.4.1, OpenJDK 17 |
| Topology | Spark `local[4]` — driver and executors in one JVM on one machine |
| Timing | Wall clock around the whole CLI process, so JVM start-up, Spark context creation and report printing are all included |
| Known distortions | No network: every shuffle is local disk and memory, so cluster runs will cost more per shuffle. The synthetic workload has almost every process in a cycle, so pruning removes nearly nothing (a realistic workload is phase 1). Other desktop load was not controlled. |

---

## Headline numbers

| Claim | Current | Before | Commit | Entry |
|---|---|---|---|---|
| End-to-end runtime, 210k-line log, 9,999 deadlocks | **51.2 s** | 216.9 s | `adc1e9f` | §1 |
| Speedup over the original implementation | **4.2×** at 210k lines, 2.1× at 5k | — | `adc1e9f` | §1 |

---

## §1 · Rebuilt pipeline vs the original coursework implementation

**Question this answers:** did the rebuild (exact pruning, each cycle enumerated once, explicit
partition counts, cached search rounds) make it faster without changing the answer?

**Method:** `deadlock-detector LOG -c 4` (current) and `python AOS_Assignment4.py LOG 4`
(original), each a fresh process, run one after another on an otherwise idle machine.
**Workload:** `scripts/generate_log.py` logs — disjoint 10-process deadlock cycles,
identical bytes for both versions.
**Current:** commit `adc1e9f` (`src/` identical to `355b124`), median of 3 runs.
**Original:** `AOS_Assignment4.py` (sha256 prefix `c4e6eac13e6995fe`, not in this repository,
its own `venv/`, Java 17), median of 3 runs.
**Both measured in the same session, original first, then current, back to back,** so drift in
machine state between sessions does not enter the ratio.
**Date:** 12 Oct 2026

| Log lines | Deadlocks | Original (median of 3) | Original runs | Current (median of 3) | Current runs | Speedup |
|---:|---:|---:|---|---:|---|---:|
| 5,247 | 249 | 72.7 s | 78.8 / 72.7 / 71.9 | 34.7 s | 34.7 / 33.7 / 34.8 | 2.1× |
| 52,497 | 2,499 | 94.3 s | 94.3 / 99.1 / 93.9 | 39.9 s | 39.9 / 42.3 / 39.2 | 2.4× |
| 209,997 | 9,999 | 216.9 s | 216.9 / 216.9 / 221.6 | 51.2 s | 51.2 / 54.2 / 50.8 | 4.2× |

**Superseded:** an earlier session measured the original once per size (69.6 / 95.3 / 221.3 s) and the
current version at `355b124` (medians 36.8 / 41.7 / 51.6 s, giving 4.3×). The same-session medians
above replace them. Single runs drift by about 10% between sessions; the 5k original went from 69.6 s to 72–79 s.

Both versions report the same number of cycles at every size.

**Reading:** the current version's runtime barely moves across a 40× increase in input. The
original's grows roughly linearly, so the gap widens with size.

**Why it came out this way:** the original enumerated each 10-process cycle 10 times (once per
rotation), and its iterative joins compounded partition counts round after round. Both costs
scale with input. What remains in the current version is mostly fixed per-round overhead: about
5 pruning rounds plus 19 search hops, each a few Spark jobs.

**Stage split, current, 210k lines (single run, `-c 4`):** build RAG 5.3 s · prune 18.1 s ·
cycle search 22.0 s · the rest is JVM and Spark start-up.

---

## §2 · Core count on a small input

**Method:** `deadlock-detector rag_5k.log -c N -v`, single run each, commit `2d30dae` (same
algorithm as `355b124`).

| Cores | Partitions | Total |
|---:|---:|---:|
| 2 | 4 | 27.2 s |
| 8 | 16 | 49.6 s |

**Reading:** on 5k lines, more cores make it *slower*. The work per round is tiny, so extra
partitions only add per-task scheduling and Python-worker overhead. This is the honest answer
to "why Spark for 9 MB?": at this size it isn't faster. Phase 1 measures where it starts to win.

---

## §3 · "Why Spark?" — runtime vs log size, Spark vs a single process

**Question this answers:** at what log size, if any, does Spark beat a good single-process
implementation on this laptop?

**Method:** `scripts/benchmark.py`, one fresh process per run, wall clock from outside (JVM and
Spark start-up included), median of 3 runs.
- **Workload:** `scripts/generate_realistic_log.py --seed 42` (D9), 30 days, about 2% of processes deadlocked.
- **Spark:** RDD engine, `local[8]`, 16 partitions, `--driver-memory 6g`, `--max-paths 10000000`.
- **Baseline:** `deadlock-detector-baseline` (networkx, D10), capped at 9 GB of address space by `prlimit`.
- **Correctness:** every run is checked against the ground-truth file. All 31 successful runs below
  matched exactly: every cycle and its formation time, no truncation.
- **Commits:** baseline `2527486` (0.2M, 0.6M) and `0764bab` (2M, 6M); Spark `f176358`. The `-dirty`
  suffix in the CSV comes from uncommitted docs and tests only; `git diff f176358 b1806a4 -- src` is empty.
- **Raw rows:** `benchmarks/results.csv`
- **Date:** 12 Oct 2026

| Log lines | Size | Cycles | networkx, 1 process | Baseline peak memory | Spark RDD, 8 cores | Spark runs |
|---:|---:|---:|---:|---:|---:|---|
| 200,037 | 9 MB | 612 | **2.5 s** | 0.3 GB | 62.5 s | 63.2 / 59.9 / 62.5 |
| 600,108 | 27 MB | 1,776 | **8.1 s** | 0.7 GB | 67.1 s | 67.1 / 65.2 / 67.5 |
| 2,000,333 | 90 MB | 5,834 | **30.1 s** | 2.3 GB | 87.3 s | 87.3 / 90.7 / 83.7 |
| 6,001,000 | 281 MB | 17,587 | **107.5 s** | 7.2 GB | 144.9 s | 146.0 / 143.6 / 144.9 |
| 20,003,350 | 936 MB | 59,047 | **out of memory** (`MemoryError` after 154 s, 8.8 GB) | — | **374.4 s** | 374.4 / 370.3 / 391.8 |

**Reading:** at every size the single process can hold in memory, it is faster than Spark: 25×
faster at 0.2M lines and still 1.35× faster at 6M. It cannot run the 20M-line log at all. Spark
finishes that log in 6.2 minutes with the exact answer. **The crossover is memory, not speed.**
The baseline needs about 1.2 KB per log line (Python dicts of strings, then networkx's
dict-of-dicts graph), so on an 11 GB machine it stops somewhere between 6M and 20M lines.
Spark's per-partition working set is bounded and spills to disk, so its runtime keeps growing
sub-linearly: 3.3× the data (6M → 20M) cost 2.6× the time.

**Why it came out this way:** Spark pays a fixed cost of about 60 s on this workload. That covers
JVM start-up, plus 6 pruning rounds and 27 search hops, each made of several jobs. That cost is
why it loses at small sizes. The baseline is linear in the input with a small constant, but it is
bounded by RAM.

**Stage split, Spark RDD, 8 cores (median):**

| Log lines | Build RAG | Prune | Cycle search |
|---:|---:|---:|---:|
| 0.2M | 5.9 s | 21.1 s | 29.5 s |
| 2M | 13.6 s | 33.2 s | 32.7 s |
| 6M | 30.6 s | 63.6 s | 41.6 s |
| 20M | 91.9 s | 197.2 s | 73.4 s |

Pruning becomes the bottleneck as size grows. Each round joins the full wait-for graph (7.1M
edges at 20M lines) against the set of surviving processes, and on this workload it takes 6
rounds. The search stays small because pruning has already cut the graph to about 2% of processes.

**Not yet tested (the obvious attack):** a leaner single process, with integer ids, arrays and
`scipy.sparse.csgraph` SCCs, would move the memory wall, possibly past 20M lines.
`ESTIMATE`: about 5–10× less memory per edge.

---

## §4 · Core scaling on the 6M-line log

**Method:** as §3, Spark RDD engine, `real_6m.log`, median of 3.

**First attempt (confounded — kept because the confound is the lesson):** partitions defaulted to
2 × cores, so changing the core count also changed the partition count.

| Cores | Partitions | Median | Runs | Build / prune / search |
|---:|---:|---:|---|---|
| 1 | 2 | 842.1 s | 718.8 / 1001.4 / 842.1 | 321.9 / 329.4 / 171.6 |
| 2 | 4 | 256.2 s | 256.4 / 256.2 / 255.6 | 59.2 / 149.1 / 42.6 |
| 4 | 8 | 150.1 s | 150.1 / 149.8 / 151.2 | 36.4 / 72.0 / 34.8 |
| 8 | 16 | 144.9 s | 146.0 / 143.6 / 144.9 | 30.6 / 63.6 / 41.6 |

1 → 2 cores gave 3.3×, which is impossible from parallelism alone. With 2 partitions, each task
held about 3M edges: the build stage alone went from 59 s to 322 s, and the runs spread by 40%.
That is memory pressure (spill and garbage collection) from oversized partitions, not a
parallelism effect. See error log E8. The fixed-partition re-run is below.

## Curves to produce in phase 1

| Curve | Why it earns its place |
|---|---|
| Runtime vs log size (0.2M → 2M → 20M lines), Spark vs single-process networkx | Finds the crossover point — the answer to "why Spark?" |
| Runtime vs cores (1/2/4/8) at the largest size | Strong scaling: whether the parallelism is real |
| Stage split (RAG / prune / search) vs size | Shows which stage becomes the bottleneck |
| RDD engine vs DataFrame engine (phase 3) | Justifies the rewrite with a number |

---

## Numbers not yet measured

| Not measured | Why not | Expectation |
|---|---|---|
| Single-machine baseline | Phase 1 | `ESTIMATE`: networkx finishes the 210k log in a few seconds, beating Spark well past this size |
| Anything on a real cluster | No Docker yet (phase 4) | More cost per shuffle than local mode |
| Memory use | Not instrumented | Driver-side `collect` of cycles is the first risk at scale |
