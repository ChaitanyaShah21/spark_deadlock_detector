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
| 20M-line log (936 MB, 59,047 deadlocks), DataFrame engine, 8 cores | **170.8 s** | 374.4 s (RDD engine) | `b1806a4` / `f176358` | §5 |
| DataFrame engine vs RDD engine | **2.2× faster** at every size, 0.2M–20M lines | — | `b1806a4` | §5 |
| Single process (networkx) on the same 20M log | **out of memory** at 8.8 GB; 107.5 s at 6M vs 71.5 s for the DataFrame engine | — | `0764bab` | §3 |
| Strong scaling, 1 → 8 cores, 6M lines, RDD | **3.9×** (564.7 s → 144.9 s) | — | `b1806a4` / `f176358` | §4 |

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

**Re-run with partitions fixed at 16** (commit `b1806a4`; the 8-core row is the §3 run, which
already used 16 partitions). Figure: `docs/figures/core_sweep_{light,dark}.png`.

| Cores | Median | Runs | Build / prune / search | Speedup vs 1 core |
|---:|---:|---|---|---:|
| 1 | 564.7 s | 564.7 / 561.0 / 566.4 | 120.7 / 257.5 / 178.2 | 1.0× |
| 2 | 294.2 s | 294.2 / 288.9 / 295.8 | 62.1 / 132.4 / 90.9 | 1.9× |
| 4 | 178.5 s | 178.5 / 179.0 / 173.4 | 38.3 / 78.9 / 52.9 | 3.2× |
| 8 | 144.9 s | 146.0 / 143.6 / 144.9 | 30.6 / 63.6 / 41.6 | 3.9× |

**Reading:** near-linear to 2 cores, then flattening; 4 → 8 cores buys only 1.23×. The 1-core
time is now stable (spread under 1%), which confirms that the first sweep's spread came from the
2-partition configuration.

**Why it flattens:**
- **The driver-side loop is serial.** 6 pruning rounds and 27 search hops run one after another,
  each as several Spark jobs with a scheduling floor that more cores cannot shrink. Fitting
  Amdahl's law to the 8-core point gives a serial fraction of about 15% (`ESTIMATE`, a curve
  fit, not a measurement).
- **8 cores means 8 Python worker processes**, plus the JVM, sharing one memory bus and 11 GB of RAM.
- **16 partitions on 8 cores** is only 2 waves of tasks, so a straggler task holds up the whole stage.

The DataFrame engine was not core-swept; that is listed under "not yet measured".

---

## §5 · DataFrame engine vs RDD engine

**Question this answers:** what did rewriting in DataFrames / Spark SQL buy (D11, D12)?

**Method:** as §3, 8 cores, 16 shuffle partitions (AQE on), same logs, median of 3, every run
exact against ground truth. **Commits:** DataFrame `b1806a4`, RDD `f176358`; `src/` is identical
between them (`git diff f176358 b1806a4 -- src` is empty). The `-dirty`
suffixes come from docs and harness edits only; `git diff b1806a4 90d63dc -- src` is empty.
Figure: `docs/figures/size_sweep_{light,dark}.png`.

| Log lines | RDD | DataFrame | DataFrame runs | Speedup | networkx |
|---:|---:|---:|---|---:|---:|
| 0.2M | 62.5 s | 27.5 s | 27.5 / 28.2 / 27.4 | 2.3× | **2.5 s** |
| 0.6M | 67.1 s | 30.1 s | 32.6 / 30.1 / 30.1 | 2.2× | **8.1 s** |
| 2M | 87.3 s | 43.8 s | 46.0 / 43.2 / 43.8 | 2.0× | **30.1 s** |
| 6M | 144.9 s | **71.5 s** | 69.5 / 71.5 / 72.9 | 2.0× | 107.5 s |
| 20M | 374.4 s | **170.8 s** | 170.8 / 169.1 / 170.8 | 2.2× | out of memory |

**Per stage at 20M lines (median):**

| Stage | RDD | DataFrame | Speedup |
|---|---:|---:|---:|
| Build RAG (read 936 MB, parse, deduplicate) | 91.9 s | 86.3 s | 1.1× |
| Prune (6 rounds) | 197.2 s | 47.4 s | **4.2×** |
| Cycle search (27 hops) | 73.4 s | 27.0 s | 2.7× |

**Reading:**
- **Pruning is where the rewrite pays.** Each RDD round pickles every edge out to a Python
  worker for each `map`, then back into the JVM for each shuffle. The DataFrame rounds are
  `left_semi` joins on JVM rows, and AQE broadcasts the shrinking survivor set rather than
  shuffling it. The search gains less, because its frontier is small (at most 108k paths).
- **The build stage barely moved.** Reading and splitting 936 MB of text, then a group-by over
  19M edges, costs about the same in either API. The SQL parser is not faster than Python's
  `split` by enough to matter against the I/O and the shuffle.
- **With the DataFrame engine, Spark overtakes the single process between 2M and 6M lines**
  (1.5× faster at 6M), not only by surviving at 20M. The RDD engine never overtook it on speed.

---

## Numbers not yet measured

| Not measured | Why not | Expectation |
|---|---|---|
| A lean single-process baseline (integer ids, `scipy.sparse.csgraph`) | Not scheduled (`DEFENCE.md` weak spot) | `ESTIMATE`: 5–10× less memory than networkx; would likely fit 20M lines and beat Spark there on one laptop |
| Core sweep for the DataFrame engine | Time | Better scaling than RDD (no Python workers), still limited by the serial driver loop |
| AQE off vs on, broadcast vs shuffle join | Not isolated | The §5 gain mixes JVM execution, Catalyst and AQE; it is not attributed to any one of them |
| Anything on a real cluster | No Docker in WSL (phase 4 cut) | More cost per shuffle than local mode |
| Spark's memory use | `/usr/bin/time` sees only the Python driver, not the JVM | Bounded per partition; spills to disk |
