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
| End-to-end runtime, 210k-line log, 9,999 deadlocks | **51.6 s** | 221.3 s | `355b124` | §1 |
| Speedup over the original implementation | **4.3×** at 210k lines, 1.9× at 5k | — | `355b124` | §1 |

---

## §1 · Rebuilt pipeline vs the original coursework implementation

**Question this answers:** did the rebuild (exact pruning, each cycle enumerated once, explicit
partition counts, cached search rounds) make it faster without changing the answer?

**Method:** `deadlock-detector LOG -c 4` (current) and `python AOS_Assignment4.py LOG 4`
(original), each a fresh process, run one after another on an otherwise idle machine.
**Workload:** `scripts/generate_log.py` logs — disjoint 10-process deadlock cycles,
identical bytes for both versions.
**Current:** commit `355b124`, median of 3 runs.
**Original:** `AOS_Assignment4.py` (sha256 prefix `c4e6eac13e6995fe`, not in this repository),
**single run per size** — re-measuring it three times is the first step of phase 1.
**Date:** 12 Oct 2026

| Log lines | Deadlocks | Original (1 run) | Current (median of 3) | Current runs | Speedup |
|---:|---:|---:|---:|---|---:|
| 5,247 | 249 | 69.6 s | 36.8 s | 35.6 / 36.8 / 39.1 | 1.9× |
| 52,497 | 2,499 | 95.3 s | 41.7 s | 41.7 / 43.5 / 41.0 | 2.3× |
| 209,997 | 9,999 | 221.3 s | 51.6 s | 51.6 / 54.7 / 51.2 | 4.3× |

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
