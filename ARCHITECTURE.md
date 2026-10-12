# Architecture — spark-deadlock-detector

The current design and the log of the decisions that produced it. The README's design notes
are written from this file.

---

## What this system does

It reads a log of resource-allocation events (`WAIT P R`: process P requests resource R;
`HOLD P R`: P has been granted R) and reports every deadlock in it. With single-instance
resources, a deadlock is exactly a cycle `P → R → P → R → … → P` in the Resource Allocation
Graph (RAG). For each cycle it reports its members and when it formed. It also reports the
deadlocked processes, a per-day timeline of deadlocks, and contention hotspots. All
graph work runs as Spark jobs, so the same code runs on a laptop (`local[N]`) or a cluster.

**Scope boundary — what it deliberately does not do:**
- **No release events.** An edge exists from its first appearance onward, so this is
  post-mortem analysis of a log, not live detection or deadlock recovery.
- **No multi-instance resources.** With multi-instance resources a cycle is necessary but not
  sufficient for deadlock, and you would need the banker's-style reduction instead.
- **Cycles longer than `--max-cycle-length` processes (default 13) are not searched for.**
- **Not streaming.** It processes one log per run.

---

## The diagram

```mermaid
flowchart TD
    A[log file] -->|1. textFile + parse_line| B["RAG edges<br/>((src, dst), first_seen)<br/>reduceByKey(min)"]
    B -->|2. join on resource| C["wait-for edges<br/>Pw → Ph, keeping R and both timestamps"]
    C -->|3. prune to fixpoint<br/>2 joins per round| D["live wait-for edges<br/>every process has in- and out-degree ≥ 1"]
    C --> E
    D -->|4. join back| E["candidate sub-RAG<br/>only P→R and R→P edges on live wait-for edges"]
    E -->|5. iterative path extension<br/>1 join per hop| F["cycles<br/>(canonical nodes, formed_at)"]
    B -->|side: counts + top-k| G[hotspots]
    F --> H[report: cycles, deadlocked processes, timeline]
    G --> H
```

**Reading it:**
1. Each line is parsed into one directed RAG edge. Duplicate edges collapse to their earliest
   timestamp.
2. A join on the resource turns `Pw → R` plus `R → Ph` into a wait-for edge `Pw → Ph`.
   Self-waits (`Pw == Ph`) are dropped.
3. A process with no outgoing or no incoming wait-for edge cannot be on a cycle. Such processes
   are removed repeatedly until nothing changes. What survives is every process on a cycle,
   plus any process sitting on a path between cycles.
4. Only the RAG edges that make up surviving wait-for edges are kept.
5. Every `P → R` edge seeds an open path. Each round joins the frontier (keyed by its last node)
   with the adjacency lists, extends each path one hop, and closes cycles that return to their
   start.

---

## Components

| Component | Responsibility | Spark operations |
|---|---|---|
| `parsing.py` | Line → `Event` → RAG edge; tolerant of tabs, spaces, trailing commas and either-order `HOLD` | none — pure Python, runs inside `map` |
| `graph.py` | RAG construction, wait-for join, fixpoint pruning, candidate subgraph | `reduceByKey`, `join`, `distinct`, `localCheckpoint` |
| `cycles.py` | Distributed cycle enumeration | `groupByKey` (adjacency), `join` + `flatMap` per hop, `collect` of closed cycles |
| `detector.py` | Pipeline order, caching, hotspots, timeline, timings | `countByValue`, `takeOrdered`, `cache` |
| `cli.py` | Arguments, report printing, JSON output | — |
| `scripts/generate_log.py` | Synthetic logs with a known number of deadlocks | — |

---

## Open forks — the upgrade phases

Each fork has a recommended option. Until a fork is decided, the recommendation is what gets built.

| # | Fork | Options (recommended first) | Why it matters |
|---|---|---|---|
| F1 | Single-machine baseline to compare Spark against | **(a) `networkx` SCC + `simple_cycles` in one Python process** · (b) a hand-written DFS · (c) the same code in `local[1]` | "Why Spark?" has no answer without it. (a) is an independent, well-known implementation; (c) measures Spark overhead, not the alternative. |
| F2 | Benchmark workload | **(a) a realistic generator: most processes not deadlocked, chains feeding into cycles, cycles of varied length that share processes, timestamps spread over days, malformed lines** · (b) scale up the current generator | The current generator puts almost every process in a cycle, so pruning removes nothing and the numbers describe an unrealistic best case. |
| F3 | DataFrame rewrite scope | **(a) DataFrame engine end to end, selectable with `--engine df`, with the RDD engine kept for comparison** · (b) a hybrid: DataFrames for parse/RAG/prune, RDDs for the path search · (c) GraphFrames | (a) gives a clean before/after and the Catalyst / AQE / broadcast-join story. (c) adds a dependency, and its motif finding has a fixed cycle length. |
| F4 | Frontier join strategy (with F3) | **(a) broadcast hint when the frontier is small, AQE (adaptive query execution) on** · (b) always a shuffle join | The frontier was only about 250 paths at 5k lines; shuffling it every hop is pure overhead. |
| F5 | Cluster run | **(a) Docker Compose: 1 master + 2 workers, `spark-submit --py-files`** · (b) skip — local mode only | Shows the code is not tied to `local[N]`. All containers share one host, and that must be stated. |
| F6 | CI | **(a) GitHub Actions, Ubuntu, `setup-java` 17, Python 3.12, `pytest`** · (b) a 3.9–3.12 × Java 11/17 matrix | (b) costs CI minutes for little gain. |

---

## Decision log

Decisions D1–D8 were taken while rebuilding the coursework version (12 Oct 2026, commit
`2d30dae`). Each was **adopted by default and is open to override**.

### D1 — RDD API rather than DataFrames (for now)
**The fork:** which Spark API to express the graph algorithm in.

| Option | What it means in practice | Cost |
|---|---|---|
| RDDs (chosen) | Plain Python functions over key-value pairs, joins by hand | No Catalyst optimiser; every lambda round-trips through Python workers |
| DataFrames / Spark SQL | Columnar, optimised plans, broadcast joins, AQE | Path lists become array columns; the cycle loop has to be written as repeated joins with array functions |

**Chosen:** RDDs. The coursework version was written with RDDs, and keeping them let the cleanup be
measured like-for-like against the original.
**What would change my mind:** this is fork F3. Phase 3 rewrites it and measures both.

### D2 — exact degree pruning instead of connected components
| Option | What it means | Cost |
|---|---|---|
| Fixpoint degree pruning (chosen) | Drop processes with in- or out-degree 0 until stable | Rounds grow with the longest chain feeding into a cycle |
| Undirected connected components, 10 rounds (original) | Keep components with more than one node | **Unsound:** with a 10-round cap a cycle member can be excluded; also keeps acyclic trees |
| Strongly connected components | Exact: cycle members are exactly the nodes in non-trivial SCCs | Distributed SCC (forward/backward reachability) is much more code |

**Chosen:** degree pruning. It is exact (it never drops a cycle member), simple to explain, and removes
every chain that feeds into a cycle.
**What would change my mind:** inputs with very long waiting chains, where the number of rounds dominates.
At that point SCC wins.

### D3 — enumerate each cycle once, from its smallest process
| Option | What it means | Cost |
|---|---|---|
| Canonical origin (chosen) | A path may only visit processes that sort after its first one | Paths must carry their origin; ordering is by string |
| Explore every rotation, deduplicate at the end (original) | Each k-process cycle found k times | Frontier k times larger, and deduplication runs on the driver |

### D4 — explicit partition counts on every shuffle
PySpark's `join` defaults to the partition count of the union of its inputs, so iterative
loops compounded it (320 → 35,040 tasks in four rounds). One fixed count, `2 × cores` by
default, is passed everywhere. **Rejected:** per-stage tuning (more knobs, little gain at this
scale). AQE in phase 3 makes this automatic.

### D5 — a bounded search that reports when it is incomplete
`--max-cycle-length` bounds the depth, and `--max-paths` bounds the frontier. If the frontier
limit is hit, the search stops and the report says so. **Rejected:** an unbounded search
(cycle enumeration is exponential in the worst case) and the original's silent filter.

### D6 — when a deadlock "formed"
A cycle forms when its *last* edge first appears: the max over its edges of each edge's
earliest timestamp. Without release events this is the earliest moment the whole cycle
existed. **Rejected:** the most recent timestamp of each edge, which would date a deadlock by
repeated retries rather than by its formation.

### D7 — hotspot ties broken by name
The original sorted only by count, so ties came out in partition order, which can change
between runs and core counts. `takeOrdered` with a key of `(-count, name)` makes the output
deterministic.

### D8 — generator ported from C++ to Python
One language in the repo and no compiled binary to commit. The port was proved byte-identical
to the C++ output on all six original logs.

---

## Invariants

| # | Invariant | Enforced by | Tested by |
|---|---|---|---|
| I1 | Pruning never removes a process that is on a cycle | Only degree-0 processes are removed | `test_matches_brute_force_on_random_graphs`, `test_chain_feeding_a_cycle_is_pruned` |
| I2 | Every cycle is reported exactly once, starting at its smallest process | Canonical-origin rule in `extend` | brute-force comparison (exact list equality) |
| I3 | A reported cycle alternates P, R, P, R and has no repeated node | The RAG is bipartite by construction; `nbr not in path` | brute-force comparison |
| I4 | `formed_at` = max over the cycle's edges of each edge's first appearance | `max` carried along each path | `test_resource_first_hold_lines_count`, brute-force comparison |
| I5 | Partition counts stay fixed across iterations | Explicit `numPartitions` everywhere | **not tested yet** — a `getNumPartitions()` assertion is the cheap fix |

---

## Execution model

- **Driver** (one Python process plus the JVM via py4j) runs the loop, collects closed cycles
  each round and builds the report.
- **Executors** run the tasks. In local mode they are threads in the same JVM, and each task's
  Python code runs in a pooled Python worker process. `PYSPARK_PYTHON` is pinned to the
  driver's interpreter so workers can import the package.
- **Per pruning round:** two `distinct`s, three joins, and one `count` to detect the fixpoint.
  **Per search hop:** one join, one `collect` and one `count`. Each `count` is a job boundary, so the
  cost per round has a fixed floor, and that floor dominates at small sizes.
- **Lineage:** pruning uses `localCheckpoint()` each round, so the DAG (directed acyclic graph
  of transformations) does not grow with the number of rounds. The search instead caches each frontier and
  unpersists the previous one.

## Failure model

| Failure | What happens |
|---|---|
| A task fails | Spark retries it and recomputes the lost partition from lineage |
| An executor is lost after `localCheckpoint` | Locally checkpointed blocks are lost with it and **cannot** be recomputed — the job fails. Accepted for single-machine use; a cluster run would use `checkpoint()` to reliable storage instead |
| A malformed log line | Dropped silently (counted nowhere yet) |
| The frontier explodes | The search stops at `--max-paths` and the report is flagged incomplete |
