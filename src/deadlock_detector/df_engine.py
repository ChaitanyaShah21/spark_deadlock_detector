"""The same pipeline as detector.py, written with DataFrames instead of RDDs.

Every step is a Spark SQL expression, so rows stay in the JVM: there are no
Python lambdas, and the Catalyst optimiser plans each query. Adaptive query
execution (AQE) is on, so shuffle partitions are coalesced at run time, and a
join whose side turns out small is switched to a broadcast join.

Parsing is re-expressed in SQL rather than wrapping parse_line in a Python
user-defined function, which would ship every line to a Python worker and back.
tests/test_df_engine.py checks that both parsers accept and reject the same lines.
"""

import time
from collections import Counter

from pyspark.sql import functions as F

from .cycles import Cycle
from .detector import Report
from .parsing import node_sort_key


def is_proc(col):
    return F.col(col).startswith("P")


def parse(lines):
    """Log lines (column `value`) -> events (ts, level, process, resource); bad lines dropped.

    Mirrors parsing.parse_line: whitespace-separated tokens, trailing commas
    stripped, a two-token or one-token timestamp, WAIT/HOLD in any case, and the
    process and resource in either order.
    """
    tokens = F.split(F.regexp_replace("value", r"^\s+|\s+$", ""), r"\s+")
    tokens = F.transform(tokens, lambda t: F.regexp_replace(t, ",+$", ""))
    df = lines.select(tokens.alias("t")).select("t", F.size("t").alias("n"))
    five = F.col("n") >= 5
    df = df.select(
        F.when(five, F.concat_ws(" ", F.col("t")[0], F.col("t")[1])).otherwise(F.col("t")[0]).alias("ts"),
        F.upper(F.when(five, F.col("t")[2]).otherwise(F.col("t")[1])).alias("level"),
        F.when(five, F.col("t")[3]).otherwise(F.col("t")[2]).alias("a"),
        F.when(five, F.col("t")[4]).otherwise(F.col("t")[3]).alias("b"),
        "n",
    ).where((F.col("n") >= 4) & F.col("level").isin("WAIT", "HOLD"))
    a_p = F.col("a").startswith("P") & F.col("b").startswith("R")
    a_r = F.col("a").startswith("R") & F.col("b").startswith("P")
    return df.where(a_p | a_r).select(
        "ts",
        "level",
        F.when(a_p, F.col("a")).otherwise(F.col("b")).alias("process"),
        F.when(a_p, F.col("b")).otherwise(F.col("a")).alias("resource"),
    )


def build_rag(lines):
    """Unique RAG edges (src, dst, ts), ts the earliest time the edge was seen."""
    events = parse(lines)
    wait = F.col("level") == "WAIT"
    edges = events.select(
        F.when(wait, F.col("process")).otherwise(F.col("resource")).alias("src"),
        F.when(wait, F.col("resource")).otherwise(F.col("process")).alias("dst"),
        "ts",
    )
    return edges.groupBy("src", "dst").agg(F.min("ts").alias("ts"))


def wait_for_triples(rag):
    """(waiter, holder, resource, wait_ts, hold_ts) for every waiter -> R -> holder, waiter != holder."""
    waits = rag.where(is_proc("src")).select(
        F.col("src").alias("waiter"), F.col("dst").alias("resource"), F.col("ts").alias("wait_ts"))
    holds = rag.where(~is_proc("src")).select(
        F.col("src").alias("resource"), F.col("dst").alias("holder"), F.col("ts").alias("hold_ts"))
    return waits.join(holds, "resource").where(F.col("waiter") != F.col("holder"))


def prune_acyclic(edges):
    """Drop wait-for edges touching a process with no in- or out-edge, to a fixpoint.

    Same rule as graph.prune_acyclic. Each round is a semi-join on each end;
    localCheckpoint() truncates the plan so it does not grow with the rounds.
    """
    edges = edges.distinct().localCheckpoint()
    count = edges.count()
    rounds = 0
    while count:
        rounds += 1
        has_out = edges.select(F.col("waiter").alias("p")).distinct()
        has_in = edges.select(F.col("holder").alias("p")).distinct()
        alive = has_out.join(has_in, "p", "left_semi")
        kept = (
            edges.join(alive.withColumnRenamed("p", "waiter"), "waiter", "left_semi")
            .join(alive.withColumnRenamed("p", "holder"), "holder", "left_semi")
            .localCheckpoint()
        )
        new_count = kept.count()
        edges.unpersist()
        edges = kept
        if new_count == count:
            break
        count = new_count
    return edges, rounds


def candidate_subgraph(triples, live):
    """RAG edges (src, dst, ts) lying on a surviving wait-for edge."""
    on_live = triples.join(live, ["waiter", "holder"], "left_semi")
    return (
        on_live.select(F.col("waiter").alias("src"), F.col("resource").alias("dst"), F.col("wait_ts").alias("ts"))
        .union(on_live.select(F.col("resource").alias("src"), F.col("holder").alias("dst"), F.col("hold_ts").alias("ts")))
        .groupBy("src", "dst").agg(F.min("ts").alias("ts"))
    )


def find_cycles(rag, max_cycle_processes, max_paths, log):
    """Same search as cycles.find_cycles: one join per hop, each cycle found once from its smallest process."""
    max_nodes = 2 * max_cycle_processes
    adj = rag.select(F.col("src").alias("last"), F.col("dst").alias("nbr"), F.col("ts").alias("edge_ts")).cache()
    paths = rag.where(is_proc("src")).select(
        F.array("src", "dst").alias("path"), F.col("dst").alias("last"), "ts").localCheckpoint()

    cycles, truncated, length = [], False, 2
    while True:
        step = paths.join(adj, "last").select(
            "path", "nbr", F.greatest("ts", "edge_ts").alias("ts"), F.col("path")[0].alias("origin"))
        closes = (F.col("nbr") == F.col("origin")) & (F.size("path") >= 4)
        extends = (
            (F.col("nbr") != F.col("origin"))
            & (F.size("path") < max_nodes)
            & ~F.array_contains("path", F.col("nbr"))
            & ~(F.col("nbr").startswith("P") & (F.col("nbr") < F.col("origin")))
        )
        step = step.where(closes | extends).withColumn("closed", closes).localCheckpoint()
        found = step.where("closed").select("path", "ts").collect()
        cycles.extend(Cycle(tuple(r.path), r.ts) for r in found)

        next_paths = step.where(~F.col("closed")).select(
            F.concat("path", F.array("nbr")).alias("path"), F.col("nbr").alias("last"), "ts").localCheckpoint()
        frontier = next_paths.count()
        length += 1
        log(f"  path length {length}: {len(found)} cycles closed, {frontier} open paths")

        step.unpersist()
        paths.unpersist()
        paths = next_paths
        if frontier == 0:
            break
        if frontier > max_paths:
            truncated = True
            log(f"  frontier exceeded max_paths={max_paths}; stopping search early")
            break

    paths.unpersist()
    adj.unpersist()
    cycles.sort(key=lambda c: (len(c.nodes), [node_sort_key(n) for n in c.nodes]))
    return cycles, truncated


def top_k(df, col, k):
    rows = df.groupBy(col).count().orderBy(F.desc("count"), F.asc(col)).limit(k).collect()
    return [(r[col], r["count"]) for r in rows]


def detect_deadlocks_df(spark, log_path, partitions=8, max_cycle_processes=13, max_paths=200_000,
                        top=5, log=lambda *_: None):
    spark.conf.set("spark.sql.shuffle.partitions", partitions)
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    timings = {}
    t = time.perf_counter()

    rag = build_rag(spark.read.text(log_path)).cache()
    nodes = rag.select(F.col("src").alias("n")).union(rag.select(F.col("dst").alias("n"))).distinct()
    kinds = {r["is_p"]: r["count"] for r in nodes.groupBy(is_proc("n").alias("is_p")).count().collect()}
    num_edges = rag.count()
    waits = rag.where(is_proc("src"))
    top_resources = top_k(waits.select(F.col("dst").alias("node")), "node", top)
    top_processes = top_k(waits.select(F.col("src").alias("node")), "node", top)
    timings["build_rag"] = time.perf_counter() - t
    log(f"RAG: {kinds.get(True, 0)} processes, {kinds.get(False, 0)} resources, {num_edges} edges")

    t = time.perf_counter()
    triples = wait_for_triples(rag).cache()
    wait_for = triples.select("waiter", "holder")
    num_wait_for = wait_for.distinct().count()
    live, rounds = prune_acyclic(wait_for)
    candidates = live.select("waiter").distinct().count()
    timings["prune"] = time.perf_counter() - t
    log(f"wait-for graph: {num_wait_for} edges; {candidates} processes survive {rounds} pruning rounds")

    t = time.perf_counter()
    if candidates:
        subgraph = candidate_subgraph(triples, live).cache()
        cycles, truncated = find_cycles(subgraph, max_cycle_processes, max_paths, log)
    else:
        cycles, truncated = [], False
    timings["cycle_search"] = time.perf_counter() - t

    deadlocked = sorted({n for c in cycles for n in c.nodes if n.startswith("P")}, key=node_sort_key)
    per_day = Counter(c.formed_at.split()[0] for c in cycles)
    return Report(
        processes=kinds.get(True, 0),
        resources=kinds.get(False, 0),
        edges=num_edges,
        wait_for_edges=num_wait_for,
        candidate_processes=candidates,
        prune_rounds=rounds,
        cycles=cycles,
        truncated=truncated,
        deadlocked_processes=deadlocked,
        deadlocks_per_day=sorted(per_day.items()),
        top_resources=top_resources,
        top_processes=top_processes,
        timings=timings,
    )
