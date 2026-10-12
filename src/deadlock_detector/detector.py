"""End-to-end pipeline: log file -> RAG -> pruned wait-for graph -> cycles -> report."""

import os
import sys
import time
from collections import Counter
from dataclasses import dataclass, field
from typing import List, Tuple

from pyspark.sql import SparkSession

from .cycles import Cycle, find_cycles
from .graph import build_rag, candidate_subgraph, prune_acyclic, wait_for_triples
from .parsing import is_process, node_sort_key


@dataclass
class Report:
    processes: int
    resources: int
    edges: int
    wait_for_edges: int
    candidate_processes: int
    prune_rounds: int
    cycles: List[Cycle]
    truncated: bool
    deadlocked_processes: List[str]
    deadlocks_per_day: List[Tuple[str, int]]
    top_resources: List[Tuple[str, int]]   # most distinct processes waiting on it
    top_processes: List[Tuple[str, int]]   # waiting on the most distinct resources
    timings: dict = field(default_factory=dict)


def create_spark(cores, driver_memory="4g"):
    # Python workers must run the same interpreter (and see the same packages)
    # as the driver, otherwise they fall back to whatever `python3` is on PATH.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    spark = (
        SparkSession.builder.appName("deadlock-detector")
        .master(f"local[{cores}]")
        .config("spark.driver.memory", driver_memory)
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def top_k(rdd, k, partitions):
    """Top k (key, count) pairs by count, ties broken by key."""
    return rdd.reduceByKey(lambda a, b: a + b, partitions).takeOrdered(k, key=lambda kv: (-kv[1], kv[0]))


def detect_deadlocks(
    sc,
    log_path,
    partitions=8,
    max_cycle_processes=13,
    max_paths=200_000,
    top=5,
    log=lambda *_: None,
):
    timings = {}
    t = time.perf_counter()

    rag = build_rag(sc.textFile(log_path, minPartitions=partitions), partitions).cache()
    node_kinds = (
        rag.keys().flatMap(lambda e: e).distinct(partitions).map(is_process).countByValue()
    )
    num_edges = rag.count()
    wait_edges = rag.keys().filter(lambda e: is_process(e[0]))
    top_resources = top_k(wait_edges.map(lambda e: (e[1], 1)), top, partitions)
    top_processes = top_k(wait_edges.map(lambda e: (e[0], 1)), top, partitions)
    timings["build_rag"] = time.perf_counter() - t
    log(f"RAG: {node_kinds[True]} processes, {node_kinds[False]} resources, {num_edges} edges")

    t = time.perf_counter()
    triples = wait_for_triples(rag, partitions).cache()
    wait_for = triples.keys()
    num_wait_for = wait_for.distinct(partitions).count()
    live, rounds = prune_acyclic(wait_for, partitions)
    candidates = live.keys().distinct(partitions).count()
    timings["prune"] = time.perf_counter() - t
    log(f"wait-for graph: {num_wait_for} edges; {candidates} processes survive {rounds} pruning rounds")

    t = time.perf_counter()
    if candidates:
        subgraph = candidate_subgraph(triples, live, partitions).cache()
        cycles, truncated = find_cycles(subgraph, max_cycle_processes, partitions, max_paths, log)
    else:
        cycles, truncated = [], False
    timings["cycle_search"] = time.perf_counter() - t

    deadlocked = sorted({n for c in cycles for n in c.nodes if is_process(n)}, key=node_sort_key)
    per_day = Counter(c.formed_at.split()[0] for c in cycles)

    return Report(
        processes=node_kinds[True],
        resources=node_kinds[False],
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
