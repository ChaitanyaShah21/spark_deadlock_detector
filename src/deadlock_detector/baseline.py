"""Single-process baseline: the same deadlocks, found with networkx and no Spark.

It parses with the same parser and collapses duplicate edges to their earliest
timestamp. It keeps only the strongly connected components (SCCs) of the RAG
that contain a cycle, then enumerates cycles inside each one with
networkx.simple_cycles, bounded to the same length as the Spark search.

This is what the Spark pipeline has to beat: it answers "why not one process?".
"""

import argparse
import json
import resource
import sys
import time
from collections import Counter

import networkx as nx

from .parsing import is_process, node_sort_key, parse_line, to_edge


def canonical(cycle):
    """Rotate a cycle to start at its smallest process (string order, as the Spark search)."""
    start = min((k for k in range(len(cycle)) if is_process(cycle[k])), key=lambda k: cycle[k])
    return tuple(cycle[start:] + cycle[:start])


def detect(path, max_cycle_processes=13):
    timings = {}
    t = time.perf_counter()
    first_seen = {}
    with open(path) as f:
        for line in f:
            event = parse_line(line)
            if event is None:
                continue
            edge, ts = to_edge(event)
            old = first_seen.get(edge)
            if old is None or ts < old:
                first_seen[edge] = ts
    timings["parse"] = time.perf_counter() - t

    t = time.perf_counter()
    g = nx.DiGraph()
    g.add_edges_from(first_seen)
    processes = sum(1 for n in g if is_process(n))
    timings["build_graph"] = time.perf_counter() - t

    t = time.perf_counter()
    core = [c for c in nx.strongly_connected_components(g) if len(c) > 1]
    timings["scc"] = time.perf_counter() - t

    t = time.perf_counter()
    cycles = []
    for component in core:
        for cycle in nx.simple_cycles(g.subgraph(component), length_bound=2 * max_cycle_processes):
            nodes = canonical(cycle)
            formed = max(first_seen[e] for e in zip(nodes, nodes[1:] + nodes[:1]))
            cycles.append((nodes, formed))
    cycles.sort(key=lambda c: (len(c[0]), [node_sort_key(n) for n in c[0]]))
    timings["cycle_search"] = time.perf_counter() - t

    deadlocked = {n for nodes, _ in cycles for n in nodes if is_process(n)}
    return {
        "processes": processes,
        "resources": len(g) - processes,
        "edges": len(first_seen),
        "scc_processes": sum(1 for c in core for n in c if is_process(n)),
        "cycles": cycles,
        "deadlocked_processes": len(deadlocked),
        "deadlocks_per_day": sorted(Counter(f.split()[0] for _, f in cycles).items()),
        "timings": timings,
    }


def main(argv=None):
    p = argparse.ArgumentParser(prog="deadlock-detector-baseline",
                                description="Single-process networkx baseline for deadlock-detector.")
    p.add_argument("log")
    p.add_argument("--max-cycle-length", type=int, default=13, metavar="N")
    p.add_argument("--json", metavar="PATH", help="also write the full result as JSON")
    args = p.parse_args(argv)

    start = time.perf_counter()
    result = detect(args.log, args.max_cycle_length)
    total = time.perf_counter() - start
    peak_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024  # Linux reports KiB

    print(f"RAG: {result['processes']} processes, {result['resources']} resources, {result['edges']} edges")
    print(f"Deadlocks: {len(result['cycles'])} cycle(s), {result['deadlocked_processes']} deadlocked processes")
    stages = ", ".join(f"{k} {v:.2f}s" for k, v in result["timings"].items())
    print(f"Time: {total:.2f}s total ({stages}); peak memory {peak_mb:.0f} MB")
    if args.json:
        with open(args.json, "w") as f:
            json.dump({**result, "total_seconds": total, "peak_mb": peak_mb}, f)


if __name__ == "__main__":
    sys.exit(main())
