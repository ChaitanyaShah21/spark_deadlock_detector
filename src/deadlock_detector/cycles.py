"""Distributed cycle enumeration on the RAG by iterative path extension.

Every P -> R edge seeds a path. Each round joins the frontier (keyed by the
path's last node) with the adjacency lists and extends every path by one hop,
so round k discovers all cycles of k + 1 nodes.

To report each cycle exactly once, a path may only visit processes that sort
after its first process. A cycle is therefore found only from its smallest
process, already in canonical rotation, and the frontier is far smaller than
it would be if every rotation were explored.
"""

from typing import List, NamedTuple, Tuple

from .parsing import is_process, node_sort_key


class Cycle(NamedTuple):
    nodes: Tuple[str, ...]  # P, R, P, R, ... starting at the smallest process id
    formed_at: str          # time the last edge of the cycle first appeared


def find_cycles(rag, max_cycle_processes, partitions, max_paths, log=print):
    """Return (cycles sorted by length, then by node ids, truncated flag).

    `truncated` is True when the frontier exceeded `max_paths` and the search
    stopped early; cycles longer than the last completed round may be missing.
    """
    max_nodes = 2 * max_cycle_processes
    adj = (
        rag.map(lambda kv: (kv[0][0], (kv[0][1], kv[1])))
        .groupByKey(partitions)
        .mapValues(list)
        .cache()
    )
    # frontier: (last_node, (path, formed_at))
    paths = (
        rag.filter(lambda kv: is_process(kv[0][0]))
        .map(lambda kv: (kv[0][1], (kv[0], kv[1])))
        .partitionBy(partitions)
        .cache()
    )

    def extend(item):
        _, ((path, formed_at), neighbours) = item
        origin = path[0]
        for nbr, edge_ts in neighbours:
            ts = max(formed_at, edge_ts)
            if nbr == origin:
                if len(path) >= 4:
                    yield True, (path, ts)
            elif (
                len(path) < max_nodes
                and nbr not in path
                and not (is_process(nbr) and nbr < origin)
            ):
                yield False, (nbr, (path + (nbr,), ts))

    cycles: List[Cycle] = []
    truncated = False
    length = 2
    while True:
        step = paths.join(adj, partitions).flatMap(extend).cache()
        found = step.filter(lambda x: x[0]).values().collect()
        cycles.extend(Cycle(nodes, ts) for nodes, ts in found)

        next_paths = step.filter(lambda x: not x[0]).values().partitionBy(partitions).cache()
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
