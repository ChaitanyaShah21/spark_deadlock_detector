"""Build the Resource Allocation Graph and prune it down to the part that can deadlock.

All functions take and return Spark RDDs. Edge RDDs have the shape
((src, dst), timestamp), where timestamp is the earliest time the edge was seen.

Every shuffle is given an explicit partition count: without one, a PySpark join
groups over the union of its inputs and so has as many partitions as both
inputs combined, and an iterative loop multiplies the count every round.
"""

from .parsing import is_process, is_resource, parse_line, to_edge


def build_rag(lines, partitions):
    """Log lines -> unique RAG edges ((src, dst), earliest_timestamp)."""
    return (
        lines.map(parse_line)
        .filter(lambda event: event is not None)
        .map(to_edge)
        .reduceByKey(min, partitions)
    )


def wait_for_triples(rag, partitions):
    """Join waiters and holders on the shared resource.

    Returns ((waiter, holder), (resource, wait_ts, hold_ts)) for every
    waiter -> resource -> holder path with waiter != holder.
    """
    waits = rag.filter(lambda kv: is_process(kv[0][0])).map(
        lambda kv: (kv[0][1], (kv[0][0], kv[1]))  # (R, (P_waiter, ts))
    )
    holds = rag.filter(lambda kv: is_resource(kv[0][0])).map(
        lambda kv: (kv[0][0], (kv[0][1], kv[1]))  # (R, (P_holder, ts))
    )

    def to_triple(kv):
        resource, ((waiter, wait_ts), (holder, hold_ts)) = kv
        return (waiter, holder), (resource, wait_ts, hold_ts)

    return waits.join(holds, partitions).map(to_triple).filter(lambda kv: kv[0][0] != kv[0][1])


def prune_acyclic(edges, partitions):
    """Repeatedly drop wait-for edges touching a process with no in- or out-edge.

    A process with zero in-degree or zero out-degree cannot lie on a cycle, and
    removing it may expose new such processes, so iterate to a fixpoint. What
    remains is a superset of every process that is on a cycle. Each round is
    two distributed joins against the surviving process set.
    """
    edges = edges.distinct(partitions)
    edges.localCheckpoint()  # truncate lineage so many rounds don't build a huge DAG
    count = edges.count()
    rounds = 0
    while count:
        rounds += 1
        has_out = edges.keys().distinct(partitions).map(lambda p: (p, None))
        has_in = edges.values().distinct(partitions).map(lambda p: (p, None))
        alive = has_out.join(has_in, partitions).mapValues(lambda _: None)
        kept = (
            edges.join(alive, partitions)           # keep edges whose source is alive
            .map(lambda kv: (kv[1][0], kv[0]))
            .join(alive, partitions)                # ... and whose target is alive
            .map(lambda kv: (kv[1][0], kv[0]))
        )
        kept.localCheckpoint()
        new_count = kept.count()
        edges.unpersist()
        edges = kept
        if new_count == count:
            break
        count = new_count
    return edges, rounds


def candidate_subgraph(triples, live_wait_for, partitions):
    """RAG edges that lie on a surviving wait-for edge; cycles can only use these."""
    live = live_wait_for.map(lambda edge: (edge, None))

    def to_rag_edges(kv):
        (waiter, holder), ((resource, wait_ts, hold_ts), _) = kv
        return [((waiter, resource), wait_ts), ((resource, holder), hold_ts)]

    return triples.join(live, partitions).flatMap(to_rag_edges).reduceByKey(min, partitions)
