"""Parse resource-allocation log lines into Resource Allocation Graph (RAG) edges.

Accepted line shapes (tab- or whitespace-separated, trailing commas tolerated):

    2025-10-31 12:15:59  WAIT  P104  R008
    2025-10-31 12:15:59  HOLD  P101  R006
    2025-10-25 00:29:15  HOLD  R024  P105     <- resource-first order also accepted

WAIT means the process is requesting the resource  -> edge P -> R
HOLD means the process has been granted the resource -> edge R -> P
"""

from typing import NamedTuple, Optional

LEVELS = ("WAIT", "HOLD")


class Event(NamedTuple):
    timestamp: str  # "YYYY-MM-DD HH:MM:SS" - sorts correctly as a string
    level: str
    process: str
    resource: str


def is_process(node: str) -> bool:
    return node.startswith("P")


def is_resource(node: str) -> bool:
    return node.startswith("R")


def node_sort_key(node: str):
    """Order P2 before P10 (numeric suffix), falling back to the raw string."""
    digits = node[1:]
    return (node[0], int(digits) if digits.isdigit() else -1, node)


def parse_line(line: str) -> Optional[Event]:
    """Return an Event, or None for blank, malformed or non WAIT/HOLD lines."""
    parts = [p.strip().rstrip(",") for p in line.split()]
    if len(parts) >= 5:
        timestamp, rest = f"{parts[0]} {parts[1]}", parts[2:5]
    elif len(parts) == 4:
        timestamp, rest = parts[0], parts[1:4]
    else:
        return None

    level, a, b = rest[0].upper(), rest[1], rest[2]
    if level not in LEVELS:
        return None

    # The process and resource may appear in either order.
    if is_process(a) and is_resource(b):
        process, resource = a, b
    elif is_resource(a) and is_process(b):
        process, resource = b, a
    else:
        return None
    return Event(timestamp, level, process, resource)


def to_edge(event: Event):
    """Map an Event to a directed RAG edge ((src, dst), timestamp)."""
    if event.level == "WAIT":
        return (event.process, event.resource), event.timestamp
    return (event.resource, event.process), event.timestamp
