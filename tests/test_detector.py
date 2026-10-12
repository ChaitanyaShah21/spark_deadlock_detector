import random
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
from generate_log import generate  # noqa: E402


def log(*events):
    """log(("WAIT", "P1", "R1"), ...) -> log text with increasing timestamps."""
    return "".join(
        f"2025-01-01 00:00:{i:02d}\t{level}\t{p}\t{r}\n" for i, (level, p, r) in enumerate(events)
    )


def cycles(report):
    return [c.nodes for c in report.cycles]


def test_sample_log(detect):
    report = detect(ROOT / "data" / "sample.log")
    assert (report.processes, report.resources, report.edges) == (9, 9, 18)
    assert cycles(report) == [
        ("P101", "R002", "P106", "R006"),
        ("P105", "R003", "P107", "R007"),
        ("P102", "R005", "P103", "R009", "P108", "R004"),
    ]
    assert report.deadlocked_processes == ["P101", "P102", "P103", "P105", "P106", "P107", "P108"]
    assert report.deadlocks_per_day == [("2025-10-31", 1), ("2025-11-02", 2)]
    assert report.top_resources[:2] == [("R001", 2), ("R008", 2)]


def test_no_deadlock(detect):
    report = detect(log(("WAIT", "P1", "R1"), ("HOLD", "P2", "R1"), ("WAIT", "P2", "R2")))
    assert report.cycles == [] and report.wait_for_edges == 1


def test_resource_first_hold_lines_count(detect):
    text = (
        "2025-01-01 00:00:01 WAIT P1 R1\n"
        "2025-01-01 00:00:02 HOLD R1 P2\n"
        "2025-01-01 00:00:03 WAIT P2 R2\n"
        "2025-01-01 00:00:04 HOLD R2 P1\n"
    )
    report = detect(text)
    assert cycles(report) == [("P1", "R1", "P2", "R2")]
    assert report.cycles[0].formed_at == "2025-01-01 00:00:04"


def test_chain_feeding_a_cycle_is_pruned(detect):
    # P3 -> P4 -> {P1 <-> P2}: P3 and P4 wait but are not deadlocked themselves.
    report = detect(log(
        ("WAIT", "P1", "R1"), ("HOLD", "P2", "R1"),
        ("WAIT", "P2", "R2"), ("HOLD", "P1", "R2"),
        ("WAIT", "P4", "R3"), ("HOLD", "P1", "R3"),
        ("WAIT", "P3", "R4"), ("HOLD", "P4", "R4"),
    ))
    assert cycles(report) == [("P1", "R1", "P2", "R2")]
    assert report.candidate_processes == 2


def test_two_cycles_sharing_a_process(detect):
    report = detect(log(
        ("WAIT", "P1", "R1"), ("HOLD", "P2", "R1"),
        ("WAIT", "P2", "R2"), ("HOLD", "P1", "R2"),
        ("WAIT", "P1", "R3"), ("HOLD", "P3", "R3"),
        ("WAIT", "P3", "R4"), ("HOLD", "P1", "R4"),
    ))
    assert cycles(report) == [("P1", "R1", "P2", "R2"), ("P1", "R3", "P3", "R4")]


def test_max_cycle_length(detect):
    three = log(
        ("WAIT", "P1", "R1"), ("HOLD", "P2", "R1"),
        ("WAIT", "P2", "R2"), ("HOLD", "P3", "R2"),
        ("WAIT", "P3", "R3"), ("HOLD", "P1", "R3"),
    )
    assert len(detect(three, max_cycle_processes=3).cycles) == 1
    assert detect(three, max_cycle_processes=2).cycles == []


def test_generated_log(detect, tmp_path):
    path = tmp_path / "gen.log"
    with open(path, "w") as f:
        generate(steps=55, cycle_size=10, out=f)
    report = detect(path)
    assert len(report.cycles) == 5
    assert all(len(c.nodes) == 20 for c in report.cycles)
    assert len(report.deadlocked_processes) == 50


def reference_cycles(text):
    """Brute-force DFS over the RAG, independent of the Spark implementation."""
    first_seen = {}
    for line in text.splitlines():
        date, time, level, p, r = line.split()
        edge = (p, r) if level == "WAIT" else (r, p)
        first_seen[edge] = min(first_seen.get(edge, "~"), f"{date} {time}")
    adj = defaultdict(list)
    for u, v in first_seen:
        adj[u].append(v)

    found = {}

    def dfs(path):
        for nxt in adj[path[-1]]:
            if nxt == path[0] and len(path) >= 4:
                i = min(range(0, len(path), 2), key=lambda k: path[k])  # rotate to smallest P
                nodes = tuple(path[i:] + path[:i])
                edges = zip(path, path[1:] + path[:1])
                found[nodes] = max(first_seen[e] for e in edges)
            elif nxt not in path:
                dfs(path + [nxt])

    for node in list(adj):
        if node.startswith("P"):
            dfs([node])
    return sorted(found.items())


def test_matches_brute_force_on_random_graphs(detect):
    total = 0
    for seed in range(6):
        rng = random.Random(seed)
        procs = [f"P{i}" for i in range(8)]
        res = [f"R{i}" for i in range(8)]
        events = [("HOLD", rng.choice(procs), r) for r in res]  # one holder per resource
        events += [("WAIT", rng.choice(procs), rng.choice(res)) for _ in range(20)]
        rng.shuffle(events)
        text = log(*events)

        expected = reference_cycles(text)
        report = detect(text)
        assert sorted((c.nodes, c.formed_at) for c in report.cycles) == expected, seed
        total += len(expected)
    assert total >= 10  # the seeds above produce cycles of 2-4 processes


def test_realistic_log_matches_ground_truth(detect, tmp_path):
    from generate_realistic_log import generate as generate_realistic

    path = tmp_path / "realistic.log"
    with open(path, "w") as f:
        truth = generate_realistic(lines=4000, out=f, days=3, seed=7, cluster_rate=0.05)
    assert set(truth["cluster_kinds"]) == {"single", "hub", "theta"}  # every shape is exercised

    report = detect(path)
    assert (report.processes, report.resources, report.edges) == (
        truth["processes"], truth["resources"], truth["edges"])
    assert sorted((list(c.nodes), c.formed_at) for c in report.cycles) == sorted(map(tuple, truth["cycle_list"]))
    assert report.candidate_processes == truth["candidate_processes"]  # feeders and tails pruned, bridges kept
    assert len(report.deadlocked_processes) == truth["deadlocked_processes"]
    assert [tuple(d) for d in report.deadlocks_per_day] == [tuple(d) for d in truth["deadlocks_per_day"]]
