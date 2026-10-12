#!/usr/bin/env python3
"""Generate a realistic resource-allocation log, with its ground truth alongside.

generate_log.py puts every process in a cycle, so pruning has nothing to do.
Here most processes are not deadlocked:

* background: blocking trees - processes waiting, directly or through others,
  on a process that is running. No cycles.
* deadlock clusters: a single cycle, several cycles sharing one hub process,
  or two cycles sharing a path (a theta). Cycle lengths vary.
* feeder chains waiting on a cluster (blocked, not deadlocked) and tails a
  cluster waits on (running processes). Pruning has to remove both.
* bridges: chains from an earlier cluster to a later one. They survive pruning
  without being on any cycle, so the search has to rule them out itself.
* timestamps spread over --days days, retried WAITs, resource-first HOLDs,
  tab or space separators, and a fraction of malformed lines.

Every wait-for edge a -> b is written as `WAIT a R` where R is the resource b
holds, and each resource has exactly one holder (the format has no release
event). The cycles of the log are therefore exactly the planted ones. They are
written to the truth file in the detector's canonical form.

    python scripts/generate_realistic_log.py --lines 2000000 -o data/generated/real_2m.log
"""

import argparse
import json
import random
import sys
from collections import Counter
from datetime import date, timedelta

START = date(2025, 10, 1)
DAY = 86_400
WINDOW = 7_200  # seconds over which one structure's events are spread
PRIMES = (10_007, 100_003, 1_000_003, 10_000_019, 100_000_007, 1_000_000_007)
RECENT_CLUSTERS = 100  # bridges start from one of the last N clusters


class Generator:
    def __init__(self, lines, days, seed, cluster_rate, malformed_rate, retry_rate, max_cycle):
        self.rng = random.Random(seed)
        self.target, self.days = lines, days
        self.cluster_rate, self.malformed_rate = cluster_rate, malformed_rate
        self.retry_rate, self.max_cycle = retry_rate, max_cycle
        # ids are scrambled by a bijection n -> n * mult mod a prime, so that a
        # cycle's smallest id is not simply the first process created in it
        self.modulus = next(p for p in PRIMES if p > 2 * lines + 100)
        self.mult = self.rng.randrange(2, self.modulus - 1)
        self.n_proc = self.n_res = self.edges = 0
        self.lines = self.malformed = 0
        self.bridge_procs = 0
        self.kinds = Counter()
        self.cycles = []  # (nodes, formed_at)
        self.recent = []  # member lists of recent clusters
        self.buf = []     # (second of day, line) for the current day

    # --- ids and lines -----------------------------------------------------

    def scramble(self, n):
        return n * self.mult % self.modulus

    def new_resource(self):
        self.n_res += 1
        return f"R{self.scramble(self.n_res)}"

    def stamp(self, t):
        return f"{self.day} {t // 3600:02d}:{t // 60 % 60:02d}:{t % 60:02d}"

    def emit(self, t, level, p, r):
        a, b = (r, p) if level == "HOLD" and self.rng.random() < 0.1 else (p, r)
        s = self.sep
        self.buf.append((t, f"{self.stamp(t)}{s}{level}{s}{a}{s}{b}"))

    def new_process(self):
        """A process holding its own resource, granted early in the window."""
        self.n_proc += 1
        p = f"P{self.scramble(self.n_proc)}"
        r = self.new_resource()
        t = self.base + self.rng.randrange(WINDOW // 2)
        self.emit(t, "HOLD", p, r)
        self.edges += 1
        self.held[p] = (r, t)
        return p

    def wait(self, a, b):
        """Make a wait for b's resource, after b was granted it. Returns the time."""
        r, granted = self.held[b]
        t = granted + 1 + self.rng.randrange(WINDOW // 2)
        self.emit(t, "WAIT", a, r)
        self.edges += 1
        while self.rng.random() < self.retry_rate:
            self.emit(t + 1 + self.rng.randrange(WINDOW // 2), "WAIT", a, r)
        return t

    # --- structures ----------------------------------------------------------

    def start_structure(self):
        self.base = self.rng.randrange(DAY - 2 * WINDOW)
        self.sep = "\t" if self.rng.random() < 0.3 else "  "
        self.held = {}

    def background_tree(self):
        """Waiters on a running root, each waiting only on earlier members: no cycle."""
        members = [self.new_process()]
        while self.rng.random() < 0.6 and len(members) < 12:
            p = self.new_process()
            first = self.rng.choice(members)
            self.wait(p, first)
            if len(members) > 1 and self.rng.random() < 0.1:
                self.wait(p, self.rng.choice([m for m in members if m != first]))
            members.append(p)
        if self.rng.random() < 0.2:  # an extra resource nobody wants
            self.emit(self.base, "HOLD", members[0], self.new_resource())
            self.edges += 1
        if self.rng.random() < 0.05:  # waiting on a resource nobody holds
            self.emit(self.base + WINDOW, "WAIT", members[-1], self.new_resource())
            self.edges += 1

    def cycle_length(self, lo=2):
        return min(self.max_cycle, lo + int(self.rng.expovariate(1 / 2.5)))

    def close(self, procs, times):
        """Record a planted cycle, procs in wait-for order, in canonical form."""
        n = len(procs)
        nodes = []
        for i, p in enumerate(procs):
            nodes += [p, self.held[procs[(i + 1) % n]][0]]
        start = min(range(0, len(nodes), 2), key=lambda k: nodes[k])  # smallest by string, as the detector
        formed = max(times[(procs[i], procs[(i + 1) % n])] for i in range(n))
        self.cycles.append((tuple(nodes[start:] + nodes[:start]), self.stamp(formed)))

    def chain(self, procs, times):
        for a, b in zip(procs, procs[1:]):
            times[(a, b)] = self.wait(a, b)

    def cluster(self):
        times = {}
        kind = self.rng.choices(("single", "hub", "theta"), (0.7, 0.2, 0.1))[0]
        self.kinds[kind] += 1
        if kind == "single":
            procs = [self.new_process() for _ in range(self.cycle_length())]
            self.chain(procs + procs[:1], times)
            loops = [procs]
        elif kind == "hub":
            hub = self.new_process()
            loops = []
            for _ in range(self.rng.randint(2, 3)):
                procs = [hub] + [self.new_process() for _ in range(self.cycle_length() - 1)]
                self.chain(procs + [hub], times)
                loops.append(procs)
        else:
            # u -> ... -> v along one path, then back to u along two different paths
            path = [self.new_process() for _ in range(min(self.cycle_length(), self.max_cycle - 1))]
            u, v = path[0], path[-1]
            self.chain(path, times)
            loops = []
            for k in (0, self.rng.randint(1, self.max_cycle - len(path))):
                back = [self.new_process() for _ in range(k)]
                self.chain([v] + back + [u], times)
                loops.append(path + back)
        for procs in loops:
            self.close(procs, times)
        members = list(dict.fromkeys(p for loop in loops for p in loop))

        for _ in range(self.rng.choice((0, 0, 1, 1, 2, 3))):  # feeders: blocked, not deadlocked
            target = self.rng.choice(members)
            for _ in range(self.rng.randint(1, 5)):
                f = self.new_process()
                self.wait(f, target)
                target = f
        if self.rng.random() < 0.3:  # a tail: a member also waits on a running process
            src = self.rng.choice(members)
            for _ in range(self.rng.randint(1, 3)):
                t = self.new_process()
                self.wait(src, t)
                src = t
        if self.recent and self.rng.random() < 0.2:  # a bridge from an earlier cluster
            bridge = [self.new_process() for _ in range(self.rng.randint(1, 3))]
            src = self.rng.choice(self.rng.choice(self.recent))
            self.wait(src, bridge[0])
            self.chain(bridge + [self.rng.choice(members)], {})
            self.bridge_procs += len(bridge)
        self.recent = (self.recent + [members])[-RECENT_CLUSTERS:]

    # --- days and output -------------------------------------------------------

    def malformed_line(self, t):
        ts = self.stamp(t)
        n = self.rng.randrange(1, 10_000)
        return self.rng.choice((
            f"{ts}  INFO  scheduler heartbeat ok",
            f"{ts}  WAIT  P{n}",               # truncated
            f"{ts}  WAIT  Q{n}  R{n}",         # neither a process nor a resource
            f"{ts}  HOLD  P{n}  P{n + 1}",     # two processes
            f"{ts}"[:13],                      # cut off mid-timestamp
            "",
        ))

    def run(self, out):
        written = 0
        for d in range(self.days):
            self.day = (START + timedelta(days=d)).isoformat()
            goal = self.target * (d + 1) // self.days - written
            while len(self.buf) < goal:
                self.start_structure()
                if self.rng.random() < self.cluster_rate:
                    self.cluster()
                else:
                    self.background_tree()
            bad = int(len(self.buf) * self.malformed_rate + self.rng.random())
            for _ in range(bad):
                t = self.rng.randrange(DAY)
                self.buf.append((t, self.malformed_line(t)))
            self.malformed += bad
            self.buf.sort(key=lambda x: x[0])
            out.writelines(line + "\n" for _, line in self.buf)
            written += len(self.buf)
            self.buf = []
        self.lines = written

    def truth(self, seed):
        cycles = sorted(self.cycles, key=lambda c: (len(c[0]), c[0]))
        deadlocked = {n for nodes, _ in cycles for n in nodes[::2]}
        return {
            "seed": seed,
            "lines": self.lines,
            "malformed_lines": self.malformed,
            "processes": self.n_proc,
            "resources": self.n_res,
            "edges": self.edges,
            "cycles": len(cycles),
            "deadlocked_processes": len(deadlocked),
            "deadlocked_share": round(len(deadlocked) / max(1, self.n_proc), 5),
            # pruning keeps cycle members plus the bridges between clusters
            "candidate_processes": len(deadlocked) + self.bridge_procs,
            "cluster_kinds": dict(self.kinds),
            "cycle_lengths": dict(sorted(Counter(len(n) // 2 for n, _ in cycles).items())),
            "deadlocks_per_day": sorted(Counter(f.split()[0] for _, f in cycles).items()),
            "cycle_list": [[list(n), f] for n, f in cycles],
        }


def generate(lines, out, days=30, seed=0, cluster_rate=0.01, malformed_rate=0.005,
             retry_rate=0.1, max_cycle=10):
    """Write the log to `out` and return its ground truth as a dict."""
    g = Generator(lines, days, seed, cluster_rate, malformed_rate, retry_rate, max_cycle)
    g.run(out)
    return g.truth(seed)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--lines", type=int, required=True, help="approximate number of log lines")
    ap.add_argument("--days", type=int, default=30, help="days the log spans (default: 30)")
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--cluster-rate", type=float, default=0.01,
                    help="share of structures that are deadlock clusters (default: 0.01)")
    ap.add_argument("--malformed-rate", type=float, default=0.005, help="default: 0.005")
    ap.add_argument("--retry-rate", type=float, default=0.1, help="chance a WAIT is repeated (default: 0.1)")
    ap.add_argument("--max-cycle", type=int, default=10, help="longest planted cycle, in processes (default: 10)")
    ap.add_argument("-o", "--output", required=True, help="output log file")
    ap.add_argument("--truth", help="ground-truth JSON (default: OUTPUT.truth.json)")
    args = ap.parse_args()

    with open(args.output, "w") as f:
        truth = generate(args.lines, f, args.days, args.seed, args.cluster_rate,
                         args.malformed_rate, args.retry_rate, args.max_cycle)
    with open(args.truth or args.output + ".truth.json", "w") as f:
        json.dump(truth, f)
    summary = {k: v for k, v in truth.items() if k not in ("cycle_list", "deadlocks_per_day")}
    print(json.dumps(summary), file=sys.stderr)


if __name__ == "__main__":
    main()
