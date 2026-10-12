#!/usr/bin/env python3
"""Generate a synthetic resource-allocation log with a known number of deadlocks.

Processes are laid out in blocks of --cycle-size. Within a block, process i
holds resource i and waits on resource i-1, forming a chain; the last process
of each block also holds the block's first-waited resource, closing the chain
into a single deadlock cycle. Blocks are separated by unused ids so they never
interact. With --steps S the log contains (S // cycle_size) deadlocks.

    python scripts/generate_log.py --steps 2499 -o data/rag_5k.log   # 249 deadlocks
"""

import argparse
import sys

TIMESTAMP = "2025-10-31 12:15:59"
GAP = 5  # unused ids between blocks


def generate(steps, cycle_size, out):
    p = r = 1
    for i in range(1, steps + 1):
        out.write(f"{TIMESTAMP}  WAIT P{p}  R{r - 1}\n")
        out.write(f"{TIMESTAMP}  HOLD P{p}  R{r}\n")
        if i % cycle_size == 0:
            out.write(f"{TIMESTAMP}  HOLD P{p}  R{r - cycle_size}\n")
            p += GAP
            r += GAP
        p += 1
        r += 1


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--steps", type=int, required=True, help="number of processes to emit (before gaps)")
    ap.add_argument("--cycle-size", type=int, default=10, help="processes per deadlock cycle (default: 10)")
    ap.add_argument("-o", "--output", help="output file (default: stdout)")
    args = ap.parse_args()

    if args.output:
        with open(args.output, "w") as f:
            generate(args.steps, args.cycle_size, f)
    else:
        generate(args.steps, args.cycle_size, sys.stdout)


if __name__ == "__main__":
    main()
