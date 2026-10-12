import sys
from pathlib import Path

from deadlock_detector.df_engine import parse
from deadlock_detector.parsing import parse_line

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
from generate_realistic_log import generate  # noqa: E402

# Lines chosen to hit every branch of parse_line, including the ones real logs rarely contain.
TRICKY = [
    "2025-01-01 00:00:01 WAIT P1 R1",
    "2025-01-01 00:00:01\tHOLD\tP1\tR1",
    "  \t2025-01-01 00:00:01  wait  P2  R2  \t",   # leading/trailing tabs, lowercase level
    "2025-01-01 00:00:01 HOLD R3 P3",              # resource first
    "2025-01-01T00:00:01 WAIT P4 R4",              # one-token timestamp
    "2025-01-01 00:00:01 WAIT P5, R5,",            # trailing commas
    "2025-01-01 00:00:01 WAIT P6 R6 extra tokens", # more than five tokens
    "2025-01-01 00:00:01 WAIT P7,R7 R8",           # glued token still starts with P
    "2025-01-01 00:00:01 WAIT , R9",               # empty token after comma stripping
    "2025-01-01 00:00:01 WAIT P10",                # four tokens: time read as the level
    "2025-01-01 00:00:01 HOLD P11 P12",
    "2025-01-01 00:00:01 RELEASE P13 R13",
    "2025-01-01 00:00:01 WAIT Q14 R14",
    "2025-01-01 00:00:01 WAIT P R",                # bare prefixes count
    "",
    "   ",
    "2025-01-01",
]


def sql_events(spark, lines):
    df = spark.createDataFrame([(line,) for line in lines], ["value"])
    return sorted(tuple(r) for r in parse(df).collect())


def py_events(lines):
    return sorted(tuple(e) for e in map(parse_line, lines) if e is not None)


def test_sql_parser_matches_python_parser(spark):
    assert sql_events(spark, TRICKY) == py_events(TRICKY)


def test_sql_parser_on_generated_log(spark, tmp_path):
    path = tmp_path / "gen.log"
    with open(path, "w") as f:
        generate(lines=3000, out=f, days=2, seed=3)
    lines = path.read_text().splitlines()
    assert sql_events(spark, lines) == py_events(lines)
