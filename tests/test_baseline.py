import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
from generate_realistic_log import generate  # noqa: E402

from deadlock_detector.baseline import detect  # noqa: E402


def test_baseline_matches_ground_truth(tmp_path):
    path = tmp_path / "realistic.log"
    with open(path, "w") as f:
        truth = generate(lines=4000, out=f, days=3, seed=7, cluster_rate=0.05)
    result = detect(path)
    assert (result["processes"], result["resources"], result["edges"]) == (
        truth["processes"], truth["resources"], truth["edges"])
    assert sorted((list(n), f) for n, f in result["cycles"]) == sorted(map(tuple, truth["cycle_list"]))
    # SCCs keep only cycle members: tighter than degree pruning, which also keeps bridges
    assert result["scc_processes"] == truth["deadlocked_processes"]


def test_baseline_sample_log():
    result = detect(ROOT / "data" / "sample.log")
    assert [n for n, _ in result["cycles"]] == [
        ("P101", "R002", "P106", "R006"),
        ("P105", "R003", "P107", "R007"),
        ("P102", "R005", "P103", "R009", "P108", "R004"),
    ]
