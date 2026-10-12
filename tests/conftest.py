import pytest

from deadlock_detector import create_spark, detect_deadlocks


@pytest.fixture(scope="session")
def sc():
    spark = create_spark(cores=2, driver_memory="1g")
    yield spark.sparkContext
    spark.stop()


@pytest.fixture
def detect(sc, tmp_path):
    """Run the detector on log text (or a path) and return the Report."""

    def run(log, **kwargs):
        if isinstance(log, str) and "\n" in log:
            path = tmp_path / "events.log"
            path.write_text(log)
            log = str(path)
        return detect_deadlocks(sc, str(log), partitions=4, **kwargs)

    return run
