import pytest

from deadlock_detector import create_spark, detect_deadlocks
from deadlock_detector.df_engine import detect_deadlocks_df


@pytest.fixture(scope="session")
def spark():
    spark = create_spark(cores=2, driver_memory="1g")
    yield spark
    spark.stop()


@pytest.fixture(scope="session")
def sc(spark):
    return spark.sparkContext


@pytest.fixture(params=["rdd", "df"])
def detect(request, spark, tmp_path):
    """Run the detector (each engine in turn) on log text (or a path) and return the Report."""

    def run(log, **kwargs):
        if isinstance(log, str) and "\n" in log:
            path = tmp_path / "events.log"
            path.write_text(log)
            log = str(path)
        if request.param == "df":
            return detect_deadlocks_df(spark, str(log), partitions=4, **kwargs)
        return detect_deadlocks(spark.sparkContext, str(log), partitions=4, **kwargs)

    return run
