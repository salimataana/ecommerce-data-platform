import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    spark_session = (
        SparkSession.builder
        .master("local[2]")
        .appName("EcommerceUnitTests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    spark_session.sparkContext.setLogLevel("ERROR")

    yield spark_session

    spark_session.stop()