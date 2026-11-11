import os

# Éviter le conflit avec l'installation système Spark 3.5.1
os.environ.pop("SPARK_HOME", None)

from pyspark.sql import SparkSession


def create_spark_session(app_name="EcommerceDataPlatform"):

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[2]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    return spark