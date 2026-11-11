from src.utils.spark_session import create_spark_session

spark = create_spark_session("VerifySilver")

try:
    df = spark.read.parquet("data/silver/products")

    print("Nombre de produits Silver :", df.count())

    df.select(
        "id", "title", "category", "price", "stock"
    ).show(5, truncate=False)

finally:
    spark.stop()