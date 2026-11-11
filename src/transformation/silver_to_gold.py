from pyspark.sql import functions as F

from src.utils.spark_session import create_spark_session


SILVER_DIR = "data/silver/products"
GOLD_DIR = "data/gold/products"


def silver_to_gold():

    spark = create_spark_session("SilverToGold")

    try:
        # Lire les données Silver
        df_silver = spark.read.parquet(SILVER_DIR)

        print("Nombre de produits Silver :", df_silver.count())

        #  Calculer les indicateurs par catégorie
        df_gold = (
            df_silver
            .groupBy("category")
            .agg(
                F.count("*").alias("total_products"),
                F.round(F.avg("price"), 2).alias("average_price"),
                F.sum("stock").alias("total_stock")
            )
            .orderBy("category")
        )

        # Afficher les résultats
        print("\n=== INDICATEURS PAR CATÉGORIE ===")

        df_gold.show(50, truncate=False)

        # Enregistrer les indicateurs Gold au format Parquet
        df_gold.write \
            .mode("overwrite") \
            .parquet(GOLD_DIR)

        print("Données Gold enregistrées :", GOLD_DIR)

    finally:
        spark.stop()


if __name__ == "__main__":
    silver_to_gold()