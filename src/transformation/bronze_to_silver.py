from pathlib import Path
from pyspark.sql.functions import explode, col
from src.utils.spark_session import create_spark_session
from src.transformation.product_transformations import (
    flatten_products,
    convert_dates,
    normalize_products
)
from src.quality.data_quality import validate_products


BRONZE_DIR = Path("data/bronze/products")
SILVER_DIR = Path("data/silver/products")


def read_bronze():

    spark = create_spark_session("BronzeToSilver")

    try:
        # 1. Rechercher les fichiers JSON dans Bronze
        files = list(BRONZE_DIR.glob("*.json"))

        if not files:
            raise FileNotFoundError("Aucun fichier Bronze trouvé")

        # 2. Sélectionner le fichier le plus récent
        latest_file = max(
            files,
            key=lambda file: file.stat().st_mtime
        )

        print("Fichier Bronze sélectionné :", latest_file)

        # 3. Lire le fichier JSON
        df = (
            spark.read
            .option("multiLine", True)
            .json(str(latest_file))
        )

        # 4. Transformer le tableau products en lignes
        df_products = df.select(
            explode(col("products")).alias("product")
        )

        # 5. Récupérer tous les champs de chaque produit
        df_silver = df_products.select("product.*")

        # Aplatir les champs imbriqués
        df_transformed = flatten_products(df_silver)

        df_transformed = convert_dates(df_transformed)

        df_transformed = normalize_products(df_transformed)

        # Vérifier les règles métiers sur les produits
        is_valid = validate_products(df_transformed)


        if not is_valid:
            raise ValueError("Échec de la validation des données")

        # 6. Afficher le nombre de produits
        print("\nNombre de produits :", df_transformed.count())

        # 7. Afficher le schéma complet
        print("\nSchéma des produits :")
        df_transformed.printSchema()

        # 8. Afficher cinq produits
        print("\nAperçu des produits :")
        df_transformed.select(
            "id",
            "title",
            "category",
            "price",
            "stock"
        ).show(5, truncate=20, vertical=True)

        # Enregistrer les données validées dans Silver
        df_transformed.write \
            .mode("overwrite") \
            .parquet(str(SILVER_DIR))

        print("Données Silver enregistrées :", SILVER_DIR)

    finally:
        # 9. Arrêter Spark, même en cas d'erreur
        spark.stop()


if __name__ == "__main__":
    read_bronze()