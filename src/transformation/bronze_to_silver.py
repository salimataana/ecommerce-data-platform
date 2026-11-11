import logging
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import explode, col

from src.utils.spark_session import create_spark_session
from src.transformation.product_transformations import (
    flatten_products,
    convert_dates,
    normalize_products,
)
from src.quality.data_quality import validate_products


logger = logging.getLogger(__name__)

BRONZE_DIR = Path("data/bronze/products")
SILVER_DIR = Path("data/silver/products")


class BronzeToSilverTransformer:
    """
    Pipeline de transformation Bronze vers Silver.

    Responsabilités :
    - Sélectionner le dernier fichier Bronze.
    - Lire les données JSON.
    - Transformer les données avec PySpark.
    - Valider la qualité des données.
    - Enregistrer les données au format Parquet.
    """

    def __init__(
        self,
        bronze_dir=BRONZE_DIR,
        silver_dir=SILVER_DIR,
        spark: SparkSession | None = None,
    ):
        self.bronze_dir = Path(bronze_dir)
        self.silver_dir = Path(silver_dir)

        # Si une session est fournie, on la réutilise.
        # Sinon, on crée notre propre session Spark.
        self._owns_spark = spark is None
        self.spark = spark or create_spark_session("BronzeToSilver")

    def get_latest_bronze_file(self) -> Path:
        """Sélectionne le fichier JSON Bronze le plus récent."""

        files = list(self.bronze_dir.glob("*.json"))

        if not files:
            raise FileNotFoundError("Aucun fichier Bronze trouvé")

        latest_file = max(
            files,
            key=lambda file: file.stat().st_mtime,
        )

        logger.info("Fichier Bronze sélectionné : %s", latest_file)

        return latest_file

    def read(self, file_path: Path) -> DataFrame:
        """Lit le fichier JSON Bronze."""

        return (
            self.spark.read
            .option("multiLine", True)
            .json(str(file_path))
        )

    def transform(self, df: DataFrame) -> DataFrame:
        """Transforme les données JSON en données Silver."""

        # Chaque produit devient une ligne.
        df_products = df.select(
            explode(col("products")).alias("product")
        )

        # Extraction des champs du produit.
        df_silver = df_products.select("product.*")

        # Transformations métier.
        df_silver = flatten_products(df_silver)
        df_silver = convert_dates(df_silver)
        df_silver = normalize_products(df_silver)

        return df_silver

    def validate(self, df: DataFrame) -> None:
        """Vérifie les règles de qualité des données."""

        is_valid = validate_products(df)

        if not is_valid:
            raise ValueError(
                "Échec de la validation des données"
            )

        logger.info("Validation des données réussie")

    def write(self, df: DataFrame) -> None:
        """Enregistre les données Silver au format Parquet."""

        (
            df.write
            .mode("overwrite")
            .parquet(str(self.silver_dir))
        )

        logger.info(
            "Données Silver enregistrées : %s",
            self.silver_dir,
        )

    def run(self) -> None:
        """Exécute le pipeline complet Bronze vers Silver."""

        try:
            latest_file = self.get_latest_bronze_file()

            df = self.read(latest_file)

            df_transformed = self.transform(df)

            self.validate(df_transformed)

            logger.info(
                "Nombre de produits : %s",
                df_transformed.count(),
            )

            df_transformed.printSchema()

            df_transformed.select(
                "id",
                "title",
                "category",
                "price",
                "stock",
            ).show(5, truncate=20, vertical=True)

            self.write(df_transformed)

            logger.info(
                "Pipeline Bronze vers Silver terminé avec succès"
            )

        finally:
            # Ne ferme Spark que si cette classe l'a créé.
            if self._owns_spark:
                self.spark.stop()


# ============================================================
# Compatibilité avec les commandes existantes et Airflow
# ============================================================

def read_bronze():
    """Point d'entrée historique du pipeline."""

    transformer = BronzeToSilverTransformer()
    transformer.run()


if __name__ == "__main__":
    read_bronze()