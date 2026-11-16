import logging
import time
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, explode

from src.observability.metrics import (
    SILVER_DURATION,
    SILVER_ERRORS,
    SILVER_INPUT_ROWS,
    SILVER_OUTPUT_ROWS,
    SILVER_QUALITY_CHECKS,
    SILVER_QUALITY_RULES_PASSED,
    SILVER_QUALITY_RULES_TOTAL,
    SILVER_QUALITY_STATUS,
    SILVER_RUNS,
)
from src.observability.push_metrics import publish_metrics
from src.quality.data_quality import validate_products
from src.transformation.product_transformations import (
    convert_dates,
    flatten_products,
    normalize_products,
)
from src.utils.spark_session import create_spark_session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

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

        return self.spark.read.option("multiLine", True).json(str(file_path))

    def transform(self, df: DataFrame) -> DataFrame:
        """Transforme les données JSON en données Silver."""

        # Chaque produit devient une ligne.
        df_products = df.select(explode(col("products")).alias("product"))

        # Extraction des champs du produit.
        df_silver = df_products.select("product.*")

        # Transformations métier.
        df_silver = flatten_products(df_silver)
        df_silver = convert_dates(df_silver)
        df_silver = normalize_products(df_silver)

        return df_silver

    def validate(self, df: DataFrame) -> None:
        """Vérifie la qualité Silver et enregistre les métriques."""

        SILVER_QUALITY_STATUS.set(0)
        SILVER_QUALITY_RULES_TOTAL.set(0)
        SILVER_QUALITY_RULES_PASSED.set(0)

        try:
            summary = validate_products(df, return_summary=True)

            SILVER_QUALITY_RULES_TOTAL.set(summary["total_rules"])
            SILVER_QUALITY_RULES_PASSED.set(summary["passed_rules"])

            if not summary["success"]:
                raise ValueError("Échec de la validation des données Silver")

            SILVER_QUALITY_STATUS.set(1)
            SILVER_QUALITY_CHECKS.labels(status="success").inc()

            logger.info(
                "Qualité Silver validée : %s/%s règles réussies",
                summary["passed_rules"],
                summary["total_rules"],
            )

        except Exception:
            SILVER_QUALITY_STATUS.set(0)
            SILVER_QUALITY_CHECKS.labels(status="failure").inc()

            logger.exception("Validation des données Silver échouée")
            raise

    def write(self, df: DataFrame) -> None:
        """Enregistre les données Silver au format Parquet."""

        (df.write.mode("overwrite").parquet(str(self.silver_dir)))

        logger.info(
            "Données Silver enregistrées : %s",
            self.silver_dir,
        )

    def run(self) -> None:
        """Exécute le pipeline Bronze vers Silver avec observabilité."""

        logger.info("Démarrage du pipeline Bronze vers Silver")

        start_time = time.perf_counter()

        try:
            latest_file = self.get_latest_bronze_file()

            df = self.read(latest_file)

            # Nombre de produits présents dans Bronze.
            input_rows = df.select(explode(col("products")).alias("product")).count()

            SILVER_INPUT_ROWS.set(input_rows)

            logger.info("Nombre de produits Bronze : %s", input_rows)

            # Transformation.
            df_transformed = self.transform(df)

            # Validation de la qualité.
            self.validate(df_transformed)

            # Nombre de produits après transformation.
            output_rows = df_transformed.count()

            SILVER_OUTPUT_ROWS.set(output_rows)

            logger.info(
                "Nombre de produits Silver : %s",
                output_rows,
            )

            df_transformed.printSchema()

            df_transformed.select(
                "id",
                "title",
                "category",
                "price",
                "stock",
            ).show(5, truncate=20, vertical=True)

            # Enregistrement au format Parquet.
            self.write(df_transformed)

            SILVER_RUNS.labels(status="success").inc()

            logger.info("Pipeline Bronze vers Silver terminé avec succès")

        except Exception:
            SILVER_ERRORS.inc()
            SILVER_RUNS.labels(status="failure").inc()

            logger.exception("Échec du pipeline Bronze vers Silver")

            raise

        finally:
            duration = time.perf_counter() - start_time

            SILVER_DURATION.observe(duration)

            logger.info(
                "Durée totale Bronze vers Silver : %.2f secondes",
                duration,
            )

            publish_metrics("ecommerce_bronze_to_silver")

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
