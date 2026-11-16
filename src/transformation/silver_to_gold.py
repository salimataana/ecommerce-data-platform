import logging
import time
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.observability.metrics import (
    GOLD_DURATION,
    GOLD_ERRORS,
    GOLD_INPUT_ROWS,
    GOLD_OUTPUT_ROWS,
    GOLD_QUALITY_CHECKS,
    GOLD_QUALITY_RULES_PASSED,
    GOLD_QUALITY_RULES_TOTAL,
    GOLD_QUALITY_STATUS,
    GOLD_RUNS,
)
from src.observability.push_metrics import publish_metrics
from src.quality.data_quality import GoldDataValidator
from src.utils.spark_session import create_spark_session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)


logger = logging.getLogger(__name__)

SILVER_DIR = Path("data/silver/products")
GOLD_DIR = Path("data/gold/products")


class SilverToGoldTransformer:
    """
    Pipeline de transformation Silver vers Gold.

    Responsabilités :
    - Lire les données Silver au format Parquet.
    - Calculer les indicateurs par catégorie.
    - Enregistrer les résultats Gold au format Parquet.
    - Gérer la session Spark.
    """

    def __init__(
        self,
        silver_dir=SILVER_DIR,
        gold_dir=GOLD_DIR,
        spark: SparkSession | None = None,
    ):
        self.silver_dir = Path(silver_dir)
        self.gold_dir = Path(gold_dir)

        # Réutiliser la session injectée ou créer une session.
        self._owns_spark = spark is None

        self.spark = (
            spark if spark is not None else create_spark_session("SilverToGold")
        )

    def read(self) -> DataFrame:
        """Lit les données Silver au format Parquet."""

        logger.info(
            "Lecture des données Silver : %s",
            self.silver_dir,
        )

        return self.spark.read.parquet(str(self.silver_dir))

    def transform(self, df: DataFrame) -> DataFrame:
        """Calcule les indicateurs métiers par catégorie."""

        df_gold = (
            df.groupBy("category")
            .agg(
                F.count("*").alias("total_products"),
                F.round(
                    F.avg("price"),
                    2,
                ).alias("average_price"),
                F.sum("stock").alias("total_stock"),
            )
            .orderBy("category")
        )

        return df_gold

    def write(self, df: DataFrame) -> None:
        """Enregistre les indicateurs Gold en Parquet."""

        (df.write.mode("overwrite").parquet(str(self.gold_dir)))

        logger.info(
            "Données Gold enregistrées : %s",
            self.gold_dir,
        )

    def validate(self, df: DataFrame) -> None:
        """Vérifie la qualité Gold et enregistre les métriques."""

        GOLD_QUALITY_STATUS.set(0)
        GOLD_QUALITY_RULES_TOTAL.set(0)
        GOLD_QUALITY_RULES_PASSED.set(0)

        try:
            validator = GoldDataValidator(df)
            is_valid = validator.validate()

            summary = validator.validation_summary

            if summary is not None:
                GOLD_QUALITY_RULES_TOTAL.set(summary["total_rules"])
                GOLD_QUALITY_RULES_PASSED.set(summary["passed_rules"])

            if not is_valid:
                raise ValueError("Échec de la validation des données Gold")

            GOLD_QUALITY_STATUS.set(1)
            GOLD_QUALITY_CHECKS.labels(status="success").inc()

            logger.info("Validation des données Gold réussie")

        except Exception:
            GOLD_QUALITY_STATUS.set(0)
            GOLD_QUALITY_CHECKS.labels(status="failure").inc()
            logger.exception("Validation des données Gold échouée")
            raise

    def run(self) -> None:
        """Exécute le pipeline Silver vers Gold avec observabilité."""

        logger.info("Démarrage du pipeline Silver vers Gold")

        start_time = time.perf_counter()

        try:
            # 1. Lire les données Silver
            df_silver = self.read()

            input_rows = df_silver.count()

            GOLD_INPUT_ROWS.set(input_rows)

            logger.info(
                "Nombre de produits Silver : %s",
                input_rows,
            )

            # 2. Transformer les données Silver en Gold
            df_gold = self.transform(df_silver)

            # 3. Vérifier la qualité des données Gold
            self.validate(df_gold)

            # 4. Mesurer le nombre de catégories Gold
            output_rows = df_gold.count()

            GOLD_OUTPUT_ROWS.set(output_rows)

            logger.info(
                "Nombre de catégories Gold : %s",
                output_rows,
            )

            # 5. Afficher les indicateurs
            print("\n=== INDICATEURS PAR CATÉGORIE ===")
            df_gold.show(50, truncate=False)

            # 6. Enregistrer les données Gold
            self.write(df_gold)

            GOLD_RUNS.labels(status="success").inc()

            logger.info("Pipeline Silver vers Gold terminé avec succès")

        except Exception:
            GOLD_ERRORS.inc()
            GOLD_RUNS.labels(status="failure").inc()

            logger.exception("Échec du pipeline Silver vers Gold")

            raise

        finally:
            duration = time.perf_counter() - start_time

            GOLD_DURATION.observe(duration)

            logger.info(
                "Durée totale Silver vers Gold : %.2f secondes",
                duration,
            )

            publish_metrics("ecommerce_silver_to_gold")

            # Fermer uniquement les sessions Spark créées par cette classe.
            if self._owns_spark:
                self.spark.stop()


def silver_to_gold():
    """Point d'entrée historique du pipeline."""

    transformer = SilverToGoldTransformer()
    transformer.run()


if __name__ == "__main__":
    silver_to_gold()
