import logging
from pathlib import Path
from src.quality.data_quality import GoldDataValidator
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.utils.spark_session import create_spark_session


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
            spark
            if spark is not None
            else create_spark_session("SilverToGold")
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

        (
            df.write
            .mode("overwrite")
            .parquet(str(self.gold_dir))
        )

        logger.info(
            "Données Gold enregistrées : %s",
            self.gold_dir,
        )

    def validate(self, df: DataFrame) -> None:
        """Vérifie la qualité des indicateurs Gold."""

        validator = GoldDataValidator(df)

        if not validator.validate():
            raise ValueError(
                "Échec de la validation des données Gold"
            )

    def run(self) -> None:
        """Exécute le pipeline complet Silver vers Gold."""

        try:
            # 1. Lire les données Silver
            df_silver = self.read()

            logger.info(
                "Nombre de produits Silver : %s",
                df_silver.count(),
            )

            # 2. Transformer les données Silver en Gold
            df_gold = self.transform(df_silver)

            # 3. Vérifier la qualité des données Gold
            self.validate(df_gold)

            # 4. Afficher les indicateurs
            print("\n=== INDICATEURS PAR CATÉGORIE ===")
            df_gold.show(50, truncate=False)

            # 5. Enregistrer les données Gold
            self.write(df_gold)

            logger.info(
                "Pipeline Silver vers Gold terminé avec succès"
            )

        finally:
            if self._owns_spark:
                self.spark.stop()


def silver_to_gold():
    """Point d'entrée historique du pipeline."""

    transformer = SilverToGoldTransformer()
    transformer.run()


if __name__ == "__main__":
    silver_to_gold()