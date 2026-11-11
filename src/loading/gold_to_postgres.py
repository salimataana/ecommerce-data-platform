import logging
from pathlib import Path

import pandas as pd

from pyspark.sql import DataFrame, SparkSession
from sqlalchemy import Engine, text

from src.utils.spark_session import create_spark_session
from src.utils.db_connection import get_engine


logger = logging.getLogger(__name__)

GOLD_DIR = Path("data/gold/products")

TABLE_NAME = "products_by_category"
SCHEMA_NAME = "public"


class PostgresLoader:
    """
    Chargement des données Gold dans PostgreSQL.

    Responsabilités :
    - Lire les fichiers Parquet Gold.
    - Convertir les données Spark en Pandas.
    - Vérifier l'existence de la table PostgreSQL.
    - Remplacer les données de la table.
    - Gérer les connexions et les transactions.
    """

    def __init__(
        self,
        gold_dir=GOLD_DIR,
        table_name=TABLE_NAME,
        schema_name=SCHEMA_NAME,
        spark: SparkSession | None = None,
        engine: Engine | None = None,
    ):
        self.gold_dir = Path(gold_dir)
        self.table_name = table_name
        self.schema_name = schema_name

        # Gestion de la session Spark
        self._owns_spark = spark is None

        self.spark = (
            spark
            if spark is not None
            else create_spark_session("GoldToPostgres")
        )

        # Gestion de la connexion PostgreSQL
        self._owns_engine = engine is None

        self.engine = (
            engine
            if engine is not None
            else get_engine()
        )

    def read(self) -> DataFrame:
        """Lit les données Gold au format Parquet."""

        logger.info(
            "Lecture des données Gold : %s",
            self.gold_dir,
        )

        return self.spark.read.parquet(
            str(self.gold_dir)
        )

    def to_pandas(self, df: DataFrame) -> pd.DataFrame:
        """Convertit le DataFrame Spark en Pandas."""

        return df.toPandas()

    def table_exists(self, conn) -> bool:
        """Vérifie si la table PostgreSQL existe."""

        query = text("""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = :schema_name
                  AND table_name = :table_name
            )
        """)

        result = conn.execute(
            query,
            {
                "schema_name": self.schema_name,
                "table_name": self.table_name,
            },
        )

        return bool(result.scalar())

    def load(self, df_pandas: pd.DataFrame) -> None:
        """
        Remplace les données de la table PostgreSQL.

        Les opérations DELETE et INSERT sont exécutées
        dans une même transaction.
        """

        with self.engine.begin() as conn:

            if self.table_exists(conn):

                # Noms de table/schéma provenant uniquement
                # de la configuration du projet.
                if (
                    self.schema_name != "public"
                    or self.table_name != "products_by_category"
                ):
                    raise ValueError(
                        "Table PostgreSQL non autorisée"
                    )

                conn.execute(
                    text(
                        "DELETE FROM public.products_by_category"
                    )
                )

            df_pandas.to_sql(
                name=self.table_name,
                con=conn,
                schema=self.schema_name,
                if_exists="append",
                index=False,
            )

        logger.info(
            "Chargement PostgreSQL terminé : %s lignes",
            len(df_pandas),
        )

    def run(self) -> None:
        """Exécute le chargement complet Gold vers PostgreSQL."""

        try:
            df_gold = self.read()

            df_pandas = self.to_pandas(df_gold)

            self.load(df_pandas)

            print("Chargement PostgreSQL réussi !")
            print(
                "Nombre de catégories :",
                len(df_pandas),
            )

        finally:
            if self._owns_engine:
                self.engine.dispose()

            if self._owns_spark:
                self.spark.stop()


# ============================================================
# Compatibilité avec Airflow
# ============================================================

def load_gold_to_postgres():
    """Point d'entrée historique du pipeline."""

    loader = PostgresLoader()
    loader.run()


if __name__ == "__main__":
    load_gold_to_postgres()