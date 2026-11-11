import logging
from pathlib import Path

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from sqlalchemy import Engine, text

from src.utils.db_connection import get_engine
from src.utils.spark_session import create_spark_session

logger = logging.getLogger(__name__)

GOLD_DIR = Path("data/gold/products")

TABLE_NAME = "products_by_category"
SCHEMA_NAME = "public"

TEST_TABLE_NAME = "products_by_category_integration_test"


class PostgresLoader:
    """
    Chargement des données Gold dans PostgreSQL.

    Responsabilités :
    - Lire les fichiers Parquet Gold.
    - Convertir les données Spark en Pandas.
    - Vérifier la table PostgreSQL cible.
    - Remplacer les anciennes données.
    - Garantir la transaction DELETE + INSERT.
    - Gérer les ressources Spark et SQLAlchemy.
    """

    def __init__(
        self,
        gold_dir=GOLD_DIR,
        table_name=TABLE_NAME,
        schema_name=SCHEMA_NAME,
        spark: SparkSession | None = None,
        engine: Engine | None = None,
        allow_test_table: bool = False,
    ):
        self.gold_dir = Path(gold_dir)
        self.table_name = table_name
        self.schema_name = schema_name
        self.allow_test_table = allow_test_table

        # Gestion de la session Spark
        self._owns_spark = spark is None

        self.spark = (
            spark if spark is not None else create_spark_session("GoldToPostgres")
        )

        # Gestion de la connexion PostgreSQL
        self._owns_engine = engine is None

        self.engine = engine if engine is not None else get_engine()

    def read(self) -> DataFrame:
        """Lit les données Gold au format Parquet."""

        logger.info(
            "Lecture des données Gold : %s",
            self.gold_dir,
        )

        return self.spark.read.parquet(str(self.gold_dir))

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

    def _validate_target(self) -> None:
        """
        Vérifie que la table cible est autorisée.

        Par défaut :
        - public.products_by_category

        Pour les tests, avec allow_test_table=True :
        - public.products_by_category_integration_test
        """

        production_target = (
            self.schema_name == SCHEMA_NAME and self.table_name == TABLE_NAME
        )

        test_target = (
            self.allow_test_table
            and self.schema_name == SCHEMA_NAME
            and self.table_name == TEST_TABLE_NAME
        )

        if not (production_target or test_target):
            raise ValueError(
                f"Table PostgreSQL non autorisée : {self.schema_name}.{self.table_name}"
            )

    def load(self, df_pandas: pd.DataFrame) -> None:
        """
        Remplace les données de la table PostgreSQL.

        Étapes :
        1. Vérifier la table cible.
        2. Ouvrir une transaction.
        3. Supprimer les anciennes données si nécessaire.
        4. Insérer les nouvelles données.
        5. Valider la transaction automatiquement.

        Si une erreur survient, la transaction est annulée.
        """

        # Vérification avant toute opération SQL
        self._validate_target()

        with self.engine.begin() as conn:
            if self.table_exists(conn):
                if self.table_name == TEST_TABLE_NAME:
                    delete_query = text(
                        "DELETE FROM public.products_by_category_integration_test"
                    )
                else:
                    delete_query = text("DELETE FROM public.products_by_category")

                conn.execute(delete_query)

                logger.info(
                    "Anciennes données supprimées : %s.%s",
                    self.schema_name,
                    self.table_name,
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
            # 1. Lire les données Gold
            df_gold = self.read()

            # 2. Convertir Spark vers Pandas
            df_pandas = self.to_pandas(df_gold)

            # 3. Charger dans PostgreSQL
            self.load(df_pandas)

            logger.info(
                "Chargement PostgreSQL réussi : %s catégories",
                len(df_pandas),
            )

            print("Chargement PostgreSQL réussi !")
            print(
                "Nombre de catégories :",
                len(df_pandas),
            )

        finally:
            # Fermer uniquement les ressources créées par la classe
            if self._owns_engine:
                self.engine.dispose()

            if self._owns_spark:
                self.spark.stop()


# ============================================================
# COMPATIBILITÉ AVEC AIRFLOW
# ============================================================


def load_gold_to_postgres():
    """
    Point d'entrée historique du pipeline.

    Permet de conserver les appels existants depuis
    Airflow ou la ligne de commande.
    """

    loader = PostgresLoader()
    loader.run()


if __name__ == "__main__":
    load_gold_to_postgres()
