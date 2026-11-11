import pytest
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from sqlalchemy import text

from src.loading.gold_to_postgres import PostgresLoader
from src.utils.db_connection import get_engine

TEST_TABLE = "products_by_category_integration_test"


@pytest.fixture
def postgres_integration_environment(tmp_path, spark):
    """
    Prépare un environnement d'intégration :

    - Crée des données Gold en Parquet.
    - Utilise une table PostgreSQL dédiée aux tests.
    - Nettoie la table après l'exécution.
    """

    gold_dir = tmp_path / "gold" / "products"

    schema = StructType(
        [
            StructField("category", StringType(), False),
            StructField("total_products", LongType(), False),
            StructField("average_price", DoubleType(), False),
            StructField("total_stock", LongType(), False),
        ]
    )

    data = [
        ("smartphones", 2, 900.0, 25),
        ("laptops", 2, 1750.0, 13),
        ("fragrances", 1, 100.0, 20),
    ]

    df_gold = spark.createDataFrame(data, schema=schema)

    df_gold.write.mode("overwrite").parquet(str(gold_dir))

    engine = get_engine()

    loader = PostgresLoader(
        gold_dir=gold_dir,
        table_name=TEST_TABLE,
        schema_name="public",
        spark=spark,
        engine=engine,
        allow_test_table=True,
    )

    try:
        # S'assurer que la table de test n'existe pas.
        with engine.begin() as conn:
            conn.execute(
                text(
                    "DROP TABLE IF EXISTS public.products_by_category_integration_test"
                )
            )

        yield {
            "loader": loader,
            "engine": engine,
            "gold_dir": gold_dir,
        }

    finally:
        # Nettoyage de la table de test.
        with engine.begin() as conn:
            conn.execute(
                text(
                    "DROP TABLE IF EXISTS public.products_by_category_integration_test"
                )
            )

        engine.dispose()


def test_gold_to_postgres_integration(
    postgres_integration_environment,
):
    """
    Vérifie le chargement réel Gold → PostgreSQL.
    """

    environment = postgres_integration_environment

    loader = environment["loader"]
    engine = environment["engine"]

    # 1. Exécuter le chargement complet.
    loader.run()

    # 2. Vérifier le nombre de lignes dans PostgreSQL.
    with engine.connect() as conn:
        count = conn.execute(
            text("SELECT COUNT(*) FROM public.products_by_category_integration_test")
        ).scalar()

        assert count == 3

        # 3. Vérifier les données métier.
        result = (
            conn.execute(
                text("""
                SELECT
                    category,
                    total_products,
                    average_price,
                    total_stock
                FROM public.products_by_category_integration_test
                WHERE category = 'smartphones'
            """)
            )
            .mappings()
            .one()
        )

        assert result["category"] == "smartphones"
        assert result["total_products"] == 2
        assert result["average_price"] == pytest.approx(900.0)
        assert result["total_stock"] == 25


def test_gold_to_postgres_reload(postgres_integration_environment):
    """Vérifie que deux chargements ne créent pas de doublons."""

    loader = postgres_integration_environment["loader"]
    engine = postgres_integration_environment["engine"]

    # Premier chargement
    loader.run()

    # Deuxième chargement
    loader.run()

    # Vérification PostgreSQL
    with engine.connect() as conn:
        total = conn.execute(
            text("""
                SELECT COUNT(*)
                FROM public.products_by_category_integration_test
            """)
        ).scalar_one()

        unique_categories = conn.execute(
            text("""
                SELECT COUNT(DISTINCT category)
                FROM public.products_by_category_integration_test
            """)
        ).scalar_one()

    assert total == 3
    assert unique_categories == 3
