from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.loading.gold_to_postgres import PostgresLoader


@pytest.fixture
def loader(spark, tmp_path):
    """Crée un loader avec une connexion PostgreSQL simulée."""

    engine = MagicMock()

    return PostgresLoader(
        gold_dir=tmp_path / "gold",
        spark=spark,
        engine=engine,
    )


# ============================================================
# TEST 1 : Vérifier l'injection des dépendances
# ============================================================


def test_dependencies_injected(loader, spark):
    assert loader.spark is spark
    assert loader._owns_spark is False
    assert loader._owns_engine is False


# ============================================================
# TEST 2 : Lire les données Gold
# ============================================================


def test_read_gold(loader, spark):
    df = spark.createDataFrame(
        [("smartphones", 2, 150.0, 30)],
        schema="""
            category STRING,
            total_products BIGINT,
            average_price DOUBLE,
            total_stock BIGINT
        """,
    )

    df.write.mode("overwrite").parquet(str(loader.gold_dir))

    result = loader.read()

    assert result.count() == 1
    assert result.first()["category"] == "smartphones"


# ============================================================
# TEST 3 : Conversion Spark vers Pandas
# ============================================================


@pytest.mark.parametrize(
    "products",
    [
        pytest.param(
            [("smartphones", 2, 150.0, 30)],
            id="une_categorie",
        ),
        pytest.param(
            [
                ("smartphones", 2, 150.0, 30),
                ("laptops", 1, 500.0, 5),
            ],
            id="plusieurs_categories",
        ),
    ],
)
def test_to_pandas(loader, spark, products):
    df = spark.createDataFrame(
        products,
        schema="""
            category STRING,
            total_products BIGINT,
            average_price DOUBLE,
            total_stock BIGINT
        """,
    )

    result = loader.to_pandas(df)

    assert isinstance(result, pd.DataFrame)
    assert len(result) == len(products)


# ============================================================
# TEST 4 : Vérification de l'existence de la table
# ============================================================


@pytest.mark.parametrize(
    "exists",
    [True, False],
)
def test_table_exists(loader, exists):
    conn = MagicMock()
    conn.execute.return_value.scalar.return_value = exists

    result = loader.table_exists(conn)

    assert result is exists

    conn.execute.assert_called_once()


# ============================================================
# TEST 5 : Chargement PostgreSQL
# ============================================================


@pytest.mark.parametrize(
    "table_exists",
    [True, False],
)
def test_load_gold(loader, table_exists):
    df = pd.DataFrame(
        {
            "category": ["smartphones"],
            "total_products": [2],
            "average_price": [150.0],
            "total_stock": [30],
        }
    )

    conn = MagicMock()
    loader.engine.begin.return_value.__enter__.return_value = conn

    with (
        patch.object(
            loader,
            "table_exists",
            return_value=table_exists,
        ),
        patch.object(df, "to_sql") as mock_to_sql,
    ):
        loader.load(df)

    mock_to_sql.assert_called_once_with(
        name="products_by_category",
        con=conn,
        schema="public",
        if_exists="append",
        index=False,
    )

    if table_exists:
        assert conn.execute.call_count == 1
        assert str(conn.execute.call_args.args[0]) == (
            "DELETE FROM public.products_by_category"
        )
    else:
        conn.execute.assert_not_called()


# ============================================================
# TEST 6 : Rejeter une table non autorisée
# ============================================================


@pytest.mark.parametrize(
    "schema_name, table_name",
    [
        ("private", "products_by_category"),
        ("public", "another_table"),
    ],
)
def test_unauthorized_table(spark, tmp_path, schema_name, table_name):
    engine = MagicMock()

    loader = PostgresLoader(
        gold_dir=tmp_path / "gold",
        schema_name=schema_name,
        table_name=table_name,
        spark=spark,
        engine=engine,
    )

    df = pd.DataFrame(
        {
            "category": ["smartphones"],
            "total_products": [1],
            "average_price": [100.0],
            "total_stock": [10],
        }
    )

    conn = MagicMock()
    engine.begin.return_value.__enter__.return_value = conn

    with (
        patch.object(loader, "table_exists", return_value=True),
        pytest.raises(ValueError, match="Table PostgreSQL non autorisée"),
    ):
        loader.load(df)


# ============================================================
# TEST 7 : Exécution complète simulée
# ============================================================


def test_run_pipeline(loader, spark):
    df = spark.createDataFrame(
        [("smartphones", 2, 150.0, 30)],
        schema="""
            category STRING,
            total_products BIGINT,
            average_price DOUBLE,
            total_stock BIGINT
        """,
    )

    with (
        patch.object(loader, "read", return_value=df),
        patch.object(loader, "load") as mock_load,
    ):
        loader.run()

    mock_load.assert_called_once()

    loaded_df = mock_load.call_args.args[0]

    assert len(loaded_df) == 1
    assert loaded_df.iloc[0]["category"] == "smartphones"

    # La session Spark injectée doit rester active.
    assert loader._owns_spark is False
