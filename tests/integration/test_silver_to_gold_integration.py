import pytest
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from src.transformation.silver_to_gold import (
    SilverToGoldTransformer,
)

# ============================================================
# FIXTURE : ENVIRONNEMENT SILVER → GOLD
# ============================================================


@pytest.fixture
def silver_gold_environment(tmp_path, spark):
    """
    Prépare un environnement temporaire.

    - Crée un dossier Silver.
    - Crée un dossier Gold.
    - Génère des produits Silver.
    - Enregistre les produits en Parquet.
    - Initialise SilverToGoldTransformer.
    """

    silver_dir = tmp_path / "silver" / "products"
    gold_dir = tmp_path / "gold" / "products"

    schema = StructType(
        [
            StructField("id", IntegerType(), False),
            StructField("title", StringType(), False),
            StructField("category", StringType(), False),
            StructField("price", DoubleType(), False),
            StructField("stock", IntegerType(), False),
        ]
    )

    products = [
        (1, "iPhone 15", "smartphones", 1000.0, 10),
        (2, "Samsung Galaxy", "smartphones", 800.0, 15),
        (3, "MacBook Pro", "laptops", 2000.0, 5),
        (4, "Dell XPS", "laptops", 1500.0, 8),
        (5, "Dior Sauvage", "fragrances", 100.0, 20),
    ]

    df_silver = spark.createDataFrame(
        products,
        schema=schema,
    )

    # Enregistrement réel des données Silver.
    df_silver.write.mode("overwrite").parquet(str(silver_dir))

    transformer = SilverToGoldTransformer(
        silver_dir=silver_dir,
        gold_dir=gold_dir,
        spark=spark,
    )

    return {
        "transformer": transformer,
        "silver_dir": silver_dir,
        "gold_dir": gold_dir,
        "expected_categories": 3,
        "expected_products": 5,
    }


# ============================================================
# TEST 1 : PIPELINE COMPLET SILVER → GOLD
# ============================================================


def test_silver_to_gold_pipeline(
    silver_gold_environment,
    spark,
):
    """
    Vérifie :
    1. Lecture réelle du Parquet Silver.
    2. Agrégation des produits.
    3. Validation Great Expectations.
    4. Écriture réelle du Parquet Gold.
    5. Relecture des résultats.
    """

    environment = silver_gold_environment

    transformer = environment["transformer"]
    gold_dir = environment["gold_dir"]

    # Exécuter le pipeline.
    transformer.run()

    # Vérifier que le dossier Gold existe.
    assert gold_dir.exists()
    assert gold_dir.is_dir()

    # Vérifier la présence de fichiers Parquet.
    parquet_files = list(gold_dir.glob("*.parquet"))

    assert len(parquet_files) > 0

    # Relire les données Gold.
    df_gold = spark.read.parquet(str(gold_dir))

    # Vérifier le nombre de catégories.
    assert df_gold.count() == 3

    # Vérifier le nombre total de produits.
    total_products = sum(row["total_products"] for row in df_gold.collect())

    assert total_products == 5

    # Vérifier les colonnes attendues.
    expected_columns = {
        "category",
        "total_products",
        "average_price",
        "total_stock",
    }

    assert expected_columns == set(df_gold.columns)


# ============================================================
# TEST 2 : VÉRIFIER LES AGRÉGATIONS MÉTIER
# ============================================================


@pytest.mark.parametrize(
    "category, expected_count, expected_price, expected_stock",
    [
        pytest.param(
            "smartphones",
            2,
            900.0,
            25,
            id="smartphones",
        ),
        pytest.param(
            "laptops",
            2,
            1750.0,
            13,
            id="laptops",
        ),
        pytest.param(
            "fragrances",
            1,
            100.0,
            20,
            id="fragrances",
        ),
    ],
)
def test_silver_to_gold_aggregations(
    silver_gold_environment,
    spark,
    category,
    expected_count,
    expected_price,
    expected_stock,
):
    """
    Vérifie les agrégations par catégorie :
    - Nombre de produits.
    - Prix moyen.
    - Stock total.
    """

    transformer = silver_gold_environment["transformer"]
    gold_dir = silver_gold_environment["gold_dir"]

    # Exécuter le pipeline complet.
    transformer.run()

    # Relire les données Gold.
    df_gold = spark.read.parquet(str(gold_dir))

    result = df_gold.filter(df_gold["category"] == category).first()

    assert result is not None

    assert result["total_products"] == expected_count

    assert result["average_price"] == pytest.approx(expected_price)

    assert result["total_stock"] == expected_stock


# ============================================================
# TEST 3 : VÉRIFIER L'UNICITÉ DES CATÉGORIES
# ============================================================


def test_silver_to_gold_unique_categories(
    silver_gold_environment,
    spark,
):
    """
    Vérifie qu'une catégorie apparaît une seule fois
    dans les données Gold.
    """

    transformer = silver_gold_environment["transformer"]
    gold_dir = silver_gold_environment["gold_dir"]

    transformer.run()

    df_gold = spark.read.parquet(str(gold_dir))

    assert df_gold.count() == (df_gold.select("category").distinct().count())


# ============================================================
# TEST 4 : VÉRIFIER LES VALEURS DES INDICATEURS
# ============================================================


def test_silver_to_gold_metrics(
    silver_gold_environment,
    spark,
):
    """
    Vérifie que les indicateurs Gold sont cohérents.
    """

    transformer = silver_gold_environment["transformer"]
    gold_dir = silver_gold_environment["gold_dir"]

    transformer.run()

    df_gold = spark.read.parquet(str(gold_dir))

    results = df_gold.collect()

    for row in results:
        assert row["total_products"] >= 1
        assert row["average_price"] >= 0
        assert row["total_stock"] >= 0


# ============================================================
# TEST 5 : VÉRIFIER LE SCHÉMA GOLD
# ============================================================


def test_silver_to_gold_schema(
    silver_gold_environment,
    spark,
):
    """
    Vérifie le schéma final du Parquet Gold.
    """

    transformer = silver_gold_environment["transformer"]
    gold_dir = silver_gold_environment["gold_dir"]

    transformer.run()

    df_gold = spark.read.parquet(str(gold_dir))

    assert isinstance(
        df_gold.schema["category"].dataType,
        StringType,
    )

    assert (
        isinstance(
            df_gold.schema["total_products"].dataType,
            IntegerType,
        )
        is False
    )

    assert isinstance(
        df_gold.schema["average_price"].dataType,
        DoubleType,
    )
