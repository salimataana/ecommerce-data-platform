import pytest

from src.transformation.silver_to_gold import SilverToGoldTransformer

# ============================================================
# TEST 1 : Agrégation des produits par catégorie
# ============================================================


@pytest.mark.parametrize(
    "products, expected",
    [
        pytest.param(
            [
                ("smartphones", 100.0, 10),
                ("smartphones", 200.0, 20),
            ],
            [
                ("smartphones", 2, 150.0, 30),
            ],
            id="plusieurs_produits_meme_categorie",
        ),
        pytest.param(
            [
                ("smartphones", 100.0, 10),
                ("laptops", 500.0, 5),
            ],
            [
                ("laptops", 1, 500.0, 5),
                ("smartphones", 1, 100.0, 10),
            ],
            id="plusieurs_categories",
        ),
        pytest.param(
            [
                ("beauty", 10.0, 0),
            ],
            [
                ("beauty", 1, 10.0, 0),
            ],
            id="stock_zero",
        ),
        pytest.param(
            [
                ("furniture", 10.123, 2),
                ("furniture", 20.456, 3),
            ],
            [
                ("furniture", 2, 15.29, 5),
            ],
            id="arrondi_prix_moyen",
        ),
    ],
)
def test_transform_products(spark, tmp_path, products, expected):

    df_silver = spark.createDataFrame(
        products,
        schema="category STRING, price DOUBLE, stock INT",
    )

    transformer = SilverToGoldTransformer(
        silver_dir=tmp_path / "silver",
        gold_dir=tmp_path / "gold",
        spark=spark,
    )

    result = transformer.transform(df_silver)

    actual = [
        (
            row["category"],
            row["total_products"],
            row["average_price"],
            row["total_stock"],
        )
        for row in result.collect()
    ]

    assert actual == expected


# ============================================================
# TEST 2 : Gestion des prix nuls
# ============================================================


@pytest.mark.parametrize(
    "products, expected_average",
    [
        pytest.param(
            [("beauty", None, 2), ("beauty", 20.0, 3)],
            20.0,
            id="un_prix_null",
        ),
        pytest.param(
            [("beauty", None, 2), ("beauty", None, 3)],
            None,
            id="tous_les_prix_nuls",
        ),
    ],
)
def test_null_prices(spark, tmp_path, products, expected_average):

    df_silver = spark.createDataFrame(
        products,
        schema="category STRING, price DOUBLE, stock INT",
    )

    transformer = SilverToGoldTransformer(
        silver_dir=tmp_path / "silver",
        gold_dir=tmp_path / "gold",
        spark=spark,
    )

    result = transformer.transform(df_silver).first()

    assert result["total_products"] == len(products)
    assert result["average_price"] == expected_average
    assert result["total_stock"] == sum(product[2] for product in products)


# ============================================================
# TEST 3 : Lecture des fichiers Silver
# ============================================================


def test_read_silver(spark, tmp_path):

    silver_dir = tmp_path / "silver"

    df = spark.createDataFrame(
        [("smartphones", 100.0, 10)],
        schema="category STRING, price DOUBLE, stock INT",
    )

    df.write.mode("overwrite").parquet(str(silver_dir))

    transformer = SilverToGoldTransformer(
        silver_dir=silver_dir,
        gold_dir=tmp_path / "gold",
        spark=spark,
    )

    result = transformer.read()

    assert result.count() == 1
    assert result.first()["category"] == "smartphones"


# ============================================================
# TEST 4 : Écriture des fichiers Gold
# ============================================================


@pytest.mark.parametrize(
    "products",
    [
        pytest.param(
            [("beauty", 1, 10.0, 5)],
            id="une_categorie",
        ),
        pytest.param(
            [
                ("beauty", 1, 10.0, 5),
                ("smartphones", 2, 150.0, 30),
            ],
            id="plusieurs_categories",
        ),
    ],
)
def test_write_gold(spark, tmp_path, products):

    gold_dir = tmp_path / "gold"

    df = spark.createDataFrame(
        products,
        schema="""
            category STRING,
            total_products BIGINT,
            average_price DOUBLE,
            total_stock BIGINT
        """,
    )

    transformer = SilverToGoldTransformer(
        silver_dir=tmp_path / "silver",
        gold_dir=gold_dir,
        spark=spark,
    )

    transformer.write(df)

    saved_df = spark.read.parquet(str(gold_dir))

    assert saved_df.count() == len(products)

    assert sorted(row["category"] for row in saved_df.collect()) == sorted(
        product[0] for product in products
    )


# ============================================================
# TEST 5 : Gestion de la session Spark
# ============================================================


def test_spark_session_injected(spark, tmp_path):

    transformer = SilverToGoldTransformer(
        silver_dir=tmp_path / "silver",
        gold_dir=tmp_path / "gold",
        spark=spark,
    )

    assert transformer.spark is spark
    assert transformer._owns_spark is False
