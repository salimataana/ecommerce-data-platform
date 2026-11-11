from datetime import datetime

import pytest
from pyspark.sql import Row
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.transformation.product_transformations import (
    convert_dates,
    flatten_products,
    normalize_products,
)

# ============================================================
# TEST 1 : APLATISSEMENT DES CHAMPS IMBRIQUÉS
# ============================================================


@pytest.mark.parametrize(
    "width, height, depth, barcode, qr_code",
    [
        pytest.param(
            10.5,
            20.0,
            5.0,
            "123456",
            "https://example.com/qr",
            id="dimensions_standard",
        ),
        pytest.param(
            0.0,
            0.0,
            0.0,
            "000000",
            "QR000",
            id="dimensions_zero",
        ),
        pytest.param(
            100.5,
            200.0,
            50.5,
            "987654",
            "QR999",
            id="grandes_dimensions",
        ),
    ],
)
def test_flatten_products(
    spark,
    width,
    height,
    depth,
    barcode,
    qr_code,
):
    data = [
        Row(
            id=1,
            title="iPhone",
            dimensions=Row(
                width=width,
                height=height,
                depth=depth,
            ),
            meta=Row(
                barcode=barcode,
                createdAt="2026-01-10T10:30:00Z",
                updatedAt="2026-01-11T12:00:00Z",
                qrCode=qr_code,
            ),
        )
    ]

    df = spark.createDataFrame(data)

    result = flatten_products(df)

    product = result.first()

    assert product["dimensions_width"] == width
    assert product["dimensions_height"] == height
    assert product["dimensions_depth"] == depth

    assert product["meta_barcode"] == barcode
    assert product["meta_qr_code"] == qr_code

    assert "dimensions" not in result.columns
    assert "meta" not in result.columns

    assert "dimensions_width" in result.columns
    assert "meta_created_at" in result.columns


# ============================================================
# TEST 2 : CONVERSION DES DATES
# ============================================================


@pytest.mark.parametrize(
    "created_at, updated_at",
    [
        pytest.param(
            "2026-01-10T10:30:00Z",
            "2026-01-11T12:00:00Z",
            id="dates_iso_utc",
        ),
        pytest.param(
            "2026-02-15 08:00:00",
            "2026-02-16 09:30:00",
            id="dates_standard",
        ),
        pytest.param(
            None,
            None,
            id="dates_nulles",
        ),
    ],
)
def test_convert_dates(
    spark,
    created_at,
    updated_at,
):
    schema = """
        id INT,
        meta_created_at STRING,
        meta_updated_at STRING
    """

    df = spark.createDataFrame(
        [(1, created_at, updated_at)],
        schema=schema,
    )

    result = convert_dates(df)

    product = result.first()

    assert isinstance(
        result.schema["meta_created_at"].dataType,
        TimestampType,
    )

    assert isinstance(
        result.schema["meta_updated_at"].dataType,
        TimestampType,
    )

    if created_at is None:
        assert product["meta_created_at"] is None
    else:
        assert isinstance(
            product["meta_created_at"],
            datetime,
        )

    if updated_at is None:
        assert product["meta_updated_at"] is None
    else:
        assert isinstance(
            product["meta_updated_at"],
            datetime,
        )


# ============================================================
# TEST 3 : NORMALISATION DES CHAÎNES DE CARACTÈRES
# ============================================================


@pytest.mark.parametrize(
    "title, category, brand, sku, expected",
    [
        pytest.param(
            "  iPhone 15  ",
            "  SMARTPHONES  ",
            "  Apple  ",
            "  SKU001  ",
            ("iPhone 15", "smartphones", "Apple", "SKU001"),
            id="espaces_et_majuscules",
        ),
        pytest.param(
            "Samsung",
            "LAPTOPS",
            "Samsung",
            "SKU002",
            ("Samsung", "laptops", "Samsung", "SKU002"),
            id="categorie_majuscules",
        ),
        pytest.param(
            None,
            "  BEAUTY  ",
            None,
            "  SKU003  ",
            (None, "beauty", None, "SKU003"),
            id="valeurs_nulles",
        ),
        pytest.param(
            "  Laptop  ",
            "  COMPUTERS  ",
            "  Dell  ",
            "  D001  ",
            ("Laptop", "computers", "Dell", "D001"),
            id="normalisation_complete",
        ),
        pytest.param(
            "",
            "",
            "",
            "",
            ("", "", "", ""),
            id="chaines_vides",
        ),
        pytest.param(
            "  Parfum  ",
            "  FRAGRANCES  ",
            "  Dior  ",
            "  P001  ",
            ("Parfum", "fragrances", "Dior", "P001"),
            id="categorie_fragrances",
        ),
    ],
)
def test_normalize_products(
    spark,
    title,
    category,
    brand,
    sku,
    expected,
):
    schema = StructType(
        [
            StructField("id", IntegerType(), False),
            StructField("title", StringType(), True),
            StructField("category", StringType(), True),
            StructField("brand", StringType(), True),
            StructField("sku", StringType(), True),
        ]
    )

    df = spark.createDataFrame(
        [(1, title, category, brand, sku)],
        schema=schema,
    )

    result = normalize_products(df)

    product = result.first()

    actual = (
        product["title"],
        product["category"],
        product["brand"],
        product["sku"],
    )

    assert actual == expected


# ============================================================
# TEST 4 : CONSERVATION DU NOMBRE DE PRODUITS
# ============================================================


@pytest.mark.parametrize(
    "products",
    [
        pytest.param(
            [
                (1, "  iPhone  ", "  PHONES  ", " Apple ", " A001 "),
            ],
            id="un_produit",
        ),
        pytest.param(
            [
                (1, "  iPhone  ", "  PHONES  ", " Apple ", " A001 "),
                (2, "  Samsung ", "  PHONES  ", " Samsung ", " S001 "),
            ],
            id="deux_produits",
        ),
        pytest.param(
            [
                (1, "  iPhone  ", "  PHONES  ", " Apple ", " A001 "),
                (2, "  Samsung ", "  PHONES  ", " Samsung ", " S001 "),
                (3, "  Laptop  ", " COMPUTERS ", " Dell ", " D001 "),
            ],
            id="trois_produits",
        ),
    ],
)
def test_normalize_products_row_count(
    spark,
    products,
):
    schema = """
        id INT,
        title STRING,
        category STRING,
        brand STRING,
        sku STRING
    """

    df = spark.createDataFrame(
        products,
        schema=schema,
    )

    result = normalize_products(df)

    assert result.count() == len(products)

    assert result.select("id").distinct().count() == len(products)


# ============================================================
# TEST 5 : VÉRIFIER LES COLONNES APRÈS NORMALISATION
# ============================================================


def test_normalize_products_preserves_columns(spark):
    schema = """
        id INT,
        title STRING,
        category STRING,
        brand STRING,
        sku STRING,
        price DOUBLE,
        stock INT
    """

    df = spark.createDataFrame(
        [
            (
                1,
                "  iPhone  ",
                "  SMARTPHONES  ",
                "  Apple  ",
                "  SKU001  ",
                100.0,
                10,
            )
        ],
        schema=schema,
    )

    result = normalize_products(df)

    assert result.columns == df.columns

    product = result.first()

    assert product["price"] == 100.0
    assert product["stock"] == 10
    assert product["title"] == "iPhone"
    assert product["category"] == "smartphones"
