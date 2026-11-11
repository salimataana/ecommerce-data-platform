from datetime import datetime

from pyspark.sql import Row
from pyspark.sql.types import TimestampType

from src.transformation.product_transformations import (
    flatten_products,
    convert_dates,
    normalize_products,
)
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# TEST 1 : Aplatir les champs imbriqués
def test_flatten_products(spark):

    data = [
        Row(
            id=1,
            title="iPhone",
            dimensions=Row(
                width=10.5,
                height=20.0,
                depth=5.0
            ),
            meta=Row(
                barcode="123456",
                createdAt="2026-01-10T10:30:00Z",
                updatedAt="2026-01-11T12:00:00Z",
                qrCode="https://example.com/qr"
            )
        )
    ]

    df = spark.createDataFrame(data)

    result = flatten_products(df)

    product = result.first()

    assert product["dimensions_width"] == 10.5
    assert product["dimensions_height"] == 20.0
    assert product["dimensions_depth"] == 5.0

    assert product["meta_barcode"] == "123456"
    assert product["meta_qr_code"] == "https://example.com/qr"

    assert "dimensions" not in result.columns
    assert "meta" not in result.columns


# TEST 2 : Convertir les dates
def test_convert_dates(spark):

    data = [
        (
            1,
            "2026-01-10T10:30:00Z",
            "2026-01-11T12:00:00Z"
        )
    ]

    columns = [
        "id",
        "meta_created_at",
        "meta_updated_at"
    ]

    df = spark.createDataFrame(data, columns)

    result = convert_dates(df)

    product = result.first()

    assert isinstance(product["meta_created_at"], datetime)
    assert isinstance(product["meta_updated_at"], datetime)

    assert isinstance(
        result.schema["meta_created_at"].dataType,
        TimestampType
    )

    assert isinstance(
        result.schema["meta_updated_at"].dataType,
        TimestampType
    )


# TEST 3 : Normaliser les chaînes de caractères
def test_normalize_products(spark):

    data = [
        (
            1,
            "  iPhone 15  ",
            "  SMARTPHONES  ",
            "  Apple  ",
            "  SKU001  "
        )
    ]

    columns = [
        "id",
        "title",
        "category",
        "brand",
        "sku"
    ]

    df = spark.createDataFrame(data, columns)

    result = normalize_products(df)

    product = result.first()

    assert product["title"] == "iPhone 15"
    assert product["category"] == "smartphones"
    assert product["brand"] == "Apple"
    assert product["sku"] == "SKU001"


# TEST 4 : Gérer les valeurs nulles
def test_normalize_products_null_values(spark):

    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("title", StringType(), True),
        StructField("category", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("sku", StringType(), True),
    ])

    data = [
        (1, None, "  BEAUTY  ", None, "  SKU002  ")
    ]

    df = spark.createDataFrame(data, schema=schema)

    result = normalize_products(df)

    product = result.first()

    assert product["title"] is None
    assert product["brand"] is None
    assert product["category"] == "beauty"
    assert product["sku"] == "SKU002"


# TEST 5 : Conserver le nombre de produits
def test_normalize_products_row_count(spark):

    data = [
        (1, "  iPhone  ", "  PHONES  ", " Apple ", " A001 "),
        (2, "  Samsung ", "  PHONES  ", " Samsung ", " S001 "),
        (3, "  Laptop  ", " COMPUTERS ", " Dell ", " D001 ")
    ]

    columns = [
        "id",
        "title",
        "category",
        "brand",
        "sku"
    ]

    df = spark.createDataFrame(data, columns)

    result = normalize_products(df)

    assert result.count() == 3

    assert result.select("id").distinct().count() == 3