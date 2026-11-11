import json
import os
from unittest.mock import patch

import pytest
from pyspark.sql import Row

from src.transformation.bronze_to_silver import (
    BronzeToSilverTransformer,
)


# ============================================================
# TEST 1 : Sélection du dernier fichier Bronze
# ============================================================

@pytest.mark.parametrize(
    "timestamps, expected_index",
    [
        pytest.param([100, 200, 300], 2, id="trois_fichiers"),
        pytest.param([300, 100, 200], 0, id="ordre_desordonne"),
        pytest.param([100], 0, id="un_seul_fichier"),
    ],
)
def test_get_latest_bronze_file(
    tmp_path,
    spark,
    timestamps,
    expected_index,
):
    files = []

    for index, timestamp in enumerate(timestamps):
        file_path = tmp_path / f"products_{index}.json"
        file_path.write_text('{"products": []}', encoding="utf-8")

        os.utime(file_path, (timestamp, timestamp))
        files.append(file_path)

    transformer = BronzeToSilverTransformer(
        bronze_dir=tmp_path,
        silver_dir=tmp_path / "silver",
        spark=spark,
    )

    assert transformer.get_latest_bronze_file() == files[expected_index]


# ============================================================
# TEST 2 : Aucun fichier Bronze
# ============================================================

def test_get_latest_bronze_file_not_found(tmp_path, spark):

    transformer = BronzeToSilverTransformer(
        bronze_dir=tmp_path,
        silver_dir=tmp_path / "silver",
        spark=spark,
    )

    with pytest.raises(FileNotFoundError, match="Aucun fichier Bronze"):
        transformer.get_latest_bronze_file()


# ============================================================
# TEST 3 : Transformation des produits
# ============================================================

@pytest.mark.parametrize(
    "title, category, brand, sku, expected",
    [
        pytest.param(
            "  iPhone  ",
            "  SMARTPHONES  ",
            "  Apple  ",
            "  SKU001  ",
            ("iPhone", "smartphones", "Apple", "SKU001"),
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
            "BEAUTY",
            None,
            "SKU003",
            (None, "beauty", None, "SKU003"),
            id="valeurs_nulles",
        ),
    ],
)
def test_transform_products(
    spark,
    tmp_path,
    title,
    category,
    brand,
    sku,
    expected,
):

    schema = """
        products ARRAY<STRUCT<
            id: BIGINT,
            title: STRING,
            category: STRING,
            brand: STRING,
            sku: STRING,
            dimensions: STRUCT<
                width: DOUBLE,
                height: DOUBLE,
                depth: DOUBLE
            >,
            meta: STRUCT<
                barcode: STRING,
                createdAt: STRING,
                updatedAt: STRING,
                qrCode: STRING
            >
        >>
    """

    df = spark.createDataFrame(
        [
            (
                [
                    {
                        "id": 1,
                        "title": title,
                        "category": category,
                        "brand": brand,
                        "sku": sku,
                        "dimensions": {
                            "width": 10.0,
                            "height": 20.0,
                            "depth": 5.0,
                        },
                        "meta": {
                            "barcode": "123456",
                            "createdAt": "2026-11-10T10:00:00",
                            "updatedAt": "2026-11-10T12:00:00",
                            "qrCode": "QR001",
                        },
                    }
                ],
            )
        ],
        schema=schema,
    )

    transformer = BronzeToSilverTransformer(
        bronze_dir=tmp_path,
        silver_dir=tmp_path / "silver",
        spark=spark,
    )

    result = transformer.transform(df).first()

    assert (
        result["title"],
        result["category"],
        result["brand"],
        result["sku"],
    ) == expected

    assert result["dimensions_width"] == 10.0
    assert result["meta_barcode"] == "123456"
    assert result["meta_created_at"] is not None


# ============================================================
# TEST 4 : Validation des données
# ============================================================

@pytest.mark.parametrize(
    "is_valid",
    [True, False],
)
def test_validate_products(spark, tmp_path, is_valid):

    transformer = BronzeToSilverTransformer(
        bronze_dir=tmp_path,
        silver_dir=tmp_path / "silver",
        spark=spark,
    )

    df = spark.createDataFrame(
        [(1, "iPhone")],
        ["id", "title"],
    )

    with patch(
        "src.transformation.bronze_to_silver.validate_products",
        return_value=is_valid,
    ):
        if is_valid:
            transformer.validate(df)
        else:
            with pytest.raises(ValueError, match="Échec de la validation"):
                transformer.validate(df)


# ============================================================
# TEST 5 : Écriture Parquet
# ============================================================

@pytest.mark.parametrize(
    "products",
    [
        pytest.param(
            [(1, "iPhone")],
            id="un_produit",
        ),
        pytest.param(
            [(1, "iPhone"), (2, "Samsung")],
            id="plusieurs_produits",
        ),
    ],
)
def test_write_silver(spark, tmp_path, products):

    output_dir = tmp_path / "silver"

    transformer = BronzeToSilverTransformer(
        bronze_dir=tmp_path,
        silver_dir=output_dir,
        spark=spark,
    )

    df = spark.createDataFrame(products, ["id", "title"])

    transformer.write(df)

    saved_df = spark.read.parquet(str(output_dir))

    assert saved_df.count() == len(products)
    assert sorted(row["id"] for row in saved_df.collect()) == sorted(
        product[0] for product in products
    )


# ============================================================
# TEST 6 : Gestion de la session Spark
# ============================================================

def test_spark_session_injected(spark, tmp_path):

    transformer = BronzeToSilverTransformer(
        bronze_dir=tmp_path,
        silver_dir=tmp_path / "silver",
        spark=spark,
    )

    assert transformer.spark is spark
    assert transformer._owns_spark is False