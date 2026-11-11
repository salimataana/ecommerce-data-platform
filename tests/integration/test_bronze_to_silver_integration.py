import json
from datetime import datetime

import pytest
from pyspark.sql.types import TimestampType

from src.transformation.bronze_to_silver import (
    BronzeToSilverTransformer,
)


@pytest.fixture
def bronze_silver_environment(tmp_path, spark):
    """
    Prépare un environnement temporaire pour le pipeline.

    - Crée un dossier Bronze.
    - Crée un dossier Silver.
    - Génère un fichier JSON représentatif de DummyJSON.
    - Injecte la session Spark utilisée par les tests.
    """

    bronze_dir = tmp_path / "bronze" / "products"
    silver_dir = tmp_path / "silver" / "products"

    bronze_dir.mkdir(parents=True)

    products = {
        "products": [
            {
                "id": 1,
                "title": "  iPhone 15  ",
                "category": "  SMARTPHONES  ",
                "brand": "  Apple  ",
                "sku": "  SKU001  ",
                "price": 999.99,
                "stock": 10,
                "rating": 4.5,
                "dimensions": {
                    "width": 10.5,
                    "height": 20.0,
                    "depth": 5.0,
                },
                "meta": {
                    "barcode": "123456",
                    "createdAt": "2026-01-10T10:30:00Z",
                    "updatedAt": "2026-01-11T12:00:00Z",
                    "qrCode": "https://example.com/qr1",
                },
            },
            {
                "id": 2,
                "title": "  Samsung Galaxy  ",
                "category": "  SMARTPHONES  ",
                "brand": "  Samsung  ",
                "sku": "  SKU002  ",
                "price": 799.99,
                "stock": 15,
                "rating": 4.3,
                "dimensions": {
                    "width": 11.0,
                    "height": 21.0,
                    "depth": 6.0,
                },
                "meta": {
                    "barcode": "789012",
                    "createdAt": "2026-01-12T08:00:00Z",
                    "updatedAt": "2026-01-13T09:00:00Z",
                    "qrCode": "https://example.com/qr2",
                },
            },
        ],
        "total": 2,
        "skip": 0,
        "limit": 2,
    }

    bronze_file = bronze_dir / "products_test.json"

    bronze_file.write_text(
        json.dumps(products, indent=2),
        encoding="utf-8",
    )

    transformer = BronzeToSilverTransformer(
        bronze_dir=bronze_dir,
        silver_dir=silver_dir,
        spark=spark,
    )

    return {
        "transformer": transformer,
        "bronze_dir": bronze_dir,
        "silver_dir": silver_dir,
        "bronze_file": bronze_file,
    }


def test_bronze_to_silver_pipeline(
    bronze_silver_environment,
    spark,
):
    """
    Test d'intégration complet Bronze → Silver.

    Vérifie :
    1. Lecture du JSON Bronze.
    2. Transformation des produits.
    3. Validation Great Expectations.
    4. Écriture du Parquet Silver.
    5. Relecture et vérification des données.
    """

    environment = bronze_silver_environment

    transformer = environment["transformer"]
    silver_dir = environment["silver_dir"]

    # 1. Exécuter le véritable pipeline.
    transformer.run()

    # 2. Vérifier que le dossier Silver a été créé.
    assert silver_dir.exists()
    assert silver_dir.is_dir()

    # 3. Vérifier qu'au moins un fichier Parquet existe.
    parquet_files = list(silver_dir.glob("*.parquet"))

    assert len(parquet_files) > 0

    # 4. Relire les données Silver avec Spark.
    df_silver = spark.read.parquet(str(silver_dir))

    # 5. Vérifier le nombre de produits.
    assert df_silver.count() == 2

    # 6. Vérifier l'unicité des identifiants.
    assert df_silver.select("id").distinct().count() == 2

    # 7. Vérifier les colonnes attendues.
    expected_columns = {
        "id",
        "title",
        "category",
        "brand",
        "sku",
        "price",
        "stock",
        "rating",
        "dimensions_width",
        "dimensions_height",
        "dimensions_depth",
        "meta_barcode",
        "meta_created_at",
        "meta_updated_at",
        "meta_qr_code",
    }

    assert expected_columns.issubset(set(df_silver.columns))

    # 8. Vérifier que les structures ont été aplaties.
    assert "dimensions" not in df_silver.columns
    assert "meta" not in df_silver.columns

    # 9. Vérifier les types des dates.
    assert isinstance(
        df_silver.schema["meta_created_at"].dataType,
        TimestampType,
    )

    assert isinstance(
        df_silver.schema["meta_updated_at"].dataType,
        TimestampType,
    )

    # 10. Vérifier les données métier.
    products = {row["id"]: row.asDict() for row in df_silver.collect()}

    iphone = products[1]
    samsung = products[2]

    assert iphone["title"] == "iPhone 15"
    assert iphone["category"] == "smartphones"
    assert iphone["brand"] == "Apple"
    assert iphone["sku"] == "SKU001"

    assert iphone["price"] == pytest.approx(999.99)
    assert iphone["stock"] == 10

    assert iphone["dimensions_width"] == pytest.approx(10.5)
    assert iphone["meta_barcode"] == "123456"

    assert isinstance(iphone["meta_created_at"], datetime)

    assert samsung["title"] == "Samsung Galaxy"
    assert samsung["category"] == "smartphones"
    assert samsung["brand"] == "Samsung"
    assert samsung["sku"] == "SKU002"

    assert samsung["price"] == pytest.approx(799.99)
    assert samsung["stock"] == 15
