import json

from sqlalchemy import text

from src.loading.gold_to_postgres import PostgresLoader
from src.transformation.bronze_to_silver import BronzeToSilverTransformer
from src.transformation.silver_to_gold import SilverToGoldTransformer
from src.utils.db_connection import get_engine


def test_pipeline_e2e(tmp_path, spark):
    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    gold = tmp_path / "gold"

    bronze.mkdir()

    products = {
        "products": [
            {
                "id": 1,
                "title": "iPhone 15",
                "category": "smartphones",
                "brand": "Apple",
                "sku": "SKU001",
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
                    "qrCode": "qr1",
                },
            },
            {
                "id": 2,
                "title": "Samsung Galaxy",
                "category": "smartphones",
                "brand": "Samsung",
                "sku": "SKU002",
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
                    "qrCode": "qr2",
                },
            },
        ]
    }

    (bronze / "products_test.json").write_text(json.dumps(products))

    engine = get_engine()

    try:
        # Bronze → Silver
        BronzeToSilverTransformer(bronze, silver, spark).run()

        # Silver → Gold
        SilverToGoldTransformer(silver, gold, spark).run()

        # Gold → PostgreSQL
        loader = PostgresLoader(
            gold_dir=gold,
            table_name="products_by_category_integration_test",
            spark=spark,
            engine=engine,
            allow_test_table=True,
        )

        loader.run()

        # Vérification finale
        with engine.connect() as conn:
            result = conn.execute(
                text("""
                    SELECT total_products, total_stock
                    FROM public.products_by_category_integration_test
                    WHERE category = 'smartphones'
                """)
            ).one()

        assert result.total_products == 2
        assert result.total_stock == 25

    finally:
        with engine.begin() as conn:
            conn.execute(
                text(
                    "DROP TABLE IF EXISTS public.products_by_category_integration_test"
                )
            )
        engine.dispose()
