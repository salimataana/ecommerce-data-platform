import pytest

from src.quality.data_quality import (
    ProductDataValidator,
    validate_products,
)


@pytest.mark.parametrize(
    "products, expected",
    [
        pytest.param(
            [(1, "iPhone", 100.0, 10)],
            True,
            id="produit_valide",
        ),
        pytest.param(
            [(1, "iPhone", 100.0, 10), (2, "Samsung", 200.0, 20)],
            True,
            id="plusieurs_produits_valides",
        ),
        pytest.param(
            [(None, "iPhone", 100.0, 10)],
            False,
            id="identifiant_null",
        ),
        pytest.param(
            [(1, "iPhone", 100.0, 10), (1, "Samsung", 200.0, 20)],
            False,
            id="identifiant_duplique",
        ),
        pytest.param(
            [(1, "iPhone", -100.0, 10)],
            False,
            id="prix_negatif",
        ),
        pytest.param(
            [(1, "iPhone", 100.0, -5)],
            False,
            id="stock_negatif",
        ),
        pytest.param(
            [(1, None, 100.0, 10)],
            False,
            id="titre_null",
        ),
        pytest.param(
            [(1, "", 100.0, 10)],
            False,
            id="titre_vide",
        ),
        pytest.param(
            [(1, "iPhone", 0.0, 0)],
            True,
            id="prix_et_stock_zero",
        ),
    ],
)
def test_product_data_validation(spark, products, expected):

    df = spark.createDataFrame(
        products,
        schema="""
            id BIGINT,
            title STRING,
            price DOUBLE,
            stock INT
        """,
    )

    validator = ProductDataValidator(df)

    result = validator.validate()

    assert result is expected


def test_validate_products_compatibility(spark):

    df = spark.createDataFrame(
        [(1, "iPhone", 100.0, 10)],
        schema="""
            id BIGINT,
            title STRING,
            price DOUBLE,
            stock INT
        """,
    )

    assert validate_products(df) is True
