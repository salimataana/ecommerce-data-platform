import pytest

from src.quality.data_quality import GoldDataValidator
from src.transformation.silver_to_gold import SilverToGoldTransformer


GOLD_SCHEMA = """
    category STRING,
    total_products BIGINT,
    average_price DOUBLE,
    total_stock BIGINT
"""


@pytest.mark.parametrize(
    "products, expected",
    [
        pytest.param(
            [("smartphones", 2, 150.0, 30)],
            True,
            id="donnees_valides",
        ),
        pytest.param(
            [
                ("smartphones", 2, 150.0, 30),
                ("laptops", 1, 500.0, 5),
            ],
            True,
            id="plusieurs_categories_valides",
        ),
        pytest.param(
            [(None, 2, 150.0, 30)],
            False,
            id="categorie_nulle",
        ),
        pytest.param(
            [
                ("smartphones", 2, 150.0, 30),
                ("smartphones", 1, 100.0, 10),
            ],
            False,
            id="categorie_dupliquee",
        ),
        pytest.param(
            [("smartphones", 0, 150.0, 30)],
            False,
            id="nombre_produits_zero",
        ),
        pytest.param(
            [("smartphones", 2, -150.0, 30)],
            False,
            id="prix_moyen_negatif",
        ),
        pytest.param(
            [("smartphones", 2, 150.0, -10)],
            False,
            id="stock_negatif",
        ),
        pytest.param(
            [("smartphones", 2, 0.0, 0)],
            True,
            id="prix_et_stock_zero",
        ),
    ],
)
def test_gold_data_validation(spark, products, expected):

    df = spark.createDataFrame(
        products,
        schema=GOLD_SCHEMA,
    )

    validator = GoldDataValidator(df)

    assert validator.validate() is expected


@pytest.mark.parametrize(
    "average_price, should_fail",
    [
        (100.0, False),
        (-100.0, True),
    ],
)
def test_silver_to_gold_validation(
    spark,
    tmp_path,
    average_price,
    should_fail,
):
    df = spark.createDataFrame(
        [("smartphones", 1, average_price, 10)],
        schema=GOLD_SCHEMA,
    )

    transformer = SilverToGoldTransformer(
        silver_dir=tmp_path / "silver",
        gold_dir=tmp_path / "gold",
        spark=spark,
    )

    if should_fail:
        with pytest.raises(
            ValueError,
            match="Échec de la validation des données Gold",
        ):
            transformer.validate(df)
    else:
        transformer.validate(df)