import logging

import great_expectations as gx
from pyspark.sql import DataFrame


logger = logging.getLogger(__name__)


class ProductDataValidator:
    """
    Validation de la qualité des produits avec Great Expectations.

    Règles :
    1. id non nul
    2. id unique
    3. price >= 0
    4. stock >= 0
    5. title non nul
    6. longueur(title) >= 1
    """

    def __init__(self, dataframe: DataFrame):
        self.dataframe = dataframe
        self.context = None
        self.batch = None
        self.suite = None

    def create_batch(self):
        """Prépare le DataFrame Spark pour Great Expectations."""

        self.context = gx.get_context(mode="ephemeral")

        data_source = self.context.data_sources.add_spark(
            name="ecommerce_spark"
        )

        data_asset = data_source.add_dataframe_asset(
            name="products"
        )

        batch_definition = (
            data_asset.add_batch_definition_whole_dataframe(
                "products_batch"
            )
        )

        self.batch = batch_definition.get_batch(
            batch_parameters={
                "dataframe": self.dataframe
            }
        )

        return self.batch

    def create_expectation_suite(self):
        """Définit les six règles de qualité."""

        self.suite = gx.ExpectationSuite(
            name="products_quality",
            expectations=[
                gx.expectations.ExpectColumnValuesToNotBeNull(
                    column="id"
                ),
                gx.expectations.ExpectColumnValuesToBeUnique(
                    column="id"
                ),
                gx.expectations.ExpectColumnValuesToBeBetween(
                    column="price",
                    min_value=0
                ),
                gx.expectations.ExpectColumnValuesToBeBetween(
                    column="stock",
                    min_value=0
                ),
                gx.expectations.ExpectColumnValuesToNotBeNull(
                    column="title"
                ),
                gx.expectations.ExpectColumnValueLengthsToBeBetween(
                    column="title",
                    min_value=1
                ),
            ]
        )

        return self.suite

    def validate(self) -> bool:
        """Exécute les contrôles et retourne leur résultat global."""

        batch = self.create_batch()
        suite = self.create_expectation_suite()

        result = batch.validate(suite)

        for validation in result.results:
            logger.info(
                "%s : %s",
                validation.expectation_config.type,
                validation.success,
            )

        logger.info(
            "Qualité des produits : %s",
            result.success,
        )

        return bool(result.success)


# ============================================================
# Compatibilité avec le pipeline Bronze -> Silver
# ============================================================

def validate_products(df: DataFrame) -> bool:
    """
    Point d'entrée historique conservé pour éviter
    de modifier BronzeToSilverTransformer.
    """

    validator = ProductDataValidator(dataframe=df)

    return validator.validate()


class GoldDataValidator:
    """
    Validation des indicateurs agrégés Gold.
    """

    def __init__(self, dataframe: DataFrame):
        self.dataframe = dataframe

    def validate(self) -> bool:
        context = gx.get_context(mode="ephemeral")

        data_source = context.data_sources.add_spark(
            name="ecommerce_gold_spark"
        )

        data_asset = data_source.add_dataframe_asset(
            name="products_by_category"
        )

        batch_definition = (
            data_asset.add_batch_definition_whole_dataframe(
                "gold_batch"
            )
        )

        batch = batch_definition.get_batch(
            batch_parameters={"dataframe": self.dataframe}
        )

        suite = gx.ExpectationSuite(
            name="gold_quality",
            expectations=[
                gx.expectations.ExpectColumnValuesToNotBeNull(
                    column="category"
                ),
                gx.expectations.ExpectColumnValuesToBeUnique(
                    column="category"
                ),
                gx.expectations.ExpectColumnValuesToBeBetween(
                    column="total_products",
                    min_value=1
                ),
                gx.expectations.ExpectColumnValuesToBeBetween(
                    column="average_price",
                    min_value=0
                ),
                gx.expectations.ExpectColumnValuesToBeBetween(
                    column="total_stock",
                    min_value=0
                ),
            ]
        )

        result = batch.validate(suite)

        for validation in result.results:
            logger.info(
                "%s : %s",
                validation.expectation_config.type,
                validation.success,
            )

        logger.info("Qualité Gold : %s", result.success)

        return bool(result.success)