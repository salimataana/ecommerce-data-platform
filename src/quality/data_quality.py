import great_expectations as gx


def validate_products(df):

    # 1. Initialiser Great Expectations
    context = gx.get_context(mode="ephemeral")

    # 2. Déclarer la source Spark
    data_source = context.data_sources.add_spark(
        name="ecommerce_spark"
    )

    # 3. Déclarer le DataFrame
    data_asset = data_source.add_dataframe_asset(
        name="products"
    )

    batch_definition = data_asset.add_batch_definition_whole_dataframe(
        "products_batch"
    )

    batch = batch_definition.get_batch(
        batch_parameters={"dataframe": df}
    )

    # 4. Définir les règles de qualité
    suite = gx.ExpectationSuite(
        name="products_quality",
        expectations=[
            # Identifiant obligatoire
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column="id"
            ),

            # Identifiant unique
            gx.expectations.ExpectColumnValuesToBeUnique(
                column="id"
            ),

            # Prix positif ou nul
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="price",
                min_value=0
            ),

            # Stock positif ou nul
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="stock",
                min_value=0
            ),

            # Nom du produit obligatoire
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column="title"
            ),

            # Nom du produit non vide
            gx.expectations.ExpectColumnValueLengthsToBeBetween(
                column="title",
                min_value=1
            )
        ]
    )

    # 5. Exécuter les contrôles
    result = batch.validate(suite)

    for validation in result.results:
        print(
            validation.expectation_config.type,
            ":", validation.success
        )

    print("\n=== VALIDATION GREAT EXPECTATIONS ===")
    print("Qualité des produits :", result.success)

    return result.success