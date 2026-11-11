from src.utils.spark_session import create_spark_session
from src.quality.data_quality import validate_products


def test_invalid_products():

    spark = create_spark_session("TestDataQuality")

    try:
        # Données volontairement incorrectes
        data = [
            (1, "Produit A", 10.0, 5),
            (1, "Produit B", -20.0, 3),
            (3, "Produit C", 15.0, -2),
        ]

        columns = ["id", "title", "price", "stock"]

        df = spark.createDataFrame(data, columns)

        # Great Expectations doit refuser ces données
        is_valid = validate_products(df)

        assert is_valid is False, (
            "ERREUR : les données invalides ont été acceptées"
        )

        print("\nTEST RÉUSSI : les anomalies ont été détectées.")

    finally:
        spark.stop()


if __name__ == "__main__":
    test_invalid_products()