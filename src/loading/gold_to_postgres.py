from src.utils.spark_session import create_spark_session
from src.utils.db_connection import get_engine
from sqlalchemy import text

def load_gold_to_postgres():
    spark = create_spark_session("GoldToPostgres")

    try:
        # Lire les données Gold
        df_gold = spark.read.parquet(
            "data/gold/products_by_category"
        )

        # Convertir Spark DataFrame en Pandas
        df_pandas = df_gold.toPandas()

        # Connexion PostgreSQL
        engine = get_engine()

        try:
            # Charger les données dans PostgreSQL
            with engine.begin() as conn:

                # Vérifier si la table existe
                table_exists = conn.execute(
                    text("""
                        SELECT EXISTS (
                            SELECT 1
                            FROM information_schema.tables
                            WHERE table_schema = 'public'
                            AND table_name = 'products_by_category'
                        )
                    """)
                ).scalar()

                # Vider la table uniquement si elle existe
                if table_exists:
                    conn.execute(
                        text("DELETE FROM public.products_by_category")
                    )

                # Charger les nouvelles données
                df_pandas.to_sql(
                    name="products_by_category",
                    con=conn,
                    schema="public",
                    if_exists="append",
                    index=False
                )
        finally:
            engine.dispose()

        print("Chargement PostgreSQL réussi !")
        print("Nombre de catégories :", len(df_pandas))

    finally:
        spark.stop()


if __name__ == "__main__":
    load_gold_to_postgres()