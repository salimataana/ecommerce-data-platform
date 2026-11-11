import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

load_dotenv()


# Créer une connexion PostgreSQL réutilisable
def get_engine():
    url = URL.create(
        drivername="postgresql+psycopg2",
        username=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT")),
        database=os.getenv("DB_NAME")
    )

    return create_engine(url)


# Tester la connexion PostgreSQL
def test_connection():
    engine = get_engine()

    try:
        with engine.connect() as connection:
            result = connection.execute(
                text("SELECT current_database(), current_user")
            )

            print(
                "Connexion PostgreSQL réussie :",
                result.fetchone()
            )
    finally:
        engine.dispose()


if __name__ == "__main__":
    test_connection()