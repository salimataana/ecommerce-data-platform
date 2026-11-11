import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)

API_URL = "https://dummyjson.com/products"
OUTPUT_DIR = Path("data/bronze/products")


class ProductAPIClient:
    """
    Client responsable de l'extraction des produits
    depuis l'API DummyJSON et de leur sauvegarde en Bronze.
    """

    def __init__(self, api_url=API_URL, output_dir=OUTPUT_DIR, limit=100):
        self.api_url = api_url
        self.output_dir = Path(output_dir)
        self.limit = limit

    def create_session(self):
        """Crée une session HTTP avec gestion des tentatives."""

        session = requests.Session()

        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)

        session.mount("https://", adapter)
        session.mount("http://", adapter)

        return session

    def extract_products(self):
        """Récupère tous les produits grâce à la pagination."""

        logger.info("Début de l'extraction des produits")

        all_products = []
        skip = 0
        total = None

        with self.create_session() as session:
            while total is None or skip < total:
                logger.info(
                    "Récupération des produits : skip=%s, limit=%s", skip, self.limit
                )

                response = session.get(
                    self.api_url, params={"limit": self.limit, "skip": skip}, timeout=30
                )

                response.raise_for_status()

                data = response.json()

                if not isinstance(data, dict):
                    raise ValueError("Format JSON inattendu")

                products = data.get("products")

                if not isinstance(products, list):
                    raise ValueError("Liste de produits absente")

                page_total = data.get("total")

                if not isinstance(page_total, int) or page_total < 0:
                    raise ValueError("Total de produits invalide")

                if total is None:
                    total = page_total
                    logger.info("Nombre total de produits : %s", total)

                if not products and skip < total:
                    raise ValueError("Page vide reçue avant la fin de la pagination")

                all_products.extend(products)

                logger.info("%s produits récupérés sur %s", len(all_products), total)

                skip += len(products)

        logger.info("Extraction terminée : %s produits récupérés", len(all_products))

        return {"products": all_products, "total": len(all_products)}

    def save_raw_data(self, data):
        """Enregistre les produits au format JSON dans Bronze."""

        self.output_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")

        filename = self.output_dir / f"products_{timestamp}.json"

        with open(filename, "w", encoding="utf-8") as file:
            json.dump(data, file, ensure_ascii=False, indent=4)

        logger.info("Données enregistrées : %s", filename)

        return filename

    def run(self):
        """Exécute le pipeline d'ingestion."""

        logger.info("Démarrage du pipeline")

        data = self.extract_products()

        self.save_raw_data(data)

        logger.info("Pipeline terminé avec succès")


# Fonctions conservées pour la compatibilité avec le code existant.


def create_session():
    return ProductAPIClient().create_session()


def extract_products():
    return ProductAPIClient().extract_products()


def save_raw_data(data):
    return ProductAPIClient().save_raw_data(data)


def run_pipeline():
    return ProductAPIClient().run()


if __name__ == "__main__":
    run_pipeline()
