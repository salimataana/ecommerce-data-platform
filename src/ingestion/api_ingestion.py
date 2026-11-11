import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import requests


# Configuration des logs
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


# Configuration
API_URL = "https://dummyjson.com/products"
OUTPUT_DIR = Path("data/bronze/products")


def create_session():
    """
    Crée une session HTTP avec une stratégie de retry.
    """

    session = requests.Session()

    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )

    adapter = HTTPAdapter(
        max_retries=retry_strategy
    )

    session.mount("https://", adapter)
    session.mount("http://", adapter)

    return session


def extract_products():
    """
    Récupère tous les produits de l'API avec pagination.
    """

    logger.info("Début de l'extraction des produits")

    all_products = []

    limit = 100
    skip = 0
    total = None

    with create_session() as session:

        while total is None or skip < total:

            logger.info(
                "Récupération des produits : skip=%s, limit=%s",
                skip,
                limit
            )

            response = session.get(
                API_URL,
                params={
                    "limit": limit,
                    "skip": skip
                },
                timeout=30
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
                raise ValueError(
                    "Page vide reçue avant la fin de la pagination"
                )

            all_products.extend(products)

            logger.info(
                "%s produits récupérés sur %s",
                len(all_products),
                total
            )

            skip += len(products)

    logger.info(
        "Extraction terminée : %s produits récupérés",
        len(all_products)
    )

    return {
        "products": all_products,
        "total": len(all_products)
    }


def save_raw_data(data):
    """
    Stocke les données brutes au format JSON.
    """

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    timestamp = datetime.now(timezone.utc).strftime(
        "%Y%m%d_%H%M%S_%f"
    )

    filename = OUTPUT_DIR / f"products_{timestamp}.json"

    with open(filename, "w", encoding="utf-8") as file:
        json.dump(
            data,
            file,
            ensure_ascii=False,
            indent=4
        )

    logger.info("Données enregistrées : %s", filename)

    return filename


def run_pipeline():
    """
    Exécute le pipeline d'ingestion.
    """

    logger.info("Démarrage du pipeline")

    data = extract_products()

    save_raw_data(data)

    logger.info("Pipeline terminé avec succès")


if __name__ == "__main__":
    run_pipeline()