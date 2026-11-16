import logging
import os

from prometheus_client import CollectorRegistry, push_to_gateway

from src.observability import metrics

logger = logging.getLogger(__name__)

PUSHGATEWAY_URL = os.getenv(
    "PUSHGATEWAY_URL",
    "localhost:9091",
)

JOB_METRICS = {
    "ecommerce_ingestion": [
        metrics.INGESTION_PRODUCTS,
        metrics.INGESTION_DURATION,
        metrics.INGESTION_RUNS,
        metrics.INGESTION_ERRORS,
    ],
    "ecommerce_bronze_to_silver": [
        metrics.SILVER_INPUT_ROWS,
        metrics.SILVER_OUTPUT_ROWS,
        metrics.SILVER_DURATION,
        metrics.SILVER_RUNS,
        metrics.SILVER_ERRORS,
        metrics.SILVER_QUALITY_CHECKS,
        metrics.SILVER_QUALITY_STATUS,
        metrics.SILVER_QUALITY_RULES_TOTAL,
        metrics.SILVER_QUALITY_RULES_PASSED,
    ],
    "ecommerce_silver_to_gold": [
        metrics.GOLD_INPUT_ROWS,
        metrics.GOLD_OUTPUT_ROWS,
        metrics.GOLD_DURATION,
        metrics.GOLD_RUNS,
        metrics.GOLD_ERRORS,
        metrics.GOLD_QUALITY_CHECKS,
        metrics.GOLD_QUALITY_STATUS,
        metrics.GOLD_QUALITY_RULES_TOTAL,
        metrics.GOLD_QUALITY_RULES_PASSED,
    ],
    "ecommerce_gold_to_postgres": [
        metrics.POSTGRES_INPUT_ROWS,
        metrics.POSTGRES_LOADED_ROWS,
        metrics.POSTGRES_DURATION,
        metrics.POSTGRES_RUNS,
        metrics.POSTGRES_ERRORS,
    ],
}


def publish_metrics(job_name: str) -> None:
    """Publie uniquement les métriques du pipeline concerné."""

    if job_name not in JOB_METRICS:
        raise ValueError(f"Job Prometheus inconnu : {job_name}")

    registry = CollectorRegistry()

    for metric in JOB_METRICS[job_name]:
        registry.register(metric)

    try:
        push_to_gateway(
            gateway=PUSHGATEWAY_URL,
            job=job_name,
            registry=registry,
            timeout=5,
        )

        logger.info(
            "Métriques publiées vers Pushgateway | job=%s",
            job_name,
        )

    except Exception:
        logger.exception(
            "Impossible de publier les métriques | job=%s",
            job_name,
        )
