from prometheus_client import Counter, Gauge, Histogram

# Nombre de produits récupérés lors de la dernière ingestion réussie.
INGESTION_PRODUCTS = Gauge(
    "ingestion_products_total",
    "Nombre de produits recuperes lors de la derniere ingestion reussie",
)

# Durée des ingestions.
INGESTION_DURATION = Histogram(
    "ingestion_duration_seconds",
    "Duree des executions du pipeline d'ingestion en secondes",
)

# Nombre d'exécutions selon leur statut.
INGESTION_RUNS = Counter(
    "ingestion_runs_total",
    "Nombre total d'executions du pipeline d'ingestion",
    ["status"],
)

# Nombre d'échecs.
INGESTION_ERRORS = Counter(
    "ingestion_errors_total",
    "Nombre total d'echecs du pipeline d'ingestion",
)


# ============================================================
# OBSERVABILITÉ : BRONZE → SILVER
# ============================================================

SILVER_INPUT_ROWS = Gauge(
    "silver_input_rows",
    "Nombre de produits lus depuis Bronze lors de la derniere execution",
)

SILVER_OUTPUT_ROWS = Gauge(
    "silver_output_rows",
    "Nombre de produits produits dans Silver lors de la derniere execution",
)

SILVER_DURATION = Histogram(
    "silver_transformation_duration_seconds",
    "Duree totale du pipeline Bronze vers Silver en secondes",
)

SILVER_RUNS = Counter(
    "silver_runs_total",
    "Nombre total d'executions Bronze vers Silver",
    ["status"],
)

SILVER_ERRORS = Counter(
    "silver_errors_total",
    "Nombre total d'echecs du pipeline Bronze vers Silver",
)

SILVER_QUALITY_CHECKS = Counter(
    "silver_quality_checks_total",
    "Nombre de validations de qualite Bronze vers Silver",
    ["status"],
)


# ============================================================
# OBSERVABILITÉ : SILVER → GOLD
# ============================================================

GOLD_INPUT_ROWS = Gauge(
    "gold_input_rows",
    "Nombre de produits lus depuis Silver lors de la derniere execution",
)

GOLD_OUTPUT_ROWS = Gauge(
    "gold_output_rows",
    "Nombre de categories produites dans Gold lors de la derniere execution",
)

GOLD_DURATION = Histogram(
    "gold_transformation_duration_seconds",
    "Duree totale du pipeline Silver vers Gold en secondes",
)

GOLD_RUNS = Counter(
    "gold_runs_total",
    "Nombre total d'executions Silver vers Gold",
    ["status"],
)

GOLD_ERRORS = Counter(
    "gold_errors_total",
    "Nombre total d'echecs du pipeline Silver vers Gold",
)

GOLD_QUALITY_CHECKS = Counter(
    "gold_quality_checks_total",
    "Nombre de validations de qualite Silver vers Gold",
    ["status"],
)


# ============================================================
# OBSERVABILITÉ : GOLD → POSTGRESQL
# ============================================================

POSTGRES_INPUT_ROWS = Gauge(
    "postgres_input_rows",
    "Nombre de lignes Gold a charger lors de la derniere execution",
)

POSTGRES_LOADED_ROWS = Gauge(
    "postgres_loaded_rows",
    "Nombre de lignes chargees lors du dernier chargement reussi",
)

POSTGRES_DURATION = Histogram(
    "postgres_loading_duration_seconds",
    "Duree totale du pipeline Gold vers PostgreSQL en secondes",
)

POSTGRES_RUNS = Counter(
    "postgres_runs_total",
    "Nombre total d'executions Gold vers PostgreSQL",
    ["status"],
)

POSTGRES_ERRORS = Counter(
    "postgres_errors_total",
    "Nombre total d'echecs du chargement PostgreSQL",
)
