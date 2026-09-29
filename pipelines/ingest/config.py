"""Endpoints for the local stack (see infra/envs/local). Override via env vars."""
import os

KAFKA = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:8333")
S3_KEY = os.getenv("S3_ACCESS_KEY", "local")  # dev-only, matches infra/envs/local variables
S3_SECRET = os.getenv("S3_SECRET_KEY", "localdev-only")
BUCKET = os.getenv("LAKE_BUCKET", "lake")
PG_PASSWORD = os.getenv("PG_PASSWORD", "localdev-only")
CATALOG_URI = os.getenv(
    "CATALOG_URI", f"postgresql+psycopg2://platform:{PG_PASSWORD}@localhost:5432/platform"
)

TOPIC_TELEMETRY = "telemetry.raw"
TOPIC_DOCS = "docs.raw"
