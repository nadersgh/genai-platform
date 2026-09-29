"""Iceberg catalog (SQL catalog in Postgres, data files on S3) + bronze table definitions."""
import boto3
from botocore.exceptions import ClientError
from pyiceberg.catalog import Catalog
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import DayTransform
from pyiceberg.types import (
    DoubleType, LongType, NestedField, StringType, TimestamptzType,
)

from . import config

NAMESPACE = "bronze"

# Kafka provenance columns on every bronze row => replay + dedupe by (topic, partition, offset).
_PROVENANCE = [
    NestedField(100, "_topic", StringType(), required=True),
    NestedField(101, "_partition", LongType(), required=True),
    NestedField(102, "_offset", LongType(), required=True),
    NestedField(103, "_ingested_at", TimestamptzType(), required=True),
]

TELEMETRY_SCHEMA = Schema(
    NestedField(1, "aircraft_id", StringType(), required=True),
    NestedField(2, "event_time", TimestamptzType(), required=True),
    NestedField(3, "latitude", DoubleType()),
    NestedField(4, "longitude", DoubleType()),
    NestedField(5, "altitude_m", DoubleType()),
    NestedField(6, "speed_kts", DoubleType()),
    NestedField(7, "engine_temp_c", DoubleType()),
    *_PROVENANCE,
)

DOCS_SCHEMA = Schema(
    NestedField(1, "doc_id", StringType(), required=True),
    NestedField(2, "title", StringType()),
    NestedField(3, "tenant", StringType()),
    NestedField(4, "content", StringType()),
    NestedField(5, "content_sha256", StringType()),
    NestedField(6, "source_path", StringType()),
    *_PROVENANCE,
)

TABLES = {
    "telemetry": (TELEMETRY_SCHEMA, PartitionSpec(
        PartitionField(source_id=2, field_id=1000, transform=DayTransform(), name="event_day"))),
    "docs": (DOCS_SCHEMA, PartitionSpec()),
}


def ensure_bucket() -> None:
    s3 = boto3.client(
        "s3", endpoint_url=config.S3_ENDPOINT, region_name="us-east-1",
        aws_access_key_id=config.S3_KEY, aws_secret_access_key=config.S3_SECRET,
    )
    try:
        s3.head_bucket(Bucket=config.BUCKET)
    except ClientError:
        s3.create_bucket(Bucket=config.BUCKET)


def get_catalog() -> Catalog:
    return SqlCatalog(
        "local",
        uri=config.CATALOG_URI,
        warehouse=f"s3://{config.BUCKET}/warehouse",
        **{
            "s3.endpoint": config.S3_ENDPOINT,
            "s3.access-key-id": config.S3_KEY,
            "s3.secret-access-key": config.S3_SECRET,
            "s3.region": "us-east-1",
            "s3.force-virtual-addressing": "false",
        },
    )


def polars_storage_options() -> dict:
    """Polars uses its own object-store client, so it needs the S3 settings passed explicitly."""
    return {
        "aws_endpoint_url": config.S3_ENDPOINT,
        "aws_access_key_id": config.S3_KEY,
        "aws_secret_access_key": config.S3_SECRET,
        "aws_region": "us-east-1",
        "aws_allow_http": "true",
    }


def scan(name: str):
    """Lazy Polars scan of a bronze table: `scan("telemetry").filter(...).collect()`."""
    import polars as pl

    return pl.scan_iceberg(get_catalog().load_table(f"{NAMESPACE}.{name}"),
                           storage_options=polars_storage_options())


def bootstrap() -> Catalog:
    """Idempotent: create bucket, namespace and bronze tables if missing."""
    ensure_bucket()
    cat = get_catalog()
    cat.create_namespace_if_not_exists(NAMESPACE)
    for name, (schema, spec) in TABLES.items():
        cat.create_table_if_not_exists(f"{NAMESPACE}.{name}", schema=schema, partition_spec=spec)
    return cat


if __name__ == "__main__":
    c = bootstrap()
    print("tables:", c.list_tables(NAMESPACE))
