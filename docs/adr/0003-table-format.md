# ADR-0003: Table format: Apache Iceberg (SQL catalog in Postgres)
Status: accepted

## Context
Bronze/silver/gold tables need transactions, schema evolution and time travel over plain Parquet.

## Options considered
Iceberg, Delta Lake, Hudi, plain partitioned Parquet.

## Decision
Iceberg via pyiceberg; catalog in Postgres (no extra service); Polars/DuckDB read it. Schema evolution already exercised: `lang`, `section_label`, `heading_path` added to `bronze.docs` with no rewrite.

## Consequences
SQL catalog is single-node and simple; swap for Nessie/Polaris/Glue when multi-engine access matters. Small-file compaction is our job (week 11 benchmark).
