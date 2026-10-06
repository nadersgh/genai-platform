# GenAI / ML Platform

Learning + portfolio project: governed GenAI data platform, all infra via OpenTofu (local Docker + AWS).

```bash
make local-up     # MinIO, Postgres+pgvector, Qdrant, Redpanda, MLflow
make local-down
make aws-plan     # needs AWS creds; nothing is created by plan
make validate
```

Local endpoints print as the `endpoints` output. UIs: MLflow http://localhost:5001, Postgres (Adminer) http://localhost:8081 (System: PostgreSQL, user `platform`, db `platform`). See [docs/architecture.md](docs/architecture.md) and [docs/ROADMAP.md](docs/ROADMAP.md).
Local passwords are dev-only defaults; override with a git-ignored `*.tfvars`.

## Week 2: ingestion
```bash
make bronze-init produce-telemetry produce-docs sink verify
```
Synthetic telemetry (with injected faults + late events) and tenant-tagged docs go to Redpanda
(`telemetry.raw`, `docs.raw`), then an at-least-once sink writes Iceberg `bronze.*` tables
(SQL catalog in Postgres, data on SeaweedFS). Each row carries `_topic/_partition/_offset` for
replay + dedupe. Local Python env: `uv` in `pipelines/`.

### Querying with Polars (no pandas)
```python
# cd pipelines && PYTHONPATH=. uv run python
import polars as pl
from ingest.catalog import scan, get_catalog

scan("telemetry").filter(pl.col("engine_temp_c") > 1500).head(5).collect()   # lazy, pushdown
get_catalog().load_table("bronze.telemetry").scan().to_polars()              # eager alternative
```
Polars reads Parquet with its own S3 client, so `scan()` passes the local endpoint/credentials
via `polars_storage_options()`. The sink still builds Arrow tables: pyiceberg writes Arrow.
