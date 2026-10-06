# Roadmap (12 weeks, ~6-8 h/week)

Goal: governed GenAI data platform. Ingest -> chunk -> embed -> versioned index -> retrieval API -> evals -> observability -> cloud deploy.

| Wk | Focus | Deliverable |
|----|-------|-------------|
| 1 | IaC foundation | `make local-up` works; AWS state bucket bootstrapped; CI runs `tofu fmt/validate` |
| 2 | Ingestion | Docs + telemetry -> Redpanda -> Iceberg/Parquet bronze on SeaweedFS (S3 API) |
| 3 | Doc pipeline | parse -> chunk -> embed; chunk + embedding versioning registry (Silver/Gold style) |
| 4 | Vector index | Qdrant + pgvector, hybrid search (BM25 + vector) |
| 5 | Retrieval API | FastAPI `/search`, `/ask`; tenant-aware filtering |
| 6 | Access control | Per-tenant ACLs enforced at retrieval; audit log |
| 7 | Evals | recall@k, answer-quality set, MLflow tracking |
| 8 | CI gates + migration | Eval regression gate in CI; zero-downtime re-embed drill (new model version, alias swap) |
| 9 | AWS deploy | ECS/EKS + RDS pgvector via OpenTofu; ECR image push |
| 10 | Observability | OTel traces, LLM token/cost per query, freshness + drift alerts |
| 11 | Load + cost | Benchmarks, cost model, cluster/index sizing |
| 12 | Architecture write-up | 6-8 ADRs, reference architecture, threat model (PII in embeddings, prompt injection via docs), blog post |

## ADR backlog
See [adr/README.md](adr/README.md) for the index and [architecture.md](architecture.md) for how each ADR maps to a component.
