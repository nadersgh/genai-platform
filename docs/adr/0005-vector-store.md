# ADR-0005: Vector store: Qdrant vs pgvector
Status: proposed

## Context
Retrieval needs ANN search with metadata filtering (tenant, language) and hybrid (BM25 + vector).

## Options considered
Qdrant, pgvector (already in Postgres), OpenSearch, Databricks Vector Search.

## Decision
TBD in week 4 by benchmark: recall@k, p95 latency, filtered-search cost, ops burden.

## Consequences
Cloud choice interacts with cost (RDS pgvector is cheapest to run).
