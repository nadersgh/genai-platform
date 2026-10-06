# ADR-0007: Tenant isolation at retrieval time
Status: proposed

## Context
Docs carry a tenant (`faa`, `tc-canada`, `acme`, `globex`, `public`). RAG can leak data through retrieved context.

## Options considered
Filter on payload/metadata; collection per tenant; row-level security in Postgres; per-tenant index.

## Decision
TBD in weeks 5-6. Enforce in the retrieval layer, never in the prompt; audit-log every retrieval.

## Consequences
Collection-per-tenant scales poorly; filter-only relies on correct query construction (needs tests).
