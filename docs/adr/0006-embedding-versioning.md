# ADR-0006: Embedding and chunk versioning
Status: proposed

## Context
Changing chunking or embedding model must not break consumers; ML teams need stable, certified snapshots (as in the Plusgrade silver/gold registry).

## Options considered
Overwrite in place; versioned tables + active-version registry with alias swap; separate index per model version.

## Decision
TBD in weeks 3 and 8: versioned chunk/embedding tables, registry of the active version, zero-downtime re-embed drill.

## Consequences
Storage cost of keeping old versions vs rollback safety.
