# ADR-0009: Re-embedding: streaming vs scheduled batch
Status: proposed

## Context
Documents change and embedding models get replaced; freshness vs cost trade-off.

## Options considered
Event-driven per document; scheduled micro-batch; full rebuild on model change only.

## Decision
TBD in weeks 3 and 8 using `content_sha256` to skip unchanged docs.

## Consequences
Streaming adds moving parts; batch is simpler but staler.
