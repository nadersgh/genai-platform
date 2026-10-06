# ADR-0002: Object store for the local lake: SeaweedFS
Status: accepted

## Context
Need an S3-compatible store locally so code is identical to AWS S3.

## Options considered
MinIO (Docker Hub `minio/minio` no longer exists; the quay.io mirror also failed a manifest check on 2026-09-29), SeaweedFS, Garage, LocalStack.

## Decision
SeaweedFS with an identity file created by OpenTofu (dev-only keys). Garage manifest exists too and is the fallback.

## Consequences
Path-style S3 only; Polars needs explicit S3 options (its own client). Revisit if S3 semantics gaps appear (multipart edge cases).
