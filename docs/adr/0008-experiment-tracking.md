# ADR-0008: Experiment tracking and eval store: MLflow
Status: proposed

## Context
Evals (recall@k, answer quality), model/embedding versions and traces need a system of record that CI can gate on.

## Options considered
MLflow (self-hosted), managed MLflow (Databricks), Weights & Biases, Langfuse.

## Decision
MLflow self-hosted locally with Postgres backend and file artifacts; hosting in AWS decided in week 9.

## Consequences
Runs `pip install` at start today; needs a proper image. Artifacts on a local volume, not S3 yet.
