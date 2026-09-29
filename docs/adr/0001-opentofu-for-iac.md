# ADR-0001: OpenTofu for infrastructure as code
Status: accepted

## Context
Need one IaC tool for local (Docker) and cloud (AWS). Prior team experience is with Terraform.

## Options considered
Terraform (BUSL license), OpenTofu (MPL, drop-in compatible, native state encryption), Pulumi.

## Decision
OpenTofu. Separate root modules per environment (`envs/local`, `envs/aws`).

## Consequences
Local and cloud share no modules yet. Revisit once a real abstraction emerges
(e.g. `object_store`, `vector_db`) rather than forcing one up front.
