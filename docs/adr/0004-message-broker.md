# ADR-0004: Message broker: Redpanda (Kafka API)
Status: accepted

## Context
Need a replayable event log between producers and the lake, runnable on a 4 GiB laptop VM.

## Options considered
Apache Kafka (KRaft), Redpanda, Pulsar, no broker (direct file drops).

## Decision
Redpanda single node; the Kafka API keeps client code portable to MSK or Kafka later.

## Consequences
Not production-equivalent (single node, overprovisioned flag). AWS choice deferred to week 9.
