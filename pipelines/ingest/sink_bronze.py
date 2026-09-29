"""Kafka -> Iceberg bronze sink. At-least-once: offsets are committed only after the Iceberg
commit succeeds. Every row keeps (_topic,_partition,_offset), so a replay can be deduped."""
import argparse
import json
import time
from datetime import datetime, timezone

import pyarrow as pa
from confluent_kafka import Consumer

from . import config
from .catalog import NAMESPACE, TABLES, bootstrap

ROUTES = {config.TOPIC_TELEMETRY: "telemetry", config.TOPIC_DOCS: "docs"}


def to_arrow(rows: list[dict], table) -> pa.Table:
    schema = table.schema().as_arrow()
    cols = {f.name: [r.get(f.name) for r in rows] for f in schema}
    for f in schema:
        if pa.types.is_timestamp(f.type):
            cols[f.name] = [
                datetime.fromisoformat(v) if isinstance(v, str) else v for v in cols[f.name]
            ]
    return pa.Table.from_pydict(cols, schema=schema)


def flush(catalog, consumer, buffers: dict[str, list[dict]]) -> int:
    total = 0
    for name, rows in buffers.items():
        if not rows:
            continue
        table = catalog.load_table(f"{NAMESPACE}.{name}")
        table.append(to_arrow(rows, table))
        total += len(rows)
        rows.clear()
    if total:
        consumer.commit(asynchronous=False)
    return total


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--batch-size", type=int, default=2000)
    ap.add_argument("--max-wait", type=float, default=10.0, help="flush at least every N sec")
    ap.add_argument("--idle-exit", type=float, default=0, help="exit after N sec idle (0 = never)")
    a = ap.parse_args()

    catalog = bootstrap()
    consumer = Consumer({
        "bootstrap.servers": config.KAFKA, "group.id": "bronze-sink",
        "enable.auto.commit": False, "auto.offset.reset": "earliest",
    })
    buffers: dict[str, list[dict]] = {t: [] for t in TABLES}
    last_flush = last_msg = time.monotonic()
    assigned = False

    def on_assign(_consumer, _partitions):
        nonlocal assigned, last_msg
        assigned, last_msg = True, time.monotonic()  # idle clock starts once we own partitions

    consumer.subscribe(list(ROUTES), on_assign=on_assign)
    written = 0
    try:
        while True:
            msg = consumer.poll(1.0)
            now = time.monotonic()
            if msg is not None and not msg.error():
                row = json.loads(msg.value())
                row.update(_topic=msg.topic(), _partition=msg.partition(), _offset=msg.offset(),
                           _ingested_at=datetime.now(timezone.utc))
                buffers[ROUTES[msg.topic()]].append(row)
                last_msg = now
            elif msg is not None:
                print("kafka error:", msg.error())
            pending = sum(map(len, buffers.values()))
            if pending >= a.batch_size or (pending and now - last_flush >= a.max_wait):
                n = flush(catalog, consumer, buffers)
                written += n
                last_flush = now
                print(f"flushed {n} rows (total {written})")
            if a.idle_exit and assigned and now - last_msg > a.idle_exit and not pending:
                break
    except KeyboardInterrupt:
        pass
    finally:
        written += flush(catalog, consumer, buffers)
        consumer.close()
        print(f"done, {written} rows written")


if __name__ == "__main__":
    main()
