"""Kafka -> Iceberg bronze sink. At-least-once: offsets are committed only after the Iceberg
commit succeeds. Every row keeps (_topic,_partition,_offset) and the raw payload (_raw), so a
replay can be deduped or rebuilt from the lake. Bad messages go to bronze.rejects, never block."""
import argparse
import json
import time
from datetime import datetime, timezone

import pyarrow as pa
from confluent_kafka import Consumer, Message
from pyiceberg.catalog import Catalog

from . import config
from .catalog import NAMESPACE, REJECTS, TABLES, bootstrap

ROUTES = {config.TOPIC_TELEMETRY: "telemetry", config.TOPIC_DOCS: "docs"}


class BronzeRowInvalidError(ValueError):
    pass


def parse_message(msg: Message, ingested_at: datetime) -> tuple[str, dict]:
    """Return (target table, row). Undecodable payloads are routed to the rejects table."""
    raw = msg.value()
    row = {"_topic": msg.topic(), "_partition": msg.partition(), "_offset": msg.offset(),
           "_ingested_at": ingested_at,
           "_raw": raw.decode("utf-8", errors="replace") if raw is not None else None}
    try:
        payload = json.loads(raw)
        if not isinstance(payload, dict):
            raise BronzeRowInvalidError(f"payload is {type(payload).__name__}, not an object")
    except (ValueError, TypeError) as e:
        return REJECTS, {**row, "error": f"{type(e).__name__}: {e}"}
    return ROUTES[msg.topic()], {**payload, **row}


def coerce_row(row: dict, schema: pa.Schema) -> dict:
    out = {}
    for f in schema:
        v = row.get(f.name)
        if v is None and not f.nullable:
            raise BronzeRowInvalidError(f"missing required field {f.name}")
        if isinstance(v, str) and pa.types.is_timestamp(f.type):
            v = datetime.fromisoformat(v)
        out[f.name] = v
    return out


def build_batch(rows: list[dict], schema: pa.Schema) -> tuple[pa.Table, list[dict]]:
    """Return (valid rows as Arrow, rejected rows with an `error` field)."""
    valid, rejects = [], []
    for row in rows:
        try:
            valid.append(coerce_row(row, schema))
        except (ValueError, TypeError) as e:
            rejects.append({**row, "error": f"{type(e).__name__}: {e}"})
    try:
        return pa.Table.from_pylist(valid, schema=schema), rejects
    except (pa.ArrowInvalid, pa.ArrowTypeError):
        pass
    # Type mismatch somewhere in the batch: isolate the offending rows one by one.
    tables = []
    for row in valid:
        try:
            tables.append(pa.Table.from_pylist([row], schema=schema))
        except (pa.ArrowInvalid, pa.ArrowTypeError) as e:
            rejects.append({**row, "error": f"{type(e).__name__}: {e}"})
    return (pa.concat_tables(tables) if tables else schema.empty_table()), rejects


def flush(catalog: Catalog, consumer: Consumer, buffers: dict[str, list[dict]]) -> int:
    total = sum(map(len, buffers.values()))
    if not total:
        return 0
    rejects = buffers[REJECTS]
    for name, rows in buffers.items():
        if name == REJECTS or not rows:
            continue
        table = catalog.load_table(f"{NAMESPACE}.{name}")
        batch, bad = build_batch(rows, table.schema().as_arrow())
        if batch.num_rows:
            table.append(batch)
        rejects.extend(bad)
        rows.clear()
    if rejects:
        table = catalog.load_table(f"{NAMESPACE}.{REJECTS}")
        table.append(pa.Table.from_pylist(rejects, schema=table.schema().as_arrow()))
        print(f"rejected {len(rejects)} rows -> {NAMESPACE}.{REJECTS}")
        rejects.clear()
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
                name, row = parse_message(msg, datetime.now(timezone.utc))
                buffers[name].append(row)
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
