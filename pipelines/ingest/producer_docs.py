"""Publish text/markdown files under a directory to Kafka. Tenant = first path segment.
Re-publishing an unchanged file is safe: content_sha256 lets downstream stages dedupe.
`--delete PATH...` publishes delete events instead (op=delete, no content) for removed files."""
import argparse
import hashlib
import json
from pathlib import Path

from confluent_kafka import Producer

from . import config
from .catalog import DocOp


def identify_doc(rel: Path) -> dict:
    return {
        "doc_id": hashlib.sha1(str(rel).encode()).hexdigest()[:16],
        "tenant": rel.parts[0] if len(rel.parts) > 1 else "public",
        "source_path": str(rel),
    }


def doc_event(root: Path, path: Path) -> dict:
    text = path.read_text(encoding="utf-8")
    first = next((ln.lstrip("# ").strip() for ln in text.splitlines() if ln.strip()), path.stem)
    return {
        **identify_doc(path.relative_to(root)),
        "title": first, "content": text,
        "content_sha256": hashlib.sha256(text.encode()).hexdigest(),
        "op": DocOp.UPSERT,
    }


def delete_event(rel: Path) -> dict:
    return {**identify_doc(rel), "op": DocOp.DELETE}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("root", nargs="?", default="data/docs")
    ap.add_argument("--delete", nargs="+", type=Path, default=[], metavar="PATH",
                    help="paths relative to root, e.g. acme/data-retention.md")
    a = ap.parse_args()
    root = Path(a.root)
    if a.delete:
        events = [delete_event(rel) for rel in a.delete]
    else:
        events = [doc_event(root, f) for f in sorted([*root.rglob("*.md"), *root.rglob("*.txt")])]
    p = Producer({"bootstrap.servers": config.KAFKA, "enable.idempotence": True})
    for ev in events:
        p.produce(config.TOPIC_DOCS, key=ev["doc_id"], value=json.dumps(ev))
    p.flush()
    print(f"produced {len(events)} {'delete' if a.delete else 'upsert'} events to {config.TOPIC_DOCS}")


if __name__ == "__main__":
    main()
