"""Publish text/markdown files under a directory to Kafka. Tenant = first path segment.
Re-publishing an unchanged file is safe: content_sha256 lets downstream stages dedupe."""
import argparse
import hashlib
import json
from pathlib import Path

from confluent_kafka import Producer

from . import config


def doc_event(root: Path, path: Path) -> dict:
    text = path.read_text(encoding="utf-8")
    rel = path.relative_to(root)
    tenant = rel.parts[0] if len(rel.parts) > 1 else "public"
    first = next((ln.lstrip("# ").strip() for ln in text.splitlines() if ln.strip()), path.stem)
    return {
        "doc_id": hashlib.sha1(str(rel).encode()).hexdigest()[:16],
        "title": first, "tenant": tenant, "content": text,
        "content_sha256": hashlib.sha256(text.encode()).hexdigest(),
        "source_path": str(rel),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("root", nargs="?", default="data/docs")
    a = ap.parse_args()
    root = Path(a.root)
    p = Producer({"bootstrap.servers": config.KAFKA, "enable.idempotence": True})
    n = 0
    for f in sorted([*root.rglob("*.md"), *root.rglob("*.txt")]):
        ev = doc_event(root, f)
        p.produce(config.TOPIC_DOCS, key=ev["doc_id"], value=json.dumps(ev))
        n += 1
    p.flush()
    print(f"produced {n} docs to {config.TOPIC_DOCS}")


if __name__ == "__main__":
    main()
