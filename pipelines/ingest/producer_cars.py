"""Canadian Aviation Regulations (Justice Laws XML) -> docs.raw, one document per Section.
EN and FR editions share section labels 1:1, giving a parallel corpus for cross-lingual evals.
Amendment history notes are dropped (noise for retrieval); heading breadcrumb is kept."""
import argparse
import hashlib
import json
import re
import xml.etree.ElementTree as ET
from pathlib import Path

from confluent_kafka import Producer

from . import config

# Block-level elements get a leading space so "200.02(1)Subject" becomes "200.02 (1) Subject".
BLOCK = {"Label", "MarginalNote", "Subsection", "Paragraph", "Subparagraph", "Clause",
         "Subclause", "Definition", "Text", "TitleText", "row", "entry", "Provision", "List", "Item"}
DROP = {"HistoricalNote", "FootnoteRef"}


def _walk(el: ET.Element, out: list[str]) -> None:
    if el.tag in DROP:
        if el.tail:
            out.append(el.tail)
        return
    if el.tag in BLOCK:
        out.append(" ")
    if el.text:
        out.append(el.text)
    for child in el:
        _walk(child, out)
    if el.tail:
        out.append(el.tail)


def clean_text(el: ET.Element) -> str:
    parts: list[str] = []
    if el.text:
        parts.append(el.text)
    for child in el:
        _walk(child, parts)
    return re.sub(r"\s+", " ", "".join(parts)).strip()


def heading_text(h: ET.Element) -> str:
    return re.sub(r"\s+", " ", " ".join(h.itertext())).strip()


def iter_sections(xml_path: Path):
    """Yield (label, heading_path, marginal_note, text) for each Section under Body."""
    body = ET.parse(xml_path).getroot().find("Body")
    stack: dict[int, str] = {}
    for el in body:
        if el.tag == "Heading":
            lvl = int(el.get("level", "1"))
            stack = {k: v for k, v in stack.items() if k < lvl}
            stack[lvl] = heading_text(el)
        elif el.tag == "Section":
            label_el = el.find("Label")
            label = (label_el.text or "").strip() if label_el is not None else ""
            note_el = el.find("MarginalNote")
            note = clean_text(note_el) if note_el is not None else ""
            yield label, " > ".join(stack[k] for k in sorted(stack)), note, clean_text(el)


def section_event(lang: str, label: str, path: str, note: str, text: str) -> dict:
    title = f"CAR {label}" + (f" - {note}" if note else "")
    return {
        "doc_id": f"cars-{lang}-{label}", "title": title, "tenant": "tc-canada",
        "content": text, "content_sha256": hashlib.sha256(text.encode()).hexdigest(),
        "source_path": f"cars_{lang}.xml#{label}", "lang": lang,
        "section_label": label, "heading_path": path,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", default="data/raw")
    ap.add_argument("--langs", nargs="+", default=["en", "fr"])
    ap.add_argument("--limit", type=int, default=0, help="max sections per language; 0 = all")
    a = ap.parse_args()
    p = Producer({"bootstrap.servers": config.KAFKA, "enable.idempotence": True, "linger.ms": 20})
    for lang in a.langs:
        n = 0
        for label, path, note, text in iter_sections(Path(a.dir) / f"cars_{lang}.xml"):
            if not label or not text:
                continue
            ev = section_event(lang, label, path, note, text)
            p.produce(config.TOPIC_DOCS, key=ev["doc_id"], value=json.dumps(ev))
            p.poll(0)
            n += 1
            if a.limit and n >= a.limit:
                break
        print(f"{lang}: produced {n} sections")
    p.flush()


if __name__ == "__main__":
    main()
