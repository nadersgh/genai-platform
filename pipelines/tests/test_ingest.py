import random
from datetime import datetime, timezone
from pathlib import Path

import pytest

from ingest.producer_docs import doc_event
from ingest.producer_telemetry import make_event


def test_telemetry_is_deterministic():
    t = datetime(2026, 1, 1, tzinfo=timezone.utc)
    a = [make_event(random.Random(1), "X", t, {}) for _ in range(3)]
    b = [make_event(random.Random(1), "X", t, {}) for _ in range(3)]
    assert a == b


def test_doc_tenant_from_path(tmp_path: Path):
    (tmp_path / "acme").mkdir()
    f = tmp_path / "acme" / "a.md"
    f.write_text("# Title\nbody")
    ev = doc_event(tmp_path, f)
    assert ev["tenant"] == "acme" and ev["title"] == "Title"
    (tmp_path / "top.md").write_text("x")
    assert doc_event(tmp_path, tmp_path / "top.md")["tenant"] == "public"


def test_opensky_state_mapping():
    from ingest.producer_opensky import state_to_event
    s = ["abc123", "ACA123 ", "Canada", 1_700_000_000, 1_700_000_001, -73.7, 45.5, 10000.0,
         False, 230.0, 90.0, 0.0, None, 10100.0, "1000", False, 0]
    ev = state_to_event(s)
    assert ev["aircraft_id"] == "adsb:abc123" and ev["latitude"] == 45.5
    assert abs(ev["speed_kts"] - 447.0) < 1
    s[8] = True                       # on ground -> dropped
    assert state_to_event(s) is None


def test_cmapss_row_mapping():
    from datetime import datetime, timezone
    from ingest.producer_cmapss import row_to_event
    cols = ["1", "1", "0.0", "0.0"] + ["0"] * 22
    cols[2], cols[3], cols[7] = "10.0", "0.5", "1591.67"   # T30 = 1591.67 R -> ~611 C
    ev = row_to_event("FD001", cols, datetime(2026, 1, 1, tzinfo=timezone.utc))
    assert ev["aircraft_id"] == "cmapss:FD001-u001"
    assert abs(ev["altitude_m"] - 3048) < 1 and abs(ev["engine_temp_c"] - 611.1) < 0.5


def test_cars_extraction_keeps_structure(tmp_path):
    from ingest.producer_cars import iter_sections, section_event
    xml = tmp_path / "c.xml"
    xml.write_text(
        '<Regulation><Body>'
        '<Heading level="1"><Label>PART I</Label><TitleText>General</TitleText></Heading>'
        '<Heading level="2"><TitleText>Short Title</TitleText></Heading>'
        '<Section><Label>100.01</Label><MarginalNote>Scope</MarginalNote>'
        '<Subsection><Label>(1)</Label><Text>Applies to <XRefInternal>(2)</XRefInternal>.'
        '<FootnoteRef>a</FootnoteRef></Text></Subsection>'
        '<HistoricalNote>SOR/2019-1</HistoricalNote></Section>'
        '<Heading level="1"><TitleText>PART II</TitleText></Heading>'
        '<Section><Label>200.01</Label><Text>Other.</Text></Section>'
        '</Body></Regulation>')
    a, b = list(iter_sections(xml))
    assert a[0] == "100.01" and a[1] == "PART I General > Short Title" and a[2] == "Scope"
    assert "SOR/2019" not in a[3] and a[3].startswith("100.01 Scope (1) Applies to (2)")
    assert b[1] == "PART II"          # level-1 heading resets the trail
    ev = section_event("en", *a)
    assert ev["doc_id"] == "cars-en-100.01" and ev["lang"] == "en" and ev["title"] == "CAR 100.01 - Scope"


def test_delete_event_matches_upsert_identity(tmp_path: Path):
    from ingest.catalog import DocOp
    from ingest.producer_docs import delete_event
    (tmp_path / "acme").mkdir()
    (tmp_path / "acme" / "a.md").write_text("# T\nbody")

    up = doc_event(tmp_path, tmp_path / "acme" / "a.md")
    rm = delete_event(Path("acme/a.md"))

    assert (rm["doc_id"], rm["tenant"], rm["op"]) == (up["doc_id"], "acme", DocOp.DELETE)
    assert "content" not in rm and up["op"] == DocOp.UPSERT


class FakeMessage:
    def __init__(self, value: bytes | None, topic: str = "telemetry.raw"):
        self._value, self._topic = value, topic

    def value(self) -> bytes | None:
        return self._value

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return 0

    def offset(self) -> int:
        return 7


NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


def test_parse_message_routes_valid_json_and_keeps_raw():
    from ingest.sink_bronze import parse_message
    raw = b'{"aircraft_id": "X", "event_time": "2026-01-01T00:00:00+00:00"}'

    name, row = parse_message(FakeMessage(raw), NOW)

    assert name == "telemetry" and row["aircraft_id"] == "X"
    assert row["_raw"] == raw.decode() and row["_offset"] == 7


@pytest.mark.parametrize("raw", [b"not json", b"[1, 2]", None, b"\xff\xfe"])
def test_parse_message_rejects_undecodable_payloads(raw):
    from ingest.sink_bronze import parse_message

    name, row = parse_message(FakeMessage(raw), NOW)

    assert name == "rejects" and row["error"] and row["_offset"] == 7


def test_build_batch_isolates_bad_rows():
    from ingest.catalog import TELEMETRY_SCHEMA
    from ingest.sink_bronze import build_batch
    prov = {"_topic": "telemetry.raw", "_partition": 0, "_ingested_at": NOW, "_raw": "{}"}
    good = {"aircraft_id": "A", "event_time": "2026-01-01T00:00:00+00:00", "engine_temp_c": 600.0}
    rows = [
        {**prov, "_offset": 1, **good},
        {**prov, "_offset": 2, **good, "aircraft_id": None},           # required field missing
        {**prov, "_offset": 3, **good, "event_time": "yesterday"},     # unparseable timestamp
        {**prov, "_offset": 4, **good, "engine_temp_c": "hot"},        # wrong type
        {**prov, "_offset": 5, **good},
    ]

    batch, rejects = build_batch(rows, TELEMETRY_SCHEMA.as_arrow())

    assert batch.column("_offset").to_pylist() == [1, 5]
    assert sorted(r["_offset"] for r in rejects) == [2, 3, 4]
    assert all(r["error"] for r in rejects)


def test_rejects_fit_rejects_schema():
    import pyarrow as pa
    from ingest.catalog import REJECTS_SCHEMA, TELEMETRY_SCHEMA
    from ingest.sink_bronze import build_batch
    rows = [{"_topic": "t", "_partition": 0, "_offset": 1, "_ingested_at": NOW, "_raw": "{}",
             "aircraft_id": None, "event_time": "2026-01-01T00:00:00+00:00"}]
    _, rejects = build_batch(rows, TELEMETRY_SCHEMA.as_arrow())

    table = pa.Table.from_pylist(rejects, schema=REJECTS_SCHEMA.as_arrow())

    assert table.num_rows == 1 and table.column("error")[0].as_py()
