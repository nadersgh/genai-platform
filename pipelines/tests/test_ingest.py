import random
from datetime import datetime, timezone
from pathlib import Path

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
