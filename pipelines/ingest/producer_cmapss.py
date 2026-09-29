"""Replay NASA C-MAPSS (turbofan run-to-failure) through telemetry.raw.
Mapping is approximate and documented: setting1=altitude(kft), setting2=Mach, sensor3=T30 HPC outlet (degR).
Each cycle is one event, spaced 1h apart from --start so engines age in "wall-clock" time."""
import argparse
import json
import time
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

from confluent_kafka import Producer

from . import config

MACH_KTS = 661.5   # ~speed of sound at sea level; good enough for a demo mapping


def row_to_event(dataset: str, cols: list[str], start: datetime) -> dict:
    unit, cycle = int(cols[0]), int(cols[1])
    alt_kft, mach, t30_r = float(cols[2]), float(cols[3]), float(cols[7])
    return {
        "aircraft_id": f"cmapss:{dataset}-u{unit:03d}",
        "event_time": (start + timedelta(hours=cycle)).isoformat(),
        "latitude": None, "longitude": None,
        "altitude_m": alt_kft * 304.8, "speed_kts": mach * MACH_KTS,
        "engine_temp_c": (t30_r - 491.67) / 1.8,   # Rankine -> Celsius
    }


def open_train(zip_path: Path, dataset: str):
    zf = zipfile.ZipFile(zip_path)
    inner = next((n for n in zf.namelist() if n.endswith(".zip")), None)
    if inner:  # the NASA archive nests a second zip
        import io
        zf = zipfile.ZipFile(io.BytesIO(zf.read(inner)))
    name = next(n for n in zf.namelist() if n.endswith(f"train_{dataset}.txt"))
    return zf.read(name).decode().splitlines()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--zip", default="data/raw/cmapss.zip")
    ap.add_argument("--dataset", default="FD001")
    ap.add_argument("--limit", type=int, default=0, help="max rows; 0 = all")
    ap.add_argument("--rate", type=float, default=0, help="rows/sec; 0 = as fast as possible")
    a = ap.parse_args()

    start = datetime.now(timezone.utc) - timedelta(days=400)
    p = Producer({"bootstrap.servers": config.KAFKA, "enable.idempotence": True, "linger.ms": 20})
    n = 0
    for line in open_train(Path(a.zip), a.dataset):
        cols = line.split()
        if len(cols) < 26:
            continue
        ev = row_to_event(a.dataset, cols, start)
        p.produce(config.TOPIC_TELEMETRY, key=ev["aircraft_id"], value=json.dumps(ev))
        p.poll(0)
        n += 1
        if a.limit and n >= a.limit:
            break
        if a.rate:
            time.sleep(1 / a.rate)
    p.flush()
    print(f"replayed {n} rows of {a.dataset} to {config.TOPIC_TELEMETRY}")


if __name__ == "__main__":
    main()
