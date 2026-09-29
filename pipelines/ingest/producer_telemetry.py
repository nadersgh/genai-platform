"""Synthetic aircraft telemetry -> Kafka. Deterministic per --seed, includes injected faults
(missing readings, out-of-range values, late events) so the quality gates in week 4+ have
something to catch."""
import argparse
import json
import random
import time
from datetime import datetime, timedelta, timezone

from confluent_kafka import Producer

from . import config


def make_event(rng: random.Random, tail: str, t: datetime, state: dict) -> dict:
    s = state.setdefault(tail, {"lat": 45.5 + rng.random(), "lon": -73.6 + rng.random(),
                                "alt": 9000.0, "temp": 620.0})
    s["lat"] += rng.uniform(-0.01, 0.02)
    s["lon"] += rng.uniform(-0.02, 0.02)
    s["alt"] = max(0.0, s["alt"] + rng.uniform(-50, 50))
    s["temp"] += rng.uniform(-3, 3)
    ev = {"aircraft_id": tail, "event_time": t.isoformat(), "latitude": s["lat"],
          "longitude": s["lon"], "altitude_m": s["alt"],
          "speed_kts": rng.uniform(380, 480), "engine_temp_c": s["temp"]}
    roll = rng.random()
    if roll < 0.02:
        ev["engine_temp_c"] = 5000.0            # out-of-range sensor glitch
    elif roll < 0.04:
        ev["engine_temp_c"] = None              # missing reading (sparsity)
    return ev


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--aircraft", type=int, default=10)
    ap.add_argument("--events", type=int, default=1000, help="total events; 0 = run forever")
    ap.add_argument("--rate", type=float, default=200, help="events/sec")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--late-pct", type=float, default=0.03, help="fraction of events sent 1-6h late")
    a = ap.parse_args()

    rng = random.Random(a.seed)
    tails = [f"C-G{chr(65 + i // 26)}{chr(65 + i % 26)}{i:02d}" for i in range(a.aircraft)]
    p = Producer({"bootstrap.servers": config.KAFKA, "enable.idempotence": True, "linger.ms": 20})
    state: dict = {}
    now = datetime.now(timezone.utc)
    n, delay = 0, 1.0 / a.rate if a.rate > 0 else 0
    while a.events == 0 or n < a.events:
        tail = tails[n % len(tails)]
        t = now + timedelta(seconds=n // len(tails))
        if rng.random() < a.late_pct:
            t -= timedelta(hours=rng.uniform(1, 6))
        ev = make_event(rng, tail, t, state)
        p.produce(config.TOPIC_TELEMETRY, key=tail, value=json.dumps(ev))
        p.poll(0)
        n += 1
        if delay:
            time.sleep(delay)
    p.flush()
    print(f"produced {n} events to {config.TOPIC_TELEMETRY}")


if __name__ == "__main__":
    main()
