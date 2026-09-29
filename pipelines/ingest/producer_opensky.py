"""Live ADS-B (OpenSky) -> telemetry.raw. Default bbox = Montreal area (1 credit/call).
Anonymous quota is ~400 credits/day, so the default poll interval is conservative.
Optional auth: OPENSKY_CLIENT_ID / OPENSKY_CLIENT_SECRET (OAuth2 client-credentials);
OPENSKY_TOKEN_URL must be set to the token endpoint from the OpenSky docs."""
import argparse
import json
import os
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone

from confluent_kafka import Producer

from . import config

API = "https://opensky-network.org/api/states/all"
MS_TO_KTS = 1.943844


def get_token() -> str | None:
    cid, sec, url = (os.getenv(k) for k in ("OPENSKY_CLIENT_ID", "OPENSKY_CLIENT_SECRET", "OPENSKY_TOKEN_URL"))
    if not (cid and sec and url):
        return None
    body = urllib.parse.urlencode({"grant_type": "client_credentials", "client_id": cid,
                                   "client_secret": sec}).encode()
    with urllib.request.urlopen(urllib.request.Request(url, data=body), timeout=30) as r:
        return json.load(r)["access_token"]


def state_to_event(s: list) -> dict | None:
    """OpenSky state vector -> telemetry.raw event. Index layout per OpenSky REST docs."""
    icao24, lon, lat, baro_alt, on_ground, velocity, t_pos = s[0], s[5], s[6], s[7], s[8], s[9], s[3]
    if lon is None or lat is None or t_pos is None or on_ground:
        return None
    return {
        "aircraft_id": f"adsb:{icao24}",
        "event_time": datetime.fromtimestamp(t_pos, timezone.utc).isoformat(),
        "latitude": lat, "longitude": lon, "altitude_m": baro_alt,
        "speed_kts": velocity * MS_TO_KTS if velocity is not None else None,
        "engine_temp_c": None,  # ADS-B carries no engine data
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bbox", type=float, nargs=4, default=[45.0, -74.5, 46.0, -73.0],
                    metavar=("LAMIN", "LOMIN", "LAMAX", "LOMAX"))
    ap.add_argument("--interval", type=float, default=90, help="seconds between polls")
    ap.add_argument("--max-polls", type=int, default=5, help="0 = forever")
    a = ap.parse_args()

    p = Producer({"bootstrap.servers": config.KAFKA, "enable.idempotence": True})
    q = urllib.parse.urlencode(dict(zip(("lamin", "lomin", "lamax", "lomax"), a.bbox)))
    token, polls, seen = get_token(), 0, {}
    while a.max_polls == 0 or polls < a.max_polls:
        req = urllib.request.Request(f"{API}?{q}")
        if token:
            req.add_header("Authorization", f"Bearer {token}")
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                states = (json.load(r).get("states") or [])
        except Exception as e:  # rate limit / transient: log and keep going
            print("poll failed:", e)
            states = []
        n = 0
        for s in states:
            ev = state_to_event(s)
            # skip unchanged positions (same aircraft + same position timestamp)
            if ev and seen.get(ev["aircraft_id"]) != ev["event_time"]:
                seen[ev["aircraft_id"]] = ev["event_time"]
                p.produce(config.TOPIC_TELEMETRY, key=ev["aircraft_id"], value=json.dumps(ev))
                n += 1
        p.flush()
        polls += 1
        print(f"poll {polls}: {len(states)} states, {n} new events")
        if a.max_polls == 0 or polls < a.max_polls:
            time.sleep(a.interval)


if __name__ == "__main__":
    main()
