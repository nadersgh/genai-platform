"""Download public sources into data/raw/. Opt-in per source (no default), skips existing files,
records sha256 + url + licence in data/raw/MANIFEST.json."""
import argparse
import hashlib
import json
import shutil
import urllib.request
from pathlib import Path

from .sources import BY_NAME, SOURCES

RAW = Path("data/raw")
UA = {"User-Agent": "Mozilla/5.0 (genai-platform learning project)"}


def fetch(url: str, dest: Path) -> str:
    req = urllib.request.Request(url, headers=UA)
    h = hashlib.sha256()
    tmp = dest.with_suffix(dest.suffix + ".part")
    with urllib.request.urlopen(req, timeout=60) as r, open(tmp, "wb") as f:
        while chunk := r.read(1 << 20):
            f.write(chunk)
            h.update(chunk)
    shutil.move(tmp, dest)
    return h.hexdigest()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("names", nargs="*", help=f"one or more of: {', '.join(BY_NAME)}")
    ap.add_argument("--list", action="store_true")
    a = ap.parse_args()
    if a.list or not a.names:
        for s in SOURCES:
            print(f"{s.name:18} ~{s.approx_mb:6.1f} MB  {s.licence:36} {s.url}")
        return
    RAW.mkdir(parents=True, exist_ok=True)
    mf_path = RAW / "MANIFEST.json"
    mf = json.loads(mf_path.read_text()) if mf_path.exists() else {}
    for n in a.names:
        s = BY_NAME[n]
        dest = RAW / s.filename
        if dest.exists():
            print(f"skip {n}: {dest} exists")
            continue
        print(f"downloading {n} (~{s.approx_mb} MB) ...")
        mf[n] = {"url": s.url, "file": s.filename, "sha256": fetch(s.url, dest),
                 "licence": s.licence, "bytes": dest.stat().st_size}
        mf_path.write_text(json.dumps(mf, indent=2))
        print(f"  ok {dest} ({dest.stat().st_size / 1e6:.1f} MB)")


if __name__ == "__main__":
    main()
