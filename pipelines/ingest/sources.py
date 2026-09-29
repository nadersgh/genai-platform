"""Registry of real public data sources. URLs/sizes verified with header-only requests on 2026-09-29.
Licences: check each publisher's terms before redistributing; this repo only downloads locally."""
from dataclasses import dataclass


@dataclass(frozen=True)
class Source:
    name: str
    url: str
    kind: str            # "docs" | "telemetry"
    tenant: str | None   # doc tenant, if kind == "docs"
    lang: str | None
    filename: str
    approx_mb: float
    licence: str


SOURCES = [
    Source("cars-en", "https://laws-lois.justice.gc.ca/eng/XML/SOR-96-433.xml", "docs",
           "tc-canada", "en", "cars_en.xml", 4.5, "Open Government Licence - Canada"),
    Source("cars-fr", "https://laws-lois.justice.gc.ca/fra/XML/DORS-96-433.xml", "docs",
           "tc-canada", "fr", "cars_fr.xml", 4.8, "Open Government Licence - Canada"),
    Source("cmapss", "https://phm-datasets.s3.amazonaws.com/NASA/6.+Turbofan+Engine+Degradation+Simulation+Data+Set.zip",
           "telemetry", None, None, "cmapss.zip", 12.4, "NASA PCoE public dataset"),
    Source("faa-amt-airframe",
           "https://www.faa.gov/regulations_policies/handbooks_manuals/aviation/FAA-H-8083-31B_Aviation_Maintenance_Technician_Handbook.pdf",
           "docs", "faa", "en", "faa_amt_airframe.pdf", 112.0, "US Government work (public domain)"),
]
BY_NAME = {s.name: s for s in SOURCES}
