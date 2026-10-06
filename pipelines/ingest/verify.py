"""Row counts, duplicate check and basic quality stats on bronze tables (Polars, lazy)."""
import polars as pl

from .catalog import REJECTS, scan

PROV = ["_topic", "_partition", "_offset"]


def main() -> None:
    for name in ("telemetry", "docs"):
        lf = scan(name)
        s = lf.select(rows=pl.len(), distinct=pl.struct(PROV).n_unique()).collect().row(0, named=True)
        print(f"bronze.{name}: rows={s['rows']} distinct_offsets={s['distinct']} dupes={s['rows'] - s['distinct']}")

    lf = scan("telemetry")
    print("\ntelemetry quality by source:")
    print(
        lf.with_columns(source=pl.col("aircraft_id").str.extract(r"^(\w+):", 1).fill_null("synthetic"))
        .group_by("source")
        .agg(
            rows=pl.len(),
            aircraft=pl.col("aircraft_id").n_unique(),
            temp_null_pct=(pl.col("engine_temp_c").is_null().mean() * 100).round(1),
            temp_out_of_range=(pl.col("engine_temp_c") > 1500).sum(),
            first=pl.col("event_time").min(),
            last=pl.col("event_time").max(),
        )
        .sort("source")
        .collect()
    )

    print(f"\n{REJECTS} by topic:")
    print(
        scan(REJECTS)
        .group_by("_topic")
        .agg(rows=pl.len(), sample_error=pl.col("error").first())
        .sort("_topic")
        .collect()
    )


if __name__ == "__main__":
    main()
