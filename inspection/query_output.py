"""Query output Parquet files with DuckDB to verify correctness.

Usage:
    uv run python inspection/query_output.py
    uv run python inspection/query_output.py --output-dir /data/scratch/buildings/output
    uv run python inspection/query_output.py --s3 s3://bucket/prefix
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import duckdb


def run_queries(con: duckdb.DuckDBPyConnection, base_path: str) -> None:
    """Run diagnostic queries on the output Parquet files."""

    print("\n" + "=" * 70)
    print(f"OUTPUT: {base_path}")
    print("=" * 70)

    # List available files
    if base_path.startswith("s3://"):
        glob_pattern = f"{base_path}/**/data.parquet"
    else:
        files = sorted(Path(base_path).rglob("data.parquet"))
        if not files:
            print("No data.parquet files found!")
            return
        print(f"\nFound {len(files)} resolution files:")
        for f in files:
            size_mb = f.stat().st_size / 1e6
            print(f"  {f.relative_to(base_path)} ({size_mb:.1f} MB)")
        glob_pattern = str(Path(base_path) / "**" / "data.parquet")

    # Schema
    print("\n--- Schema ---")
    try:
        result = con.sql(
            f"DESCRIBE SELECT * FROM read_parquet('{glob_pattern}',"
            " hive_partitioning=false) LIMIT 0"
        ).fetchall()
        for col_name, col_type, *_ in result:
            print(f"  {col_name:30s}  {col_type}")
    except Exception as e:
        print(f"  Error: {e}")
        return

    # Per-resolution stats
    print("\n--- Per-Resolution Statistics ---")
    for f in sorted(Path(base_path).rglob("data.parquet")):
        rel = str(f.relative_to(base_path))
        fpath = str(f)

        result = con.sql(f"""
            SELECT
                count(*) AS n_cells,
                sum(building_count) AS total_buildings,
                min(lat) AS lat_min, max(lat) AS lat_max,
                min(lon) AS lon_min, max(lon) AS lon_max,
                avg(area_km2)::FLOAT AS avg_area_km2
            FROM read_parquet('{fpath}')
        """).fetchone()

        n_cells, total_bldg, lat_min, lat_max, lon_min, lon_max, avg_area = result
        print(f"\n  {rel}:")
        print(f"    Cells:          {n_cells:,}")
        print(f"    Buildings:      {total_bldg:,}")
        print(f"    Lat:            {lat_min:.2f} to {lat_max:.2f}")
        print(f"    Lon:            {lon_min:.2f} to {lon_max:.2f}")
        print(f"    Avg cell area:  {avg_area:.2f} km²")

        # Building metrics summary
        stats = con.sql(f"""
            SELECT
                avg(building_density)::FLOAT AS avg_density,
                max(building_density)::FLOAT AS max_density,
                avg(coverage_ratio)::FLOAT AS avg_coverage,
                max(coverage_ratio)::FLOAT AS max_coverage,
                avg(avg_height_m)::FLOAT AS avg_height,
                max(max_height_m)::FLOAT AS tallest,
                sum(total_volume_m3)::FLOAT AS total_volume,
                avg(avg_footprint_m2)::FLOAT AS avg_footprint
            FROM read_parquet('{fpath}')
        """).fetchone()
        (
            avg_dens,
            max_dens,
            avg_cov,
            max_cov,
            avg_h,
            tallest,
            total_vol,
            avg_fp,
        ) = stats
        print(f"    Density:        avg={avg_dens:.1f}  max={max_dens:.0f} /km²")
        print(f"    Coverage:       avg={avg_cov:.4f}  max={max_cov:.4f}")
        if avg_h:
            print(f"    Height:         avg={avg_h:.1f}m  tallest={tallest:.1f}m")
        if total_vol:
            print(f"    Total volume:   {total_vol:,.0f} m³")
        if avg_fp:
            print(f"    Avg footprint:  {avg_fp:.1f} m²")

    # Densest cells (first available file at highest resolution)
    parquet_files = sorted(Path(base_path).rglob("data.parquet"))
    last_file = parquet_files[-1]  # highest resolution
    fpath = str(last_file)

    print(f"\n--- Top 10 Densest Cells ({last_file.parent.name}) ---")
    result = con.sql(f"""
        SELECT h3_index, lat, lon, building_count, building_density,
               coverage_ratio, avg_height_m, max_height_m
        FROM read_parquet('{fpath}')
        ORDER BY building_density DESC
        LIMIT 10
    """).fetchall()
    print(
        f"  {'h3_index':20s} {'lat':>8s} {'lon':>9s} {'count':>7s}"
        f" {'dens/km²':>10s} {'coverage':>10s} {'avg_h':>7s} {'max_h':>7s}"
    )
    for row in result:
        h3_id, lat, lon, count, density, cov, avg_h, max_h = row
        avg_h_str = f"{avg_h:.1f}" if avg_h else "N/A"
        max_h_str = f"{max_h:.1f}" if max_h else "N/A"
        print(
            f"  {h3_id:20s} {lat:+8.3f} {lon:+9.3f} {count:7,}"
            f" {density:10,.1f} {cov:10.4f} {avg_h_str:>7s} {max_h_str:>7s}"
        )

    # Tallest buildings
    print(f"\n--- Top 10 Tallest Building Cells ({last_file.parent.name}) ---")
    result = con.sql(f"""
        SELECT h3_index, lat, lon, building_count, max_height_m,
               avg_height_m, total_volume_m3
        FROM read_parquet('{fpath}')
        WHERE max_height_m IS NOT NULL
        ORDER BY max_height_m DESC
        LIMIT 10
    """).fetchall()
    print(
        f"  {'h3_index':20s} {'lat':>8s} {'lon':>9s} {'count':>7s}"
        f" {'max_h':>8s} {'avg_h':>7s} {'volume_m³':>14s}"
    )
    for row in result:
        h3_id, lat, lon, count, max_h, avg_h, vol = row
        avg_h_str = f"{avg_h:.1f}" if avg_h else "N/A"
        print(
            f"  {h3_id:20s} {lat:+8.3f} {lon:+9.3f} {count:7,}"
            f" {max_h:8.1f} {avg_h_str:>7s} {vol:14,.0f}"
        )

    # Cross-resolution consistency check
    print("\n--- Cross-Resolution Consistency ---")
    totals = {}
    for f in parquet_files:
        fpath = str(f)
        res_name = f.parent.name
        result = con.sql(f"""
            SELECT sum(building_count)::BIGINT
            FROM read_parquet('{fpath}')
        """).fetchone()
        totals[res_name] = result[0]
        print(f"  {res_name}: {result[0]:,} total buildings")

    values = list(totals.values())
    if len(values) > 1:
        diff = max(values) - min(values)
        print(f"  Max diff across resolutions: {diff:,}")

    # GeoParquet metadata check
    print("\n--- GeoParquet Metadata Check ---")
    import pyarrow.parquet as pq

    pf = pq.ParquetFile(str(parquet_files[0]))
    meta = pf.schema_arrow.metadata or {}
    geo_meta = meta.get(b"geo")
    if geo_meta:
        geo = json.loads(geo_meta)
        print(f"  GeoParquet version: {geo.get('version', 'N/A')}")
        print(f"  Primary column:    {geo.get('primary_column', 'N/A')}")
        cols = geo.get("columns", {})
        for name, info in cols.items():
            print(f"  Column '{name}': encoding={info.get('encoding', 'N/A')}")
    else:
        print("  No GeoParquet 'geo' metadata found")

    # Check for GEOMETRY logical type
    for field in pf.schema_arrow:
        if "geometry" in field.name.lower():
            print(f"  Arrow field '{field.name}': type={field.type}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Query building index Parquet output")
    parser.add_argument(
        "--output-dir",
        default="/data/scratch/buildings/output/buildings",
        help="Local output directory",
    )
    parser.add_argument(
        "--s3",
        default="",
        help="S3 URI (e.g. s3://bucket/prefix/buildings)",
    )
    args = parser.parse_args()

    con = duckdb.connect()
    con.install_extension("spatial")
    con.load_extension("spatial")

    if args.s3:
        base_path = args.s3
        con.install_extension("httpfs")
        con.load_extension("httpfs")
        aws_key = os.environ.get("AWS_ACCESS_KEY_ID", "")
        aws_secret = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
        if aws_key:
            con.sql(f"SET s3_access_key_id='{aws_key}'")
            con.sql(f"SET s3_secret_access_key='{aws_secret}'")
    else:
        base_path = args.output_dir

    run_queries(con, base_path)


if __name__ == "__main__":
    main()
