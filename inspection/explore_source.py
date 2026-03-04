"""Explore Global Building Atlas LOD1 source data on Source Cooperative.

Lists available tiles, samples rows, and reports schema/statistics.

Usage:
    uv run python inspection/explore_source.py
    uv run python inspection/explore_source.py --source /data/gba/
    uv run python inspection/explore_source.py --sample 5
"""

from __future__ import annotations

import argparse

import duckdb


SOURCE_S3_URI = "s3://us-west-2.opendata.source.coop/tge-labs/globalbuildingatlas-lod1"


def explore(con: duckdb.DuckDBPyConnection, source: str) -> None:
    """Run exploration queries on the GBA source data."""

    print("\n" + "=" * 70)
    print(f"SOURCE: {source}")
    print("=" * 70)

    if source.startswith("s3://"):
        # Parse S3 URI for DuckDB httpfs
        path = source.replace("s3://", "")
        host = path.split("/", 1)[0]
        rest = path.split("/", 1)[1]

        con.sql(f"SET s3_endpoint='{host}'")
        con.sql("SET s3_url_style='path'")

        glob_pattern = f"s3://{host}/{rest}/*.parquet"
    else:
        glob_pattern = f"{source}/*.parquet"

    # List files
    print("\n--- Source Files ---")
    try:
        result = con.sql(f"""
            SELECT filename, count(*) AS row_count
            FROM read_parquet('{glob_pattern}', filename=true,
                              hive_partitioning=false)
            GROUP BY filename
            ORDER BY filename
        """).fetchall()
        total_rows = 0
        for fname, count in result[:20]:
            print(f"  {fname}: {count:,} rows")
            total_rows += count
        if len(result) > 20:
            for _, count in result[20:]:
                total_rows += count
            print(f"  ... and {len(result) - 20} more files")
        print(f"\n  Total: {len(result)} files, {total_rows:,} rows")
    except Exception as e:
        print(f"  Error listing files: {e}")
        # Try single file
        print("  Trying single file read...")
        glob_pattern = f"{source}"

    # Schema
    print("\n--- Schema ---")
    try:
        result = con.sql(f"""
            DESCRIBE SELECT * FROM read_parquet('{glob_pattern}',
                                                hive_partitioning=false)
            LIMIT 0
        """).fetchall()
        for col_name, col_type, *_ in result:
            print(f"  {col_name:20s}  {col_type}")
    except Exception as e:
        print(f"  Error: {e}")

    # Summary statistics
    print("\n--- Summary Statistics ---")
    try:
        result = con.sql(f"""
            SELECT
                count(*) AS total_buildings,
                count_if(height != -999) AS with_height,
                count_if(height = -999) AS no_height,
                avg(CASE WHEN height != -999 THEN height END)::FLOAT
                    AS avg_height,
                median(CASE WHEN height != -999 THEN height END)::FLOAT
                    AS median_height,
                max(CASE WHEN height != -999 THEN height END)::FLOAT
                    AS max_height,
                min(CASE WHEN height != -999 THEN height END)::FLOAT
                    AS min_height
            FROM read_parquet('{glob_pattern}', hive_partitioning=false)
        """).fetchone()
        total, with_h, no_h, avg_h, med_h, max_h, min_h = result
        print(f"  Total buildings:   {total:,}")
        print(f"  With height:       {with_h:,} ({100 * with_h / total:.1f}%)")
        print(f"  No height (-999):  {no_h:,} ({100 * no_h / total:.1f}%)")
        print(f"  Height avg/med:    {avg_h:.1f} / {med_h:.1f} m")
        print(f"  Height range:      {min_h:.1f} – {max_h:.1f} m")
    except Exception as e:
        print(f"  Error: {e}")

    # Source distribution
    print("\n--- Source Distribution ---")
    try:
        result = con.sql(f"""
            SELECT source, count(*) AS n,
                   (count(*) * 100.0 /
                       sum(count(*)) OVER ())::FLOAT AS pct
            FROM read_parquet('{glob_pattern}', hive_partitioning=false)
            GROUP BY source
            ORDER BY n DESC
        """).fetchall()
        for src, n, pct in result:
            print(f"  {src:10s}  {n:>14,}  ({pct:.1f}%)")
    except Exception as e:
        print(f"  Error: {e}")

    # Region distribution (top 20)
    print("\n--- Top 20 Regions ---")
    try:
        result = con.sql(f"""
            SELECT region, count(*) AS n
            FROM read_parquet('{glob_pattern}', hive_partitioning=false)
            GROUP BY region
            ORDER BY n DESC
            LIMIT 20
        """).fetchall()
        for region, n in result:
            print(f"  {region:6s}  {n:>14,}")
    except Exception as e:
        print(f"  Error: {e}")


def sample_rows(con: duckdb.DuckDBPyConnection, source: str, n: int) -> None:
    """Show sample rows from the source data."""
    if source.startswith("s3://"):
        path = source.replace("s3://", "")
        host = path.split("/", 1)[0]
        glob_pattern = f"s3://{host}/{path.split('/', 1)[1]}/*.parquet"
    else:
        glob_pattern = f"{source}/*.parquet"

    print(f"\n--- Sample {n} Rows ---")
    try:
        # Exclude geometry blob for readability
        result = con.sql(f"""
            SELECT * EXCLUDE (geometry)
            FROM read_parquet('{glob_pattern}', hive_partitioning=false)
            USING SAMPLE {n}
        """)
        result.show()
    except Exception as e:
        print(f"  Error: {e}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Explore GBA source data")
    parser.add_argument(
        "--source",
        default=SOURCE_S3_URI,
        help=f"Source: local dir or S3 URI (default: {SOURCE_S3_URI})",
    )
    parser.add_argument(
        "--sample",
        type=int,
        default=5,
        help="Number of sample rows to show (default: 5)",
    )
    args = parser.parse_args()

    con = duckdb.connect()
    con.install_extension("spatial")
    con.load_extension("spatial")

    if args.source.startswith("s3://"):
        con.install_extension("httpfs")
        con.load_extension("httpfs")

    explore(con, args.source)
    sample_rows(con, args.source, args.sample)


if __name__ == "__main__":
    main()
