"""
GBA LOD1 → Pyramid GeoParquet Pipeline (v3 — in-memory, fully parallel)

Converts Global Building Atlas LOD1 (2.75B buildings, 210 GB Parquet) into a
Pyramid GeoParquet file (Yosegi-style) using DuckDB with native Parquet 2.11+
GEOMETRY and GEOPARQUET_VERSION 'BOTH'.

v3 redesign:
  - In-memory DuckDB (708 GB RAM, no persistent DB overhead)
  - Phase A: COPY centroids to Parquet using filename + file_row_number
    (no row_number() OVER (), fully parallel)
  - Phase B: Load centroids into in-memory table, iterative UPDATE for zoom
    assignment (fast RAM scans, no WAL/checkpoint overhead)
  - Phase C: Export assignments to Parquet, join with source, write final output
  - preserve_insertion_order=false for full parallelism
"""

import argparse
import json
import logging
import time
from pathlib import Path

import duckdb

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


def fmt_duration(seconds: float) -> str:
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    if h:
        return f"{h}h {m}m {s}s"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"


def setup_connection(
    spill_dir: Path, memory_limit: str, threads: int
) -> duckdb.DuckDBPyConnection:
    """Create an in-memory DuckDB connection optimized for throughput."""
    spill_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial")
    con.execute(f"SET temp_directory='{spill_dir}'")
    con.execute(f"SET memory_limit='{memory_limit}'")
    con.execute(f"SET threads={threads}")
    con.execute("SET preserve_insertion_order=false")
    log.info(
        "DuckDB connected: in-memory, threads=%d, memory=%s", threads, memory_limit
    )
    return con


def phase_a_extract_centroids(
    con: duckdb.DuckDBPyConnection, source: Path, scratch: Path
) -> Path:
    """Phase A: Extract centroids with file+row identity to temp Parquet.

    Uses DuckDB's filename and file_row_number virtual columns for a
    deterministic join key — no row_number() OVER (), fully parallel.
    """
    centroids_path = scratch / "centroids.parquet"
    if centroids_path.exists():
        count = con.execute(
            f"SELECT count(*) FROM read_parquet('{centroids_path}')"
        ).fetchone()[0]
        log.info("Phase A: SKIP (centroids exist with %s rows)", f"{count:,}")
        return centroids_path

    log.info("Phase A: Extracting centroids from %s", source)
    t0 = time.time()

    glob_pattern = str(source / "*.parquet")

    con.execute(f"""
        COPY (
            SELECT
                filename,
                file_row_number,
                ST_Centroid(geometry::GEOMETRY) AS rep_point
            FROM read_parquet(
                '{glob_pattern}',
                hive_partitioning=false,
                filename=true,
                file_row_number=true
            )
        )
        TO '{centroids_path}'
        (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 1,
         ROW_GROUP_SIZE 1000000)
    """)

    count = con.execute(
        f"SELECT count(*) FROM read_parquet('{centroids_path}')"
    ).fetchone()[0]
    elapsed = time.time() - t0
    log.info("Phase A: DONE — %s rows in %s", f"{count:,}", fmt_duration(elapsed))
    return centroids_path


def phase_b_assign_zoom(
    con: duckdb.DuckDBPyConnection,
    centroids_path: Path,
    scratch: Path,
    minzoom: int,
    maxzoom: int,
) -> Path:
    """Phase B: Zoom level assignment via spatial thinning.

    Loads centroids into an in-memory table, then iteratively assigns zoom
    levels using GROUP BY ST_ReducePrecision. In-memory UPDATEs are fast
    (no WAL/checkpoint overhead). Each zoom scans only unassigned rows.
    Finally exports assignments to Parquet for Phase C join.
    """
    assignments_path = scratch / "zoom_assignments.parquet"
    if assignments_path.exists():
        count = con.execute(
            f"SELECT count(*) FROM read_parquet('{assignments_path}')"
        ).fetchone()[0]
        log.info("Phase B: SKIP (assignments exist with %s rows)", f"{count:,}")
        return assignments_path

    log.info("Phase B: Loading centroids into memory...")
    t0 = time.time()

    con.execute(f"""
        CREATE TABLE centroids AS
        SELECT filename, file_row_number, rep_point
        FROM read_parquet('{centroids_path}')
    """)

    count = con.execute("SELECT count(*) FROM centroids").fetchone()[0]
    log.info(
        "Phase B: Loaded %s rows in %s",
        f"{count:,}",
        fmt_duration(time.time() - t0),
    )

    # Add zoomlevel column
    con.execute("ALTER TABLE centroids ADD COLUMN zoomlevel INTEGER")

    log.info("Phase B: Assigning zoom levels %d–%d", minzoom, maxzoom)

    for z in range(minzoom, maxzoom):
        tz = time.time()
        precision = 360.0 / (2**z)

        # Find one representative per grid cell from unassigned rows
        # Uses DuckDB's built-in rowid pseudocolumn (no row_number needed)
        con.execute(f"""
            UPDATE centroids
            SET zoomlevel = {z}
            WHERE zoomlevel IS NULL
              AND rowid IN (
                SELECT min(rowid)
                FROM centroids
                WHERE zoomlevel IS NULL
                GROUP BY ST_ReducePrecision(rep_point, {precision})
              )
        """)

        assigned = con.execute(
            f"SELECT count(*) FROM centroids WHERE zoomlevel = {z}"
        ).fetchone()[0]
        remaining = con.execute(
            "SELECT count(*) FROM centroids WHERE zoomlevel IS NULL"
        ).fetchone()[0]

        log.info(
            "Phase B: zoom=%d precision=%.6f assigned=%s remaining=%s elapsed=%s",
            z,
            precision,
            f"{assigned:,}",
            f"{remaining:,}",
            fmt_duration(time.time() - tz),
        )

    # All remaining → maxzoom
    tz = time.time()
    con.execute(f"UPDATE centroids SET zoomlevel = {maxzoom} WHERE zoomlevel IS NULL")
    assigned = con.execute(
        f"SELECT count(*) FROM centroids WHERE zoomlevel = {maxzoom}"
    ).fetchone()[0]
    log.info(
        "Phase B: zoom=%d (final) assigned=%s elapsed=%s",
        maxzoom,
        f"{assigned:,}",
        fmt_duration(time.time() - tz),
    )

    # Log distribution
    dist = con.execute(
        "SELECT zoomlevel, count(*) FROM centroids GROUP BY zoomlevel ORDER BY zoomlevel"
    ).fetchall()
    for z, c in dist:
        log.info("  zoom %2d: %s", z, f"{c:,}")

    # Export assignments to Parquet (drop rep_point — not needed for join)
    log.info("Phase B: Exporting assignments to %s", assignments_path)
    con.execute(f"""
        COPY (
            SELECT filename, file_row_number, zoomlevel
            FROM centroids
        )
        TO '{assignments_path}'
        (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 1,
         ROW_GROUP_SIZE 1000000)
    """)

    # Free memory
    con.execute("DROP TABLE centroids")

    elapsed = time.time() - t0
    log.info("Phase B: DONE in %s", fmt_duration(elapsed))
    return assignments_path


def _write_zoom_query(
    con: duckdb.DuckDBPyConnection,
    glob_pattern: str,
    assignments_path: Path,
    out_file: Path,
    maxzoom: int,
    zoom_filter: str,
) -> int:
    """Execute a single COPY TO for a zoom level (or sub-partition)."""
    con.execute(f"""
        COPY (
            SELECT
                ST_QuadKey(ST_Centroid(s.geometry::GEOMETRY), {maxzoom}) AS quadkey,
                s.source,
                s.id,
                s.height,
                s.var,
                s.region,
                s.geometry::GEOMETRY::GEOMETRY('EPSG:4326') AS geometry
            FROM read_parquet(
                '{glob_pattern}',
                hive_partitioning=false,
                filename=true,
                file_row_number=true
            ) s
            INNER JOIN read_parquet('{assignments_path}') a
                ON s.filename = a.filename
                AND s.file_row_number = a.file_row_number
            WHERE {zoom_filter}
            ORDER BY quadkey
        )
        TO '{out_file}'
        (FORMAT PARQUET,
         COMPRESSION ZSTD, COMPRESSION_LEVEL 3,
         ROW_GROUP_SIZE 131072, GEOPARQUET_VERSION 'BOTH')
    """)
    return con.execute(f"SELECT count(*) FROM read_parquet('{out_file}')").fetchone()[0]


# 2-char quadkey prefixes — splits world into 16 spatial quadrants
QUADKEY_PREFIXES_16 = [f"{a}{b}" for a in "0123" for b in "0123"]


def phase_c_write(
    con: duckdb.DuckDBPyConnection,
    source: Path,
    assignments_path: Path,
    output: Path,
    minzoom: int,
    maxzoom: int,
    large_zoom_threshold: int = 100_000_000,
) -> None:
    """Phase C: Final write joining assignments with full source geometry.

    Writes one zoom level at a time. For large zoom levels (>100M rows),
    splits by quadkey prefix into 16 spatial quadrants to avoid OOM on sort.
    """
    log.info("Phase C: Writing pyramid GeoParquet to %s", output)
    output.mkdir(parents=True, exist_ok=True)
    t0 = time.time()

    glob_pattern = str(source / "*.parquet")

    # Get row counts per zoom level
    zoom_counts = dict(
        con.execute(f"""
            SELECT zoomlevel, count(*)
            FROM read_parquet('{assignments_path}')
            GROUP BY zoomlevel
        """).fetchall()
    )

    for z in range(minzoom, maxzoom + 1):
        tz = time.time()
        zoom_dir = output / f"zoomlevel={z}"
        zoom_dir.mkdir(parents=True, exist_ok=True)
        row_count = zoom_counts.get(z, 0)

        if row_count <= large_zoom_threshold:
            # Small zoom: single file
            out_file = zoom_dir / "data.parquet"
            if out_file.exists():
                log.info("Phase C: zoom=%d SKIP (exists)", z)
                continue

            count = _write_zoom_query(
                con,
                glob_pattern,
                assignments_path,
                out_file,
                maxzoom,
                f"a.zoomlevel = {z}",
            )
            size_mb = out_file.stat().st_size / (1024 * 1024)
            log.info(
                "Phase C: zoom=%d rows=%s size=%.0fMB elapsed=%s",
                z,
                f"{count:,}",
                size_mb,
                fmt_duration(time.time() - tz),
            )
        else:
            # Large zoom: split into 16 quadrants by quadkey prefix
            log.info(
                "Phase C: zoom=%d has %s rows — splitting into 16 quadrants",
                z,
                f"{row_count:,}",
            )
            total_count = 0
            total_size = 0
            for prefix in QUADKEY_PREFIXES_16:
                tp = time.time()
                out_file = zoom_dir / f"data_{prefix}.parquet"
                if out_file.exists():
                    log.info("Phase C: zoom=%d quad=%s SKIP (exists)", z, prefix)
                    existing = con.execute(
                        f"SELECT count(*) FROM read_parquet('{out_file}')"
                    ).fetchone()[0]
                    total_count += existing
                    total_size += out_file.stat().st_size
                    continue

                # Compute quadkey inline and filter by prefix
                # The WHERE filters both on zoomlevel AND quadkey prefix
                count = _write_zoom_query(
                    con,
                    glob_pattern,
                    assignments_path,
                    out_file,
                    maxzoom,
                    f"a.zoomlevel = {z} AND ST_QuadKey(ST_Centroid(s.geometry::GEOMETRY), {maxzoom}) LIKE '{prefix}%'",
                )
                size_mb = out_file.stat().st_size / (1024 * 1024)
                total_count += count
                total_size += out_file.stat().st_size
                log.info(
                    "Phase C: zoom=%d quad=%s rows=%s size=%.0fMB elapsed=%s",
                    z,
                    prefix,
                    f"{count:,}",
                    size_mb,
                    fmt_duration(time.time() - tp),
                )

            log.info(
                "Phase C: zoom=%d DONE total_rows=%s total_size=%.0fMB elapsed=%s",
                z,
                f"{total_count:,}",
                total_size / (1024 * 1024),
                fmt_duration(time.time() - tz),
            )

    elapsed = time.time() - t0
    log.info("Phase C: DONE in %s", fmt_duration(elapsed))


def phase_d_metadata(output: Path, source: Path, minzoom: int, maxzoom: int) -> None:
    """Phase D: Write metadata JSON."""
    log.info("Phase D: Writing metadata")

    metadata = {
        "type": "pyramid_geoparquet",
        "source": "Global Building Atlas LOD1",
        "source_path": str(source),
        "algorithm": "yosegi_spatial_thinning",
        "minzoom": minzoom,
        "maxzoom": maxzoom,
        "quadkey_zoom": maxzoom,
        "sort_order": ["zoomlevel", "quadkey"],
        "partitioning": {"column": "zoomlevel", "type": "hive"},
        "compression": "zstd",
        "compression_level": 3,
        "row_group_size": 131072,
        "geoparquet_version": "BOTH",
        "crs": "EPSG:4326",
        "columns": {
            "zoomlevel": "Zoom level at which feature first appears (pyramid index)",
            "quadkey": f"Quadkey at zoom {maxzoom} (spatial sort key)",
            "source": "Data source identifier (original)",
            "id": "Building identifier (original)",
            "height": "Building height in meters, -999 = nodata (original)",
            "var": "Height variance (original)",
            "region": "Region code (original)",
            "geometry": "Original POLYGON, native Parquet 2.11+ GEOMETRY (original)",
        },
    }

    meta_path = output / "_metadata.json"
    meta_path.write_text(json.dumps(metadata, indent=2))
    log.info("Phase D: Wrote %s", meta_path)


def run_validation(output: Path) -> None:
    """Run post-write validation queries."""
    log.info("Validation: Running queries...")

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial")

    # Use ** to match both data.parquet and data_XX.parquet
    glob_pattern = str(output / "zoomlevel=*" / "*.parquet")

    total = con.execute(f"""
        SELECT count(*) FROM read_parquet('{glob_pattern}', hive_partitioning=true)
    """).fetchone()[0]
    log.info("Validation: Total rows = %s", f"{total:,}")

    dist = con.execute(f"""
        SELECT zoomlevel, count(*)
        FROM read_parquet('{glob_pattern}', hive_partitioning=true)
        GROUP BY zoomlevel ORDER BY zoomlevel
    """).fetchall()
    for z, c in dist:
        log.info("  zoom %2d: %s", z, f"{c:,}")

    geom_types = con.execute(f"""
        SELECT ST_GeometryType(geometry), count(*)
        FROM read_parquet('{glob_pattern}', hive_partitioning=true)
        GROUP BY ST_GeometryType(geometry)
        LIMIT 5
    """).fetchall()
    log.info("Validation: Geometry types: %s", geom_types)
    con.close()


def run_dry_run(source: Path) -> None:
    """Show file count and estimated rows without processing."""
    files = sorted(source.glob("*.parquet"))
    log.info("Dry run: %d source files found in %s", len(files), source)
    for f in files[:10]:
        log.info("  %s", f)
    if len(files) > 10:
        log.info("  ... and %d more", len(files) - 10)


def main() -> None:
    parser = argparse.ArgumentParser(description="GBA LOD1 → Pyramid GeoParquet")
    parser.add_argument(
        "--source", type=Path, required=True, help="Source Parquet directory"
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("/data/output/gba_pyramid"),
        help="Output directory",
    )
    parser.add_argument(
        "--scratch",
        type=Path,
        default=Path("/data/scratch/gba_pyramid"),
        help="Scratch directory for temp files",
    )
    parser.add_argument("--minzoom", type=int, default=2)
    parser.add_argument("--maxzoom", type=int, default=14)
    parser.add_argument("--memory-limit", default="500GB")
    parser.add_argument("--threads", type=int, default=180)
    parser.add_argument(
        "--dry-run", action="store_true", help="Preview files without processing"
    )
    parser.add_argument("--validate", action="store_true", help="Run validation only")
    args = parser.parse_args()

    if args.dry_run:
        run_dry_run(args.source)
        return

    if args.validate:
        run_validation(args.output)
        return

    t_total = time.time()
    log.info("=== GBA LOD1 → Pyramid GeoParquet Pipeline (v3) ===")
    log.info("Source: %s", args.source)
    log.info("Output: %s", args.output)
    log.info("Scratch: %s", args.scratch)
    log.info("Zoom range: %d–%d", args.minzoom, args.maxzoom)

    args.scratch.mkdir(parents=True, exist_ok=True)
    spill_dir = args.scratch / "spill"
    con = setup_connection(spill_dir, args.memory_limit, args.threads)

    # Phase A: Extract centroids to temp Parquet
    centroids_path = phase_a_extract_centroids(con, args.source, args.scratch)

    # Phase B: Assign zoom levels (in-memory)
    assignments_path = phase_b_assign_zoom(
        con, centroids_path, args.scratch, args.minzoom, args.maxzoom
    )

    # Phase C: Final write with full source geometry (per zoom level to avoid OOM)
    phase_c_write(
        con, args.source, assignments_path, args.output, args.minzoom, args.maxzoom
    )

    # Phase D: Metadata
    phase_d_metadata(args.output, args.source, args.minzoom, args.maxzoom)

    con.close()

    # Validation
    run_validation(args.output)

    log.info("=== Pipeline complete in %s ===", fmt_duration(time.time() - t_total))


if __name__ == "__main__":
    main()
