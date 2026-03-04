"""Global Building Atlas LOD1 → H3-indexed GeoParquet pipeline.

Converts Global Building Atlas LOD1 building footprints (2.75B buildings,
210 GB Parquet on Source Cooperative) to H3-indexed GeoParquet files with
per-cell urban density metrics.

Optimized for high-core-count machines (100+ threads). Uses multiprocessing
for true CPU parallelism across source tiles.

Output follows the walkthru-earth index format:
  buildings/h3_res={N}/data.parquet

Each Parquet file contains:
  h3_index             - H3 cell ID (hex string)
  geometry             - Cell center POINT, native Parquet 2.11+ GEOMETRY('EPSG:4326')
  lat, lon             - Cell center coordinates
  area_km2             - H3 cell area in km²
  building_count       - Number of buildings
  building_density     - Buildings per km²
  total_footprint_m2   - Sum of building footprint areas (m²)
  coverage_ratio       - Footprint coverage fraction (0–1)
  avg_height_m         - Mean building height (valid only)
  max_height_m         - Tallest building (m)
  height_std_m         - Height standard deviation
  total_volume_m3      - Sum(footprint × height) for valid-height buildings
  volume_density_m3_per_km2 - Built volume per km²
  avg_footprint_m2     - Mean building footprint size (m²)

Usage:
    uv run python main.py                                    # Full pipeline from S3
    uv run python main.py --source /data/gba/                # From local directory
    uv run python main.py --resolutions 5,6,7                # Specific resolutions
    uv run python main.py --workers 178                      # Explicit worker count
    uv run python main.py --scratch-dir /data/scratch/bldg   # Override scratch
    uv run python main.py --dry-run                          # Preview without processing
"""

from __future__ import annotations

import argparse
import json
import logging
import multiprocessing as mp
import os
import resource
import shutil
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import duckdb
import h3
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


def _mem_gb() -> str:
    """Current RSS memory in GB (for log lines)."""
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform == "darwin":
        return f"{rss / 1e9:.1f}GB"
    return f"{rss / 1e6:.1f}GB"


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SOURCE_S3_URI = "s3://us-west-2.opendata.source.coop/tge-labs/globalbuildingatlas-lod1"
HEIGHT_NODATA = -999.0
DEG_TO_M = 111_320.0  # meters per degree at equator

# Paths (overridable via CLI or env)
SCRATCH_DIR = Path(os.environ.get("SCRATCH_DIR", "/data/scratch/buildings"))
CHECKPOINT_FILE = SCRATCH_DIR / "checkpoint.json"

# S3 output (optional)
S3_BUCKET = os.environ.get("S3_BUCKET", "")
S3_PREFIX = os.environ.get("S3_PREFIX", "").strip("/")
AWS_REGION = os.environ.get("AWS_REGION", "") or os.environ.get(
    "AWS_DEFAULT_REGION", "us-east-1"
)


# ---------------------------------------------------------------------------
# Phase 1: Source tile listing
# ---------------------------------------------------------------------------


def _parse_s3_uri(uri: str) -> tuple[str, str, str]:
    """Parse s3://endpoint/bucket/prefix → (endpoint_url, bucket, prefix)."""
    path = uri.replace("s3://", "")
    parts = path.split("/", 2)
    endpoint = f"https://{parts[0]}"
    bucket = parts[1]
    prefix = parts[2] if len(parts) > 2 else ""
    return endpoint, bucket, prefix


def list_source_tiles(source: str) -> list[str]:
    """List Parquet tile paths from source (S3 URI or local directory)."""
    if source.startswith("s3://"):
        import botocore
        import botocore.config
        import boto3

        endpoint, bucket, prefix = _parse_s3_uri(source)
        s3 = boto3.client(
            "s3",
            endpoint_url=endpoint,
            config=botocore.config.Config(
                signature_version=botocore.UNSIGNED,
            ),
            region_name="us-west-2",
        )
        tiles = []
        paginator = s3.get_paginator("list_objects_v2")
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if not prefix.endswith("/"):
            kwargs["Prefix"] = prefix + "/"
        for page in paginator.paginate(**kwargs):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".parquet"):
                    # Reconstruct full S3 URI
                    host = source.replace("s3://", "").split("/")[0]
                    tiles.append(f"s3://{host}/{bucket}/{obj['Key']}")
        log.info("Found %d source tiles from S3", len(tiles))
        return sorted(tiles)
    else:
        tiles = sorted(str(p) for p in Path(source).glob("*.parquet"))
        log.info("Found %d source tiles from %s", len(tiles), source)
        return tiles


def _read_tile(tile_path: str, columns: list[str]) -> pa.Table:
    """Read a Parquet tile from local path or S3."""
    if tile_path.startswith("s3://"):
        from pyarrow.fs import S3FileSystem

        path = tile_path.replace("s3://", "")
        host = path.split("/", 1)[0]
        key = path.split("/", 1)[1]
        fs = S3FileSystem(
            anonymous=True,
            endpoint_override=f"https://{host}",
            region="us-west-2",
        )
        return pq.read_table(key, filesystem=fs, columns=columns)
    else:
        return pq.read_table(tile_path, columns=columns)


def _tile_id_from_path(tile_path: str) -> str:
    """Extract tile identifier from path (filename without extension)."""
    return Path(tile_path).stem


# ---------------------------------------------------------------------------
# Phase 2: Worker — process one source tile
# ---------------------------------------------------------------------------


def _process_tile(task: dict) -> tuple[str, str, dict[int, int]]:
    """Process one source tile for all H3 resolutions.

    For each building: compute centroid, footprint area, assign H3 cell,
    then groupby-aggregate metrics per cell.
    Handles tile-boundary duplicates via final DuckDB merge.

    Returns (tile_id, status, {h3_res: n_unique_cells}).
    """
    import shapely

    tile_id: str = task["tile_id"]
    tile_path: str = task["tile_path"]
    h3_resolutions: list[int] = task["h3_resolutions"]
    temp_dir_str: str = task["temp_dir"]

    try:
        # Read only geometry + height columns
        table = _read_tile(tile_path, ["geometry", "height"])
        n_buildings = len(table)
        if n_buildings == 0:
            return tile_id, "skipped_empty", {}

        # Extract raw data and free table
        geom_wkb = table.column("geometry").to_pylist()
        heights = table.column("height").to_numpy().astype(np.float32)
        del table

        # Parse WKB geometries
        geoms = shapely.from_wkb(geom_wkb)
        del geom_wkb

        # Compute centroids → coordinates
        centroids = shapely.centroid(geoms)
        coords = shapely.get_coordinates(centroids)
        lons = coords[:, 0].astype(np.float64)
        lats = coords[:, 1].astype(np.float64)
        del centroids, coords

        # Footprint areas in m² (cos-lat approximation for geographic coords)
        areas_deg2 = shapely.area(geoms)
        del geoms
        cos_lat = np.cos(np.radians(lats))
        areas_m2 = (areas_deg2 * (DEG_TO_M**2) * cos_lat).astype(np.float32)
        del areas_deg2, cos_lat

        # Valid height mask (exclude nodata sentinel)
        valid_height = heights != HEIGHT_NODATA

        cells_written: dict[int, int] = {}

        for h3_res in h3_resolutions:
            # H3 cell assignment for each building centroid
            h3_cells = np.empty(n_buildings, dtype=object)
            for i in range(n_buildings):
                h3_cells[i] = h3.latlng_to_cell(lats[i], lons[i], h3_res)

            unique_cells, inverse = np.unique(h3_cells, return_inverse=True)
            n_unique = len(unique_cells)
            del h3_cells

            # Cell center coordinates and area
            cell_lats = np.empty(n_unique, dtype=np.float32)
            cell_lons = np.empty(n_unique, dtype=np.float32)
            cell_areas = np.empty(n_unique, dtype=np.float32)
            for j, cell_id in enumerate(unique_cells):
                lat, lon = h3.cell_to_latlng(cell_id)
                cell_lats[j] = lat
                cell_lons[j] = lon
                cell_areas[j] = h3.cell_area(cell_id, unit="km^2")

            # --- Aggregates ---
            building_count = np.bincount(inverse, minlength=n_unique).astype(np.int32)
            total_footprint = np.bincount(
                inverse, weights=areas_m2, minlength=n_unique
            ).astype(np.float32)

            # Height aggregates (valid heights only)
            h_valid = np.where(valid_height, heights.astype(np.float64), 0.0)
            h_mask = valid_height.astype(np.float64)
            height_sum = np.bincount(inverse, weights=h_valid, minlength=n_unique)
            height_count = np.bincount(
                inverse, weights=h_mask, minlength=n_unique
            ).astype(np.int32)
            height_sq_sum = np.bincount(
                inverse,
                weights=np.where(valid_height, heights.astype(np.float64) ** 2, 0.0),
                minlength=n_unique,
            )

            # Max height per cell (use -inf for missing, convert later)
            max_heights = np.full(n_unique, -np.inf, dtype=np.float32)
            valid_h = np.where(valid_height, heights, -np.inf).astype(np.float32)
            np.maximum.at(max_heights, inverse, valid_h)
            # Cells with no valid heights → NaN
            max_heights[np.isneginf(max_heights)] = np.nan

            # Volume: sum(footprint_m² × height) for valid-height buildings
            vol_weights = np.where(
                valid_height, areas_m2.astype(np.float64) * heights, 0.0
            )
            total_volume = np.bincount(inverse, weights=vol_weights, minlength=n_unique)

            # Write temp Parquet
            out_dir = Path(temp_dir_str) / f"h3_res={h3_res}"
            out_dir.mkdir(parents=True, exist_ok=True)
            pa_table = pa.table(
                {
                    "h3_index": unique_cells,
                    "lat": cell_lats,
                    "lon": cell_lons,
                    "area_km2": cell_areas,
                    "building_count": building_count,
                    "total_footprint_m2": total_footprint,
                    "height_sum": height_sum.astype(np.float32),
                    "height_count": height_count,
                    "height_sq_sum": height_sq_sum.astype(np.float32),
                    "max_height_m": max_heights,
                    "total_volume_m3": total_volume.astype(np.float32),
                }
            )
            pq.write_table(
                pa_table,
                out_dir / f"{tile_id}.parquet",
                compression="zstd",
            )
            cells_written[h3_res] = n_unique

        return tile_id, "done", cells_written

    except Exception as e:
        import traceback

        return tile_id, f"error: {e}\n{traceback.format_exc()}", {}


# ---------------------------------------------------------------------------
# Phase 3: DuckDB merge
# ---------------------------------------------------------------------------


def get_duckdb_connection() -> duckdb.DuckDBPyConnection:
    """Create a DuckDB connection with spatial extension loaded.

    Configures memory and threads for large merges on high-core machines.
    Uses scratch dir for spill-to-disk when aggregating millions of cells.
    """
    log.info("Initializing DuckDB %s", duckdb.__version__)
    con = duckdb.connect()

    for ext in ("spatial",):
        try:
            con.load_extension(ext)
        except Exception:
            con.install_extension(ext)
            con.load_extension(ext)
        log.info("  Extension '%s' loaded", ext)

    # Performance tuning for large merges
    con.sql(f"SET temp_directory='{SCRATCH_DIR / 'duckdb_tmp'}'")
    log.info("  temp_directory: %s", SCRATCH_DIR / "duckdb_tmp")

    if S3_BUCKET:
        aws_key = os.environ.get("AWS_ACCESS_KEY_ID", "")
        aws_secret = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
        if aws_key and aws_secret:
            con.sql(f"SET s3_region='{AWS_REGION}'")
            con.sql(f"SET s3_access_key_id='{aws_key}'")
            con.sql(f"SET s3_secret_access_key='{aws_secret}'")
            con.sql("SET s3_url_style='path'")
            log.info("  S3 configured: region=%s", AWS_REGION)

    return con


def merge_temp_to_final(
    con: duckdb.DuckDBPyConnection,
    temp_dir: Path,
    h3_res: int,
) -> int:
    """Merge temp Parquet files into a single sorted output per resolution.

    - GROUP BY h3_index to resolve tile-boundary duplicates
    - SUM building counts and footprint areas
    - Compute derived metrics (density, coverage, avg/std height, volume)
    - Add native Parquet GEOMETRY via DuckDB spatial
    - Write with ZSTD compression, sorted by h3_index

    Returns total unique cell count.
    """
    res_temp = temp_dir / f"h3_res={h3_res}"
    if not res_temp.exists():
        log.warning("No temp data for H3 res %d — skipping", h3_res)
        return 0

    temp_glob = str(res_temp / "*.parquet")

    n_cells = con.sql(
        f"SELECT count(DISTINCT h3_index) FROM read_parquet('{temp_glob}')"
    ).fetchone()[0]
    if n_cells == 0:
        log.warning("No cells for H3 res %d — skipping", h3_res)
        return 0

    log.info("Merging H3 res %d: %d unique cells", h3_res, n_cells)

    # Output path
    if S3_BUCKET:
        base = f"s3://{S3_BUCKET}"
        if S3_PREFIX:
            base = f"{base}/{S3_PREFIX}"
        base = f"{base}/buildings"
    else:
        base = str(SCRATCH_DIR / "output" / "buildings")

    output_path = f"{base}/h3_res={h3_res}/data.parquet"
    if not S3_BUCKET:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    t0 = time.time()
    con.sql(f"""
        COPY (
            SELECT
                h3_index,
                ST_Point(
                    any_value(lon), any_value(lat)
                )::GEOMETRY('EPSG:4326') AS geometry,
                any_value(lat)::FLOAT AS lat,
                any_value(lon)::FLOAT AS lon,
                any_value(area_km2)::FLOAT AS area_km2,
                SUM(building_count)::INT AS building_count,
                (SUM(building_count) / any_value(area_km2))::FLOAT
                    AS building_density,
                SUM(total_footprint_m2)::FLOAT AS total_footprint_m2,
                (SUM(total_footprint_m2)
                    / (any_value(area_km2) * 1e6))::FLOAT
                    AS coverage_ratio,
                (SUM(height_sum)
                    / NULLIF(SUM(height_count), 0))::FLOAT
                    AS avg_height_m,
                MAX(max_height_m)::FLOAT AS max_height_m,
                SQRT(GREATEST(
                    SUM(height_sq_sum)
                        / NULLIF(SUM(height_count), 0)
                    - POWER(
                        SUM(height_sum)
                            / NULLIF(SUM(height_count), 0), 2
                      ),
                    0
                ))::FLOAT AS height_std_m,
                SUM(total_volume_m3)::FLOAT AS total_volume_m3,
                (SUM(total_volume_m3)
                    / any_value(area_km2))::FLOAT
                    AS volume_density_m3_per_km2,
                (SUM(total_footprint_m2)
                    / NULLIF(SUM(building_count), 0))::FLOAT
                    AS avg_footprint_m2
            FROM read_parquet('{temp_glob}', hive_partitioning=false)
            GROUP BY h3_index
            ORDER BY h3_index
        ) TO '{output_path}'
        (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 3,
         ROW_GROUP_SIZE 1000000, GEOPARQUET_VERSION 'BOTH')
    """)

    log.info(
        "  Wrote %s (%d cells) in %.1fs",
        output_path,
        n_cells,
        time.time() - t0,
    )
    return n_cells


# ---------------------------------------------------------------------------
# Checkpoint
# ---------------------------------------------------------------------------


def load_checkpoint(checkpoint_path: Path) -> dict:
    """Load processing checkpoint for resume capability."""
    if checkpoint_path.exists():
        return json.loads(checkpoint_path.read_text())
    return {"completed_tiles": {}}


def save_checkpoint(state: dict, checkpoint_path: Path) -> None:
    """Save processing checkpoint atomically."""
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = checkpoint_path.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2))
    tmp.replace(checkpoint_path)


# ---------------------------------------------------------------------------
# Metadata
# ---------------------------------------------------------------------------


def write_metadata(
    h3_resolutions: list[int],
    cells_per_res: dict[int, int],
    elapsed: float,
) -> None:
    """Write _metadata.json documenting the dataset."""
    meta = {
        "dataset": "buildings",
        "source": "Global Building Atlas LOD1",
        "source_url": "https://beta.source.coop/tge-labs/globalbuildingatlas-lod1/",
        "crs": "EPSG:4326",
        "geometry_type": "native_parquet_2.11_geometry",
        "geometry_encoding": "WKB with GEOMETRY logical type annotation",
        "h3_resolutions": h3_resolutions,
        "layout": "single Parquet file per resolution, sorted by h3_index",
        "columns": {
            "h3_index": "H3 cell ID (hex string)",
            "geometry": "Cell center POINT, native Parquet 2.11+ GEOMETRY('EPSG:4326')",
            "lat": "Cell center latitude (float32)",
            "lon": "Cell center longitude (float32)",
            "area_km2": "H3 cell area in km² (float32)",
            "building_count": "Number of buildings (int32)",
            "building_density": "Buildings per km² (float32)",
            "total_footprint_m2": "Sum of building footprint areas in m² (float32)",
            "coverage_ratio": "Footprint coverage fraction 0–1 (float32)",
            "avg_height_m": "Mean building height in m, valid-height only (float32)",
            "max_height_m": "Tallest building in m (float32)",
            "height_std_m": "Height standard deviation in m (float32)",
            "total_volume_m3": "Sum(footprint × height) in m³ (float32)",
            "volume_density_m3_per_km2": "Built volume per km² (float32)",
            "avg_footprint_m2": "Mean building footprint area in m² (float32)",
        },
        "height_nodata": -999,
        "height_nodata_note": (
            "Buildings with height=-999 are included in count/footprint metrics "
            "but excluded from height/volume metrics"
        ),
        "footprint_area_method": (
            "Approximate: area_deg² × 111320² × cos(lat_rad). "
            "Accurate for individual buildings in geographic coordinates."
        ),
        "aggregation": (
            "SUM for counts/areas/volumes; "
            "weighted mean/std for heights across tile boundaries"
        ),
        "compression": "ZSTD level 3",
        "cells_per_resolution": {str(k): v for k, v in sorted(cells_per_res.items())},
        "processing_time_seconds": round(elapsed, 1),
        "processing_date": time.strftime("%Y-%m-%d"),
    }

    if S3_BUCKET:
        import boto3

        s3 = boto3.client("s3", region_name=AWS_REGION)
        prefix = f"{S3_PREFIX}/buildings" if S3_PREFIX else "buildings"
        key = f"{prefix}/_metadata.json"
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=key,
            Body=json.dumps(meta, indent=2),
            ContentType="application/json",
        )
        log.info("Wrote metadata to s3://%s/%s", S3_BUCKET, key)
    else:
        path = SCRATCH_DIR / "output" / "buildings" / "_metadata.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(meta, indent=2))
        log.info("Wrote metadata to %s", path)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Global Building Atlas LOD1 → H3 GeoParquet pipeline"
    )
    parser.add_argument(
        "--source",
        default=SOURCE_S3_URI,
        help=(f"Source tiles: local directory or S3 URI (default: {SOURCE_S3_URI})"),
    )
    parser.add_argument(
        "--resolutions",
        default="3,4,5,6,7,8",
        help="Comma-separated H3 resolutions (default: 3-8)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=0,
        help="Worker process count (default: nproc - 2)",
    )
    parser.add_argument(
        "--scratch-dir",
        type=str,
        default=None,
        help="Override scratch directory (default: /data/scratch/buildings)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List source tiles without processing",
    )
    args = parser.parse_args()

    # Override globals from args
    global SCRATCH_DIR, CHECKPOINT_FILE
    if args.scratch_dir:
        SCRATCH_DIR = Path(args.scratch_dir)
    CHECKPOINT_FILE = SCRATCH_DIR / "checkpoint.json"

    h3_resolutions = sorted(int(r) for r in args.resolutions.split(","))
    workers = args.workers or max(1, (os.cpu_count() or 4) - 2)

    start = time.time()
    log.info("=" * 60)
    log.info("Global Building Atlas → H3 GeoParquet Pipeline")
    log.info("  Source:      %s", args.source)
    log.info("  Resolutions: %s", h3_resolutions)
    log.info("  Workers:     %d", workers)
    log.info("  Scratch:     %s", SCRATCH_DIR)
    log.info("  Output:      %s", f"s3://{S3_BUCKET}" if S3_BUCKET else "local")
    log.info("  Memory:      %s", _mem_gb())
    log.info("=" * 60)

    # --- Phase 1: Discover source tiles ---
    tiles = list_source_tiles(args.source)
    if not tiles:
        log.error("No source tiles found at %s", args.source)
        sys.exit(1)

    log.info("Source tiles: %d", len(tiles))

    if args.dry_run:
        log.info(
            "Dry run: %d tiles × %d resolutions",
            len(tiles),
            len(h3_resolutions),
        )
        for t in tiles[:10]:
            log.info("  %s", t)
        if len(tiles) > 10:
            log.info("  ... and %d more", len(tiles) - 10)
        return

    # --- Phase 2: Parallel tile processing ---
    checkpoint = load_checkpoint(CHECKPOINT_FILE)
    temp_dir = SCRATCH_DIR / "temp"
    temp_dir.mkdir(parents=True, exist_ok=True)

    # Build task list (skip completed tiles)
    completed = checkpoint.get("completed_tiles", {})
    tasks = []
    for tile_path in tiles:
        tid = _tile_id_from_path(tile_path)
        if tid in completed:
            continue
        tasks.append(
            {
                "tile_id": tid,
                "tile_path": tile_path,
                "h3_resolutions": h3_resolutions,
                "temp_dir": str(temp_dir),
            }
        )

    log.info(
        "Processing %d tiles (%d previously completed)",
        len(tasks),
        len(completed),
    )

    if tasks:
        ctx = mp.get_context("spawn")
        done_count = 0
        error_count = 0
        skip_count = 0

        with ProcessPoolExecutor(max_workers=workers, mp_context=ctx) as pool:
            futures = {pool.submit(_process_tile, t): t["tile_id"] for t in tasks}

            with tqdm(total=len(futures), desc="Processing tiles", unit="tile") as pbar:
                for future in as_completed(futures):
                    try:
                        tile_id, status, cell_counts = future.result()
                    except Exception as exc:
                        tile_id = futures[future]
                        log.error("Tile %s raised: %s", tile_id, exc)
                        error_count += 1
                        pbar.update(1)
                        continue

                    if status == "done":
                        checkpoint.setdefault("completed_tiles", {})[tile_id] = "done"
                        save_checkpoint(checkpoint, CHECKPOINT_FILE)
                        done_count += 1
                    elif "error" in status:
                        log.warning("Tile %s: %s", tile_id, status)
                        error_count += 1
                    else:
                        checkpoint.setdefault("completed_tiles", {})[tile_id] = status
                        save_checkpoint(checkpoint, CHECKPOINT_FILE)
                        skip_count += 1

                    pbar.update(1)

        log.info(
            "Tile processing complete: %d done, %d skipped, %d errors",
            done_count,
            skip_count,
            error_count,
        )

    # --- Phase 3: DuckDB merge ---
    log.info("Starting DuckDB merge phase")
    con = get_duckdb_connection()

    cells_per_res: dict[int, int] = {}
    for h3_res in h3_resolutions:
        n = merge_temp_to_final(con, temp_dir, h3_res)
        cells_per_res[h3_res] = n

    elapsed = time.time() - start

    # --- Phase 4: Metadata ---
    write_metadata(h3_resolutions, cells_per_res, elapsed)

    # Cleanup temp files
    if temp_dir.exists():
        shutil.rmtree(temp_dir)
        log.info("Cleaned up temp files: %s", temp_dir)

    # --- Summary ---
    log.info("=" * 60)
    log.info("Pipeline complete in %.1f minutes", elapsed / 60)
    for res in sorted(cells_per_res):
        log.info("  H3 res %2d: %12d cells", res, cells_per_res[res])
    total = sum(cells_per_res.values())
    log.info("  Total:     %12d cells", total)
    log.info("  Peak memory: %s", _mem_gb())
    log.info("=" * 60)

    # Completion marker
    marker = SCRATCH_DIR / "COMPLETE"
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text(f"Completed at {time.strftime('%Y-%m-%d %H:%M:%S')}\n")


if __name__ == "__main__":
    main()
