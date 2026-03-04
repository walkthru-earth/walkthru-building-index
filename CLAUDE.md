# CLAUDE.md

## Project

**walkthru-building-index** — Global Building Atlas LOD1 (2.75B buildings, 210 GB Parquet) → H3-indexed GeoParquet with urban density metrics and native Parquet 2.11+ GEOMETRY.

Part of the [walkthru-earth](https://github.com/walkthru-earth) index family alongside `walkthru-pop-index`, `walkthru-weather-index`, and `dem-terrain`.

## Commands

```bash
# Setup
uv sync

# Lint & format (ALWAYS before committing)
uv run ruff check . --fix
uv run ruff format .

# Inspect source data
uv run python inspection/explore_source.py

# Inspect output
uv run python inspection/query_output.py

# Run pipeline (use tmux for long-running jobs)
uv run python main.py --source /data/gba/ --resolutions 3,4,5,6,7,8 --workers 178
uv run python main.py --dry-run                    # Preview tiles
uv run python main.py --source s3://...            # Read directly from S3
```

## Source data

- **Global Building Atlas LOD1**: `s3://us-west-2.opendata.source.coop/tge-labs/globalbuildingatlas-lod1/`
- ~hundreds of 5°×5° tiles, POLYGON geometry, `height` (meters, -999=nodata), `source`, `region`
- 2.75B buildings, 210 GB total
- Median building: 136 m², 2.0 m height
- Citation: Zhu, X. X. et al. (2025). GlobalBuildingAtlas. TU Munich. [doi:10.14459/2025mp1782307](https://doi.org/10.14459/2025mp1782307)

## Architecture

```
GBA Parquet tiles (210 GB, ~hundreds of 5°×5° files on Source Cooperative)
        ↓
Phase 1: list_source_tiles() — discover tiles from S3 or local directory
Phase 2: ProcessPoolExecutor(N workers, spawn context)
          ├─ Read tile Parquet (pyarrow: geometry + height columns)
          ├─ Compute centroids + footprint areas (shapely)
          ├─ H3 cell assignment per building (h3.latlng_to_cell)
          ├─ GroupBy H3 cell → aggregate building metrics
          └─ Write temp Parquet per resolution
Phase 3: DuckDB merge per resolution
          ├─ GROUP BY h3_index across tiles (boundary dedup)
          ├─ Compute derived metrics (density, coverage, avg/std height, volume)
          ├─ Add ST_Point geometry (native Parquet 2.11+)
          └─ Write final sorted Parquet with GEOPARQUET_VERSION 'BOTH'
Phase 4: _metadata.json + progressive S3 upload
```

## S3 output layout

```
s3://us-west-2.opendata.source.coop/walkthru-earth/indices/building/
  h3/
    h3_res=3/data.parquet
    h3_res=4/data.parquet
    ...
    h3_res=8/data.parquet
    _metadata.json
  globalbuildingatlas/
    _metadata.json
    zoomlevel={2-14}/data*.parquet
```

Each Parquet file columns:
`h3_index, geometry, lat, lon, area_km2, building_count, building_density,
total_footprint_m2, coverage_ratio, avg_height_m, max_height_m, height_std_m,
total_volume_m3, volume_density_m3_per_km2, avg_footprint_m2`

## Key design decisions

- **SUM aggregation**: building counts and footprint areas summed across tiles for boundary-overlapping H3 cells
- **Height nodata = -999**: buildings with -999 height are counted for footprint/density but excluded from height/volume metrics
- **Footprint area approximation**: `area_deg² × 111320² × cos(lat)` — accurate for individual buildings in geographic coordinates
- **Combined stddev**: uses `sqrt(E[X²] - E[X]²)` across tile partials via height_sum, height_sq_sum, height_count
- **multiprocessing.ProcessPoolExecutor**: true CPU parallelism (bypasses GIL), uses `spawn` context
- **Tile-boundary dedup**: same H3 cell can appear in adjacent tiles; DuckDB GROUP BY resolves
- **Native Parquet GEOMETRY**: DuckDB 1.5.0 spatial writes `ST_Point(lon, lat)::GEOMETRY('EPSG:4326')` with `GEOPARQUET_VERSION 'BOTH'`
- **Checkpoint/resume**: completed tiles tracked in checkpoint.json; safe to kill and restart

## Code conventions

- **Always use `pathlib.Path`** instead of `os.path` for all path operations
- **Progressive S3 upload**: upload each resolution's Parquet immediately after merge
- **`uv add`** for dependency management — never edit pyproject.toml manually
- **`uv run ruff format . && uv run ruff check . --fix`** before every commit

## Documentation files

- `README.md` — GitHub repo README (code usage)
- `SC_README.md` — Source Cooperative dataset README (uploaded to S3 as `indices/building/README.md`)

## File layout

```
main.py              Pipeline entrypoint
inspection/
  explore_source.py  Explore GBA source data on S3
  query_output.py    DuckDB queries on output Parquet files
pyproject.toml       Dependencies (shapely, h3, duckdb==1.5.0.dev329, numpy, pyarrow)
CLAUDE.md            This file
```

## License

CC BY 4.0 by walkthru-earth. Source data by Zhu et al. (TU Munich), hosted by TGE Labs on Source Cooperative.
