# Global Building Density (H3-indexed)

[Global Building Atlas LOD1](https://source.coop/tge-labs/globalbuildingatlas-lod1/) (2.75B buildings) aggregated to [H3](https://h3geo.org/) hexagonal cells with urban density metrics. Six H3 resolutions (3–8), one file per resolution, sorted by `h3_index`. Available in two layouts: **v2** (recommended, BIGINT `h3_index`, 11 columns) and **v1** (legacy, VARCHAR `h3_index`, 15 columns with geometry/lat/lon/area_km2).

| | |
|---|---|
| **Source** | [GlobalBuildingAtlas LoD1](https://doi.org/10.14459/2025mp1782307) (Zhu et al., TU Munich), 2.75B buildings, hosted as [Parquet on Source Cooperative](https://source.coop/tge-labs/globalbuildingatlas-lod1/) |
| **Format** | Apache Parquet (v2: BIGINT h3_index, no geometry; v1: VARCHAR h3_index with native GEOMETRY) |
| **CRS** | EPSG:4326 (WGS 84) — v1 only; v2 derives coordinates from h3_index |
| **License** | [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) by [walkthru.earth](https://walkthru.earth/links) |
| **Code** | [walkthru-earth/walkthru-building-index](https://github.com/walkthru-earth/walkthru-building-index) |

## Quick Start

```sql
-- DuckDB (v2, recommended)
INSTALL h3 FROM community; LOAD h3;
INSTALL httpfs;  LOAD httpfs;
SET s3_region = 'us-west-2';

SELECT h3_index, building_count, building_density,
       avg_height_m, coverage_ratio, total_volume_m3,
       h3_cell_to_lat(h3_index) AS lat,
       h3_cell_to_lng(h3_index) AS lng
FROM read_parquet('s3://us-west-2.opendata.source.coop/walkthru-earth/indices/building/v2/h3/h3_res=5/data.parquet')
ORDER BY building_count DESC
LIMIT 20;
```

```python
# Python (v2, recommended)
import duckdb

con = duckdb.connect()
for ext in ("httpfs",):
    con.install_extension(ext); con.load_extension(ext)
con.install_extension("h3", repository="community"); con.load_extension("h3")
con.sql("SET s3_region = 'us-west-2'")

df = con.sql("""
    SELECT h3_index, building_count, building_density,
           avg_height_m, coverage_ratio,
           h3_cell_to_lat(h3_index) AS lat,
           h3_cell_to_lng(h3_index) AS lng
    FROM read_parquet(
        's3://us-west-2.opendata.source.coop/walkthru-earth/indices/building/v2/h3/h3_res=5/data.parquet'
    ) WHERE h3_cell_to_lat(h3_index) BETWEEN 20 AND 35
        AND h3_cell_to_lng(h3_index) BETWEEN 68 AND 90
""").fetchdf()
```

## Files

### v2 (recommended) — BIGINT h3_index, 11 columns

```
walkthru-earth/indices/building/
  v2/h3/
    h3_res=3/data.parquet       632 KB       12,239 cells
    h3_res=4/data.parquet       3.4 MB       69,823 cells
    h3_res=5/data.parquet        19 MB      399,355 cells
    h3_res=6/data.parquet       101 MB    2,175,194 cells
    h3_res=7/data.parquet       489 MB   10,392,162 cells
    h3_res=8/data.parquet       1.9 GB   40,583,320 cells
    _metadata.json
```

### v1 (legacy) — VARCHAR h3_index, 15 columns with geometry/lat/lon/area_km2

```
walkthru-earth/indices/building/
  v1/h3/
    h3_res=3/data.parquet
    ...
    h3_res=8/data.parquet
    _metadata.json
```

**Total: 53,632,093 cells per version.** Compression: ZSTD level 3. Row groups: 1,000,000 rows.

### Global Building Atlas Source Pyramid

The raw Global Building Atlas LOD1 tiles are also available as a zoom-level pyramid:

```
walkthru-earth/indices/building/
  globalbuildingatlas/
    _metadata.json
    zoomlevel=2/data*.parquet
    ...
    zoomlevel=14/data*.parquet
```

## Schema

### v2 (recommended) — 11 columns

| Column | Type | Description |
|--------|------|-------------|
| `h3_index` | BIGINT | H3 cell ID (integer representation) |
| `building_count` | INTEGER | Number of buildings in this cell |
| `building_density` | FLOAT | Buildings per km² |
| `total_footprint_m2` | FLOAT | Sum of building footprint areas (m²) |
| `coverage_ratio` | FLOAT | Footprint coverage fraction (0–1) |
| `avg_height_m` | FLOAT | Mean building height (m), valid-height buildings only |
| `max_height_m` | FLOAT | Tallest building (m) |
| `height_std_m` | FLOAT | Building height standard deviation (m) |
| `total_volume_m3` | FLOAT | Sum of footprint x height (m³), valid-height buildings only |
| `volume_density_m3_per_km2` | FLOAT | Built volume per km² (m³/km²) |
| `avg_footprint_m2` | FLOAT | Mean building footprint area (m²) |

Columns dropped from v1 (`geometry`, `lat`, `lon`, `area_km2`) are derivable from `h3_index` via the DuckDB `h3` extension:

```sql
INSTALL h3 FROM community; LOAD h3;

-- Coordinates
h3_cell_to_lat(h3_index) AS lat, h3_cell_to_lng(h3_index) AS lng
-- Hex string (e.g. for deck.gl)
h3_h3_to_string(h3_index) AS h3_hex
-- Cell area
h3_cell_area(h3_index, 'km^2') AS area_km2
```

### v1 (legacy) — 15 columns

| Column | Type | Description |
|--------|------|-------------|
| `h3_index` | VARCHAR | H3 cell ID (hex string) |
| `geometry` | GEOMETRY | Cell center point (native Parquet 2.11+ GEOMETRY, EPSG:4326) |
| `lat` | FLOAT | Cell center latitude (degrees) |
| `lon` | FLOAT | Cell center longitude (degrees) |
| `area_km2` | FLOAT | H3 cell area (km²) |
| *(plus all 10 metric columns from v2)* | | |

**Sample values** (v2, res 5, most built-up cells):

| h3_index | building_count | density/km² | avg_height_m | coverage |
|----------|----------------|-------------|--------------|----------|
| 599540926406057983 | 998,869 | 3,904 | 5.6 | 0.343 |
| 599396044782133247 | 983,761 | 3,591 | 2.1 | 0.200 |
| 599334009135005695 | 918,389 | 3,326 | 4.4 | 0.318 |

## How It Works

1. Global Building Atlas LOD1 (~hundreds of 5°x5° Parquet tiles, 210 GB) are read in parallel across 178 CPU cores
2. Building polygon centroids and footprint areas are computed via Shapely
3. Each building centroid is assigned to an H3 cell via `h3.latlng_to_cell()`
4. Per-cell metrics are **aggregated**: building counts and footprint areas are **summed**, heights are **averaged** (excluding nodata sentinel -999)
5. Overlapping tile-boundary cells are deduplicated via `GROUP BY h3_index, SUM()` in DuckDB
6. Final Parquet is sorted by `h3_index`. v2 stores BIGINT `h3_index` (no geometry); v1 adds native GEOMETRY via DuckDB spatial

All resolutions produce consistent world totals (~2.75 billion buildings).

## More Examples

```sql
-- All v2 examples require the h3 extension
INSTALL h3 FROM community; LOAD h3;
INSTALL httpfs; LOAD httpfs;
SET s3_region = 'us-west-2';

-- Densest urban areas by building count
SELECT h3_index,
       h3_cell_to_lat(h3_index) AS lat,
       h3_cell_to_lng(h3_index) AS lng,
       building_count, building_density,
       avg_height_m, coverage_ratio,
       total_volume_m3
FROM read_parquet('s3://us-west-2.opendata.source.coop/walkthru-earth/indices/building/v2/h3/h3_res=5/data.parquet')
WHERE building_count > 100000
ORDER BY building_density DESC
LIMIT 20;

-- Continental building totals
SELECT CASE
         WHEN h3_cell_to_lat(h3_index) BETWEEN -35 AND 37 AND h3_cell_to_lng(h3_index) BETWEEN -20 AND 55 THEN 'Africa'
         WHEN h3_cell_to_lat(h3_index) BETWEEN 5 AND 55 AND h3_cell_to_lng(h3_index) BETWEEN 60 AND 150 THEN 'Asia'
         WHEN h3_cell_to_lat(h3_index) BETWEEN 35 AND 72 AND h3_cell_to_lng(h3_index) BETWEEN -12 AND 45 THEN 'Europe'
         WHEN h3_cell_to_lat(h3_index) BETWEEN -56 AND 15 AND h3_cell_to_lng(h3_index) BETWEEN -82 AND -34 THEN 'South America'
         WHEN h3_cell_to_lat(h3_index) BETWEEN 15 AND 72 AND h3_cell_to_lng(h3_index) BETWEEN -170 AND -50 THEN 'North America'
         ELSE 'Other'
       END AS continent,
       SUM(building_count)::BIGINT AS buildings,
       (SUM(total_footprint_m2) / 1e6)::DECIMAL(12,1) AS footprint_km2,
       (SUM(total_volume_m3) / 1e9)::DECIMAL(12,1) AS volume_billion_m3
FROM read_parquet('s3://us-west-2.opendata.source.coop/walkthru-earth/indices/building/v2/h3/h3_res=5/data.parquet')
GROUP BY continent
ORDER BY buildings DESC;

-- Tallest buildings per cell
SELECT h3_index,
       h3_cell_to_lat(h3_index) AS lat,
       h3_cell_to_lng(h3_index) AS lng,
       max_height_m, avg_height_m, building_count
FROM read_parquet('s3://us-west-2.opendata.source.coop/walkthru-earth/indices/building/v2/h3/h3_res=8/data.parquet')
WHERE NOT isnan(max_height_m) AND max_height_m > 100
ORDER BY max_height_m DESC
LIMIT 20;

-- Join with population index for per-capita building density
SELECT b.h3_index,
       h3_cell_to_lat(b.h3_index) AS lat,
       h3_cell_to_lng(b.h3_index) AS lng,
       b.building_count,
       p.pop_2025,
       (b.building_count::FLOAT / NULLIF(p.pop_2025, 0))::DECIMAL(6,3) AS buildings_per_person
FROM read_parquet('s3://us-west-2.opendata.source.coop/walkthru-earth/indices/building/v2/h3/h3_res=5/data.parquet') b
JOIN read_parquet('s3://us-west-2.opendata.source.coop/walkthru-earth/indices/population/v2/scenario=SSP2/h3_res=5/data.parquet') p
  ON b.h3_index = p.h3_index
WHERE p.pop_2025 > 10000
ORDER BY buildings_per_person DESC
LIMIT 20;

-- Hex string for deck.gl visualization
SELECT h3_h3_to_string(h3_index) AS h3_hex,
       building_count, building_density, avg_height_m
FROM read_parquet('s3://us-west-2.opendata.source.coop/walkthru-earth/indices/building/v2/h3/h3_res=5/data.parquet')
WHERE h3_cell_to_lat(h3_index) BETWEEN 35 AND 45
  AND h3_cell_to_lng(h3_index) BETWEEN -10 AND 5
LIMIT 100;

-- DuckDB-WASM (browser) — use HTTPS URL
SELECT h3_index, building_count, building_density, avg_height_m,
       h3_cell_to_lat(h3_index) AS lat,
       h3_cell_to_lng(h3_index) AS lng
FROM read_parquet(
    'https://data.source.coop/walkthru-earth/indices/building/v2/h3/h3_res=5/data.parquet'
)
WHERE h3_cell_to_lat(h3_index) BETWEEN 35 AND 45
  AND h3_cell_to_lng(h3_index) BETWEEN -10 AND 5
LIMIT 100;
```

## Geometry Format

**v2** has no `geometry` column. Coordinates, hex strings, and cell areas are derived at query time from the BIGINT `h3_index` via the DuckDB `h3` community extension. This keeps files smaller and avoids redundant stored columns.

**v1** (legacy) retains the `geometry` column using the [native Parquet 2.11+ GEOMETRY logical type](https://github.com/apache/parquet-format/blob/master/Geospatial.md) with GeoParquet 1.0 file-level metadata for backwards compatibility (`GEOPARQUET_VERSION 'BOTH'`). DuckDB 1.5+ writes per-row-group bounding box statistics automatically. Supported by: DuckDB 1.5+, Apache Arrow (Rust), Apache Iceberg, GDAL 3.12+.

## Source

[GlobalBuildingAtlas LoD1](https://source.coop/tge-labs/globalbuildingatlas-lod1/) — 2.75 billion building footprints with LOD1 heights, distributed as ~hundreds of 5°x5° Parquet tiles (210 GB total). POLYGON geometry with `height` attribute in meters (-999 = no data). Combines Google Open Buildings (1.62B), OpenStreetMap (490M), Microsoft Building Footprints (432M), TUM deep learning from Planet Labs imagery (135M), and 3D Global Footprints (68M).

> Zhu, X. X., Chen, S., Zhang, F., Shi, Y., & Wang, Y. (2025). GlobalBuildingAtlas: An Open Global and Complete Dataset of Building Polygons, Heights and LoD1 3D Models. Technical University of Munich. [doi:10.14459/2025mp1782307](https://doi.org/10.14459/2025mp1782307)

Parquet conversion hosted on Source Cooperative:

> GlobalBuildingAtlas LoD1 (Parquet Format). [source.coop/tge-labs/globalbuildingatlas-lod1](https://source.coop/tge-labs/globalbuildingatlas-lod1)

## License

This dataset is licensed under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) by [walkthru.earth](https://walkthru.earth/links). The source [GlobalBuildingAtlas](https://doi.org/10.14459/2025mp1782307) is by Zhu et al. (TU Munich), hosted as Parquet by [TGE Labs](https://source.coop/tge-labs/) on [Source Cooperative](https://source.coop/).
