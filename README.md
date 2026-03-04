# walkthru-building-index

Global Building Atlas LOD1 (2.75B buildings) → H3-indexed GeoParquet with urban density metrics.

Part of the [walkthru-earth](https://github.com/walkthru-earth) index family.

## What it does

Processes the [Global Building Atlas LOD1](https://beta.source.coop/tge-labs/globalbuildingatlas-lod1/) — 2.75 billion building footprints with heights — into H3-indexed GeoParquet files with per-cell urban metrics:

| Column | Description |
|---|---|
| `building_count` | Number of buildings in cell |
| `building_density` | Buildings per km² |
| `total_footprint_m2` | Sum of building footprint areas |
| `coverage_ratio` | Footprint coverage (0–1) |
| `avg_height_m` | Mean building height |
| `max_height_m` | Tallest building |
| `height_std_m` | Height variation |
| `total_volume_m3` | Total built volume |
| `volume_density_m3_per_km2` | Built volume per km² |
| `avg_footprint_m2` | Mean building size |

Output at H3 resolutions 3–8 with native Parquet 2.11+ GEOMETRY.

## Setup

```bash
uv sync
```

## Usage

```bash
# From local pre-downloaded tiles
uv run python main.py --source /data/gba/ --workers 178

# From S3 directly
uv run python main.py --workers 178

# Preview without processing
uv run python main.py --dry-run

# Specific resolutions
uv run python main.py --source /data/gba/ --resolutions 5,6,7
```

## Output

```
indices/building/h3/
  h3_res=3/data.parquet
  ...
  h3_res=8/data.parquet
  _metadata.json
```

## Source

> Zhu, X. X., Chen, S., Zhang, F., Shi, Y., & Wang, Y. (2025). GlobalBuildingAtlas: An Open Global and Complete Dataset of Building Polygons, Heights and LoD1 3D Models. Technical University of Munich. [doi:10.14459/2025mp1782307](https://doi.org/10.14459/2025mp1782307)

## License

This project is licensed under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) by [walkthru.earth](https://github.com/walkthru-earth). See [LICENSE](LICENSE) for details. The source [GlobalBuildingAtlas](https://doi.org/10.14459/2025mp1782307) is by Zhu et al. (TU Munich), hosted as Parquet by [TGE Labs](https://source.coop/tge-labs/) on [Source Cooperative](https://source.coop/).

Contact: [hi@walkthru.earth](mailto:hi@walkthru.earth)
