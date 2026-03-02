# Transit Flow Maps

`transit-flow-maps` builds deterministic transit throughput layers for San Francisco Muni and BART, then publishes an interactive static map.

## Scope (v1.x)
- Throughput metric is passenger-traversals/day on undirected segments.
- Shared corridors merge flows when routes overlap physically.
- Crossing routes do not merge at intersections.
- Static site output is built for GitHub Pages in `docs/`.

## Repository layout

```text
transit-flow-maps/
  pipeline/
  web/
  data/
  docs/
```

## Quick start

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -e pipeline[dev]

# Show CLI
python -m transit_flow_maps.cli --help
```

## Current status
Milestone 5 is implemented for `tfm build-segments`, `tfm build-flows-muni`, `tfm build-flows-bart`, `tfm build-corridors`, and `tfm export-geojson`.

## Data policy
Raw data is local-only and ignored from Git. See `data/README.md` for details.
