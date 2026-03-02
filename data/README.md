# Data policy

## Tracked in Git
- `data/processed/web/*` only when needed for a release build to `docs/`.
- Small, synthetic fixtures for tests (if added later under `pipeline/tests/fixtures/`).

## Not tracked in Git
- Raw feeds and vendor files under `data/raw/**`.
- GTFS zip files anywhere in `data/**/gtfs/*.zip`.

## Recommended workflow for your attached files
1. Keep source files local in `data/raw/` (or symlink into `data/raw/gtfs/`).
2. Commit only deterministic code + config + validation reports.
3. Optionally keep a private backup location (cloud drive) for raw files.

## About Git LFS / DVC
- v1 recommendation: do not add raw GTFS/ridership binaries to Git or LFS.
- Consider DVC/LFS only if you later need reproducible shared raw snapshots across collaborators.
