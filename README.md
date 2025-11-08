# GPU Computing Compatibility Pipeline

This project scrapes vendor and framework pages (NVIDIA, AMD, PyTorch, TensorFlow, JAX), normalizes the data, and exports flattened compatibility tables to `data/processed/` and a SQLite database.

## Running the pipeline

- Orchestrated by `scripts/run_scrapers_and_export.R`
  - Strict checks may require both vendors and framework evidence; relax with `--relaxed-checks` when needed.

## Cleaning

SQLite-first workflow:
Use `scripts/clean_workspace.R` to prune transient artifacts while retaining the authoritative database:

What it does now:
  - Clears `cache/` (HTTP fetch cache)
  - Deletes debug artifacts in `dev/` (`*.csv`, `*.tmp`, `*.log`), keeps `dev/iteration_log.md`
  - Removes CSV exports in `data/processed/` if `gpu_compat.sqlite` exists
  - Preserves `gpu_compat.sqlite`

Regenerating CSVs: run the pipeline again (`run_scrapers_and_export.R`).
If you need to force a full data reset, manually remove `data/processed/gpu_compat.sqlite` and rerun.

## Outputs

Primary artifact (authoritative):
- `gpu_compat.sqlite` (contains all tables plus `compat_map` view)

Optional CSV exports (regenerated each run, may be removed by clean script):
- vendors.csv, gpu_models.csv, runtimes.csv, languages.csv, frameworks.csv
- gpu_runtime_compat.csv, runtime_language.csv, framework_compat.csv, framework_gpu_compat.csv
- compat_map.csv
