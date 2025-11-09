# GPU Computing Compatibility Pipeline

This project scrapes vendor and framework pages (NVIDIA, AMD, PyTorch, TensorFlow, JAX), normalizes the data, and exports flattened compatibility tables to `data/processed/` and a SQLite database.

## Running the pipeline

  - Strict checks may require both vendors and framework evidence; relax with `--relaxed-checks` when needed.

# gpu_computing_compat_etl

## Weekly scheduled run (CI)

This repository contains a GitHub Actions workflow that runs the ETL once a week to keep processed CSV/SQLite outputs fresh.

- File: `.github/workflows/weekly_gpu_compat_pipeline.yml`
- Schedule: Every Monday at 03:00 UTC
- Outputs: Artifacts in the workflow run (`gpu-compat-data`) containing all CSVs and `gpu_compat.sqlite`

To trigger manually, use the "Run workflow" button under the Actions tab (workflow_dispatch).

If you prefer to commit the refreshed `data/processed` files back to the repo, enable the optional commit step in the workflow and provide a token with write permissions.

## Run locally

Restore the R environment and run the pipeline. You can use relaxed checks to avoid overly strict gating during development.

```
Rscript -e "if(!requireNamespace('renv', quietly=TRUE)) install.packages('renv'); renv::restore()"
Rscript scripts/run_scrapers_and_export.R --relaxed-checks
```

Outputs will be written to `data/processed/` and a SQLite database at `data/processed/gpu_compat.sqlite`.

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
