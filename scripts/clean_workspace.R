# Simple clean tuned for SQLite-only workflow.
# Actions:
#  - Delete all files and subdirectories inside cache/ (ephemeral fetch layer)
#  - Delete debug artifacts in dev/ (*.csv, *.tmp, *.log) except iteration_log.md
#  - Remove CSV exports in data/processed/ (they are redundant if using SQLite)
#  - Preserve gpu_compat.sqlite (authoritative) and directory structure.
# If you need CSVs again, rerun the pipeline.
# Exit code 0 on success, 2 on partial failure.

root <- getwd()

safe_unlink <- function(paths, recursive = FALSE) {
  removed <- 0L
  failed <- 0L
  for (p in paths) {
    if (!file.exists(p)) next
    res <- try(unlink(p, recursive = recursive, force = TRUE), silent = TRUE)
    if (inherits(res, 'try-error')) {
      message('Failed to remove: ', p)
      failed <- failed + 1L
    } else {
      removed <- removed + 1L
    }
  }
  list(removed = removed, failed = failed)
}

summary <- list(cache = 0L, dev = 0L, failed = 0L)

# 1) cache/: remove everything inside
cache_dir <- file.path(root, 'cache')
if (dir.exists(cache_dir)) {
  items <- list.files(cache_dir, full.names = TRUE, recursive = FALSE, all.files = TRUE, no.. = TRUE)
  if (length(items) > 0) {
    res <- safe_unlink(items, recursive = TRUE)
    summary$cache <- res$removed
    summary$failed <- summary$failed + res$failed
  }
}

# 2) dev/: remove debug artifacts (*.csv, *.tmp, *.log), keep iteration_log.md
dev_dir <- file.path(root, 'dev')
if (dir.exists(dev_dir)) {
  files <- list.files(dev_dir, full.names = TRUE, recursive = FALSE)
  keep <- file.path(dev_dir, 'iteration_log.md')
  dbg <- files[ grepl('\\.(csv|tmp|log)$', files, ignore.case = TRUE) & files != keep ]
  if (length(dbg) > 0) {
    res <- safe_unlink(dbg, recursive = FALSE)
    summary$dev <- res$removed
    summary$failed <- summary$failed + res$failed
  }
}

## 3) data/processed/: remove CSVs but keep SQLite
proc_dir <- file.path(root, 'data', 'processed')
if (dir.exists(proc_dir)) {
  csvs <- list.files(proc_dir, pattern = '\\.(csv)$', full.names = TRUE, recursive = FALSE)
  # never remove compat DB
  keep_db <- file.path(proc_dir, 'gpu_compat.sqlite')
  # safeguard: only remove if DB exists (avoid accidental full purge if pipeline not run yet)
  if (file.exists(keep_db) && length(csvs) > 0) {
    res <- safe_unlink(csvs, recursive = FALSE)
    summary$processed_csv_removed <- res$removed
    summary$failed <- summary$failed + res$failed
  } else {
    summary$processed_csv_removed <- 0L
  }
}

cat(sprintf('Clean complete: cache=%d, dev=%d, csv_removed=%d, failed=%d (SQLite preserved)\n', summary$cache, summary$dev, summary$processed_csv_removed, summary$failed))

quit(status = if (summary$failed == 0L) 0 else 2)
