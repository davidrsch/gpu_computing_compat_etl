library(glue)
library(digest)
library(lubridate)


# Load helper and scrapers
source('R/fetch_with_retry.R')
source('R/scraping/vendors/scrape_nvidia.R')
source('R/scraping/vendors/scrape_amd.R')
source('R/scraping/frameworks/scrape_frameworks.R')
source('etl/normalize_minimal.R')

log_path <- 'dev/iteration_log.md'
if (!file.exists('dev')) {
  dir.create('dev', recursive = TRUE)
}
if (!file.exists(log_path)) {
  writeLines(c('# iteration log', ''), log_path)
}

max_attempts_per_source <- 5
global_max_attempts <- 5
attempt <- 1

# parse CLI args
args <- commandArgs(trailingOnly = TRUE)
relaxed <- '--relaxed-checks' %in% args

run_id <- function() {
  paste0(
    format(Sys.time(), '%Y%m%dT%H%M%SZ', tz = 'UTC'),
    '-',
    substr(digest(runif(1)), 1, 8)
  )
}

append_log <- function(line) {
  write(line, file = log_path, append = TRUE)
}

complete_checks <- function(tables, relaxed = FALSE) {
  missing <- list()
  status_ok <- TRUE
  # vendors must include at least one vendor
  vendors <- tables$vendors
  if (nrow(vendors) == 0) {
    missing$vendors <- 'no vendors'
    status_ok <- FALSE
  }

  gpus <- tables$gpu_models
  if (nrow(gpus) == 0) {
    missing$gpus <- 'no gpu models'
    status_ok <- FALSE
  }
  if (!relaxed) {
    # >=5 per vendor if page non-empty: approximate by vendor counts
    vendor_counts <- table(gpus$vendor_id)
    if (any(vendor_counts > 0 & vendor_counts < 5)) {
      missing$gpus_per_vendor <- 'some vendors have <5 models'
      status_ok <- FALSE
    }
  }

  runtimes <- tables$runtimes
  found_names <- toupper(runtimes$name)
  # Determine expected runtimes based on which vendors are present
  vnames <- toupper(na.omit(vendors$name))
  expected <- character(0)
  if ('NVIDIA' %in% vnames) expected <- c(expected, 'CUDA')
  if ('AMD' %in% vnames) expected <- c(expected, 'ROCM')
  if ('INTEL' %in% vnames) expected <- c(expected, 'ONEAPI')
  for (e in unique(expected)) {
    if (!(e %in% found_names) && !relaxed) {
      missing[[paste0('runtime_', e)]] <- paste('missing', e)
      status_ok <- FALSE
    }
  }

  runtime_language <- tables$runtime_language
  if (nrow(runtime_language) == 0 && !relaxed) {
    missing$runtime_language <- 'no runtime->language mappings'
    status_ok <- FALSE
  }

  framework_compat <- tables$framework_compat
  if (nrow(framework_compat) == 0 && !relaxed) {
    missing$framework_compat <- 'no framework compatibility mappings'
    status_ok <- FALSE
  }

  gpu_runtime <- tables$gpu_runtime
  if (nrow(gpu_runtime) < 5 && !relaxed) {
    missing$gpu_runtime <- 'less than 5 gpu->runtime mappings'
    status_ok <- FALSE
  }

  list(ok = status_ok, missing = missing)
}

# main loop
last_errs <- list()
while (attempt <= global_max_attempts) {
  runid <- run_id()
  start_ts <- Sys.time()
  cat('Starting run', runid, 'attempt', attempt, '\n')
  raw_list <- list()
  errors <- list()

  # Run scrapers with per-source retries
  scrapers <- list(
    nvidia = scrape_nvidia,
    amd = scrape_amd,
    frameworks = scrape_frameworks
  )
  for (name in names(scrapers)) {
    ok <- FALSE
    a <- 1
    while (a <= max_attempts_per_source && !ok) {
      cat(glue('Fetching {name} (attempt {a})...\n'))
      try_res <- try(
        {
          res <- scrapers[[name]]()
          raw_list[[name]] <- res
          ok <- TRUE
        },
        silent = TRUE
      )
      if (!ok) {
        msg <- if (inherits(try_res, 'try-error')) {
          as.character(try_res)
        } else {
          'unknown error'
        }
        append_log(glue(
          '{format(Sys.time(), tz="UTC")}: run {runid} - parse error for {name} attempt {a}: {msg}'
        ))
        a <- a + 1
        Sys.sleep(2^a)
      }
    }
    if (!ok) {
      append_log(glue(
        '{format(Sys.time(), tz="UTC")}: run {runid} - aborting: {name} failed after {max_attempts_per_source} attempts'
      ))
      stop(glue(
        'Scraper {name} failed after {max_attempts_per_source} attempts'
      ))
    }
  }

  # normalization
  tables <- normalize_and_export(raw_list, out_dir = 'data/processed')

  # completeness checks
  cc <- complete_checks(tables, relaxed = relaxed)
  end_ts <- Sys.time()
  counts <- list(
    vendors = nrow(tables$vendors),
    gpus = nrow(tables$gpu_models),
    runtimes = nrow(tables$runtimes),
    languages = nrow(tables$languages),
    frameworks = nrow(tables$frameworks),
    gpu_runtime_mappings = nrow(tables$gpu_runtime),
    runtime_language_mappings = nrow(tables$runtime_language),
    framework_compat_mappings = nrow(tables$framework_compat)
  )

  status <- if (cc$ok) {
    'OK'
  } else if (attempt < global_max_attempts) {
    'RETRY'
  } else {
    'FAILED'
  }
  cat(glue(
    'RUN {runid} vendors={counts$vendors} gpus={counts$gpus} runtimes={counts$runtimes} languages={counts$languages} frameworks={counts$frameworks} gpu_runtime_mappings={counts$gpu_runtime_mappings} runtime_language_mappings={counts$runtime_language_mappings} framework_compat_mappings={counts$framework_compat_mappings} — STATUS: {status}\n'
  ))

  # append summary to log
  missing_text <- if (length(cc$missing) == 0) {
    'none'
  } else {
    paste(names(cc$missing), collapse = ';')
  }
  append_log(glue(
    '{format(Sys.time(), tz="UTC")}: run {runid} — vendors={counts$vendors} gpus={counts$gpus} runtimes={counts$runtimes} languages={counts$languages} frameworks={counts$frameworks} gpu_runtime_mappings={counts$gpu_runtime_mappings} runtime_language_mappings={counts$runtime_language_mappings} framework_compat_mappings={counts$framework_compat_mappings} — missing={missing_text} — status={status}'
  ))

  if (cc$ok) {
    append_log(glue('{format(Sys.time(), tz="UTC")}: SUCCESS {runid}'))
    quit(status = 0)
  } else {
    attempt <- attempt + 1
    if (attempt > global_max_attempts) {
      append_log(glue(
        '{format(Sys.time(), tz="UTC")}: FAILED {runid} after {global_max_attempts} attempts'
      ))
      quit(status = 2)
    } else {
      append_log(glue('{format(Sys.time(), tz="UTC")}: RETRYING {runid}'))
      Sys.sleep(2^attempt)
    }
  }
}

# fallback
quit(status = 1)
