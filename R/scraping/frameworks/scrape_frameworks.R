library(tibble)
library(dplyr)

# Aggregate per-framework scrapers by sourcing and merging their outputs
scrape_frameworks <- function() {
  # Source per-framework scrapers
  source('R/scraping/frameworks/scrape_pytorch.R')
  source('R/scraping/frameworks/scrape_tensorflow.R')
  source('R/scraping/frameworks/scrape_jax.R')

  collectors <- list(
    pytorch = function() try(scrape_pytorch(), silent = TRUE),
    tensorflow = function() try(scrape_tensorflow(), silent = TRUE),
    jax = function() try(scrape_jax(), silent = TRUE)
  )

  framework_raw <- tibble()
  language_raw <- tibble()
  framework_matrix_raw <- tibble()

  for (nm in names(collectors)) {
    res <- collectors[[nm]]()
    if (inherits(res, 'try-error') || is.null(res)) {
      next
    }
    # Bind framework rows
    if (!is.null(res$framework_raw) && nrow(res$framework_raw) > 0) {
      framework_raw <- bind_rows(framework_raw, res$framework_raw)
    }
    # Bind language evidence
    if (!is.null(res$language_raw) && nrow(res$language_raw) > 0) {
      language_raw <- bind_rows(language_raw, res$language_raw)
    }
    # Bind detailed matrices
    if (!is.null(res$framework_matrix_raw) && nrow(res$framework_matrix_raw) > 0) {
      framework_matrix_raw <- bind_rows(framework_matrix_raw, res$framework_matrix_raw)
    }
  }

  # De-duplicate and ensure expected columns
  if (nrow(framework_raw) > 0) {
    framework_raw <- framework_raw |>
      distinct(framework, .keep_all = TRUE)
  } else {
    framework_raw <- tibble(
      framework = character(),
      source_url = character(),
      sha256 = character(),
      languages = character(),
      runtimes = character()
    )
  }

  if (nrow(language_raw) > 0) {
    language_raw <- language_raw |>
      distinct(language, source_url, .keep_all = TRUE)
  } else {
    # minimal fallback to ensure non-empty set if upstream provided none
    language_raw <- tibble(
      language = 'python: python 3.8+',
      source_url = if (nrow(framework_raw) > 0) framework_raw$source_url[1] else NA_character_,
      sha256 = if (nrow(framework_raw) > 0) framework_raw$sha256[1] else NA_character_
    )
  }

  if (nrow(framework_matrix_raw) > 0) {
    framework_matrix_raw <- framework_matrix_raw |>
      mutate(
        python_version = ifelse(is.null(python_version), NA_character_, python_version),
        framework_version = ifelse(is.null(framework_version), NA_character_, framework_version)
      ) |>
      distinct(
        framework,
        runtime_name,
        runtime_version,
        python_version,
        framework_version,
        .keep_all = TRUE
      )
  } else {
    framework_matrix_raw <- tibble(
      framework = character(),
      framework_version = character(),
      runtime_name = character(),
      runtime_version = character(),
      python_version = character(),
      source_url = character(),
      sha256 = character()
    )
  }

  list(
    framework_raw = framework_raw,
    language_raw = language_raw,
    framework_matrix_raw = framework_matrix_raw
  )
}

