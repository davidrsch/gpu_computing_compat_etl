library(rvest)
library(xml2)
library(dplyr)
library(tidyr)
library(purrr)
library(rlang)
library(glue)
library(tibble)

source("R/fetch_with_retry.R")

scrape_amd <- function() {
  # Canonical AMD ROCm pages to use
  urls <- list(
    gpu_support = "https://rocm.docs.amd.com/projects/install-on-linux/en/latest/reference/system-requirements.html",
    compatibility = "https://rocm.docs.amd.com/en/latest/compatibility/compatibility-matrix.html"
  )

  clean_txt <- function(x) {
    x <- gsub("[\r\n]", " ", x)
    x <- gsub("\\s+", " ", x)
    trimws(x)
  }

  fetch_html <- function(url) {
    res <- try(fetch_with_retry(url), silent = TRUE)
    if (inherits(res, "try-error")) {
      return(NULL)
    }
    doc <- try(read_html(res$path), silent = TRUE)
    if (inherits(doc, "try-error")) {
      return(NULL)
    }
    list(doc = doc, sha = res$sha256, url = url)
  }

  # 1. Scrape GPU model names from the “gpu_support” page
  gpu_res <- fetch_html(urls$gpu_support)
  if (is.null(gpu_res)) {
    abort("Failed to fetch AMD ROCm system-requirements / supported GPUs page")
  }

  # Extract table / list items from that doc
  doc <- gpu_res$doc
  cells <- c(
    doc |> html_elements("table td, table th") |> html_text2(),
    doc |> html_elements("ul li, ol li") |> html_text2()
  ) |>
    unique() |>
    clean_txt() |>
    discard(~ nchar(.x) == 0)

  # AMD GPU naming patterns (Instinct, Radeon, etc.)
  patterns <- c(
    "\\bMI[0-9]{2,3}[A-Z]?\\b",
    "Instinct\\s+MI[0-9]{2,3}[A-Z]?",
    "Radeon\\s+Pro\\s*[A-Za-z0-9-]+",
    "Radeon\\s+RX[0-9]{3,4}"
  )

  extract_models <- function(txt_vec, pats) {
    res <- pats |>
      map(
        ~ regmatches(
          txt_vec,
          gregexpr(.x, txt_vec, perl = TRUE, ignore.case = TRUE)
        )
      ) |>
      list_flatten() |>
      trimws()
    res <- gsub("(?i)^Radeon Pro\\s+", "", res, perl = TRUE)
    res <- unique(res)
    discard(res, ~ nchar(.x) == 0)
  }

  models <- extract_models(cells, patterns)
  if (length(models) < 1) {
    abort("No AMD GPU models found on support page")
  }

  gpu_raw <- tibble(
    vendor = "AMD",
    field = "model_name",
    value = models,
    source_url = gpu_res$url,
    sha256 = gpu_res$sha
  )

  # 2. From the “compatibility” page, parse mapping: which ROCm versions support which GPUs
  comp_res <- fetch_html(urls$compatibility)
  if (is.null(comp_res)) {
    # You might warn, but proceed with gpu_raw and minimal outputs
    warn("Compatibility matrix page not fetched; skipping runtime mapping")
    runtime_raw <- tibble(
      vendor = "AMD",
      field = "runtime",
      value = "ROCm",
      source_url = NA_character_,
      sha256 = NA_character_
    )
    runtime_reqs <- tibble()
    language_raw <- tibble()
    return(list(
      gpu_raw = gpu_raw,
      runtime_raw = runtime_raw,
      runtime_requirements_raw = runtime_reqs,
      language_raw = language_raw
    ))
  }

  comp_doc <- comp_res$doc

  # Get all table rows / cells as text
  comp_cells <- c(
    comp_doc |> html_elements("table td, table th") |> html_text2()
  ) |>
    unique() |>
    clean_txt() |>
    discard(~ nchar(.x) == 0)

  # On compatibility matrix, look for occurrences like “ROCm X.Y” and GPU names
  # Extract ROCm version tokens
  comp_blob <- paste(comp_cells, collapse = " ")
  ver_tokens <- regmatches(
    comp_blob,
    gregexpr(
      "ROCm\\s*[0-9]+(\\.[0-9]+)?",
      comp_blob,
      perl = TRUE,
      ignore.case = TRUE
    )
  ) |>
    list_flatten() |>
    unique()

  # Normalize
  ver_tokens <- gsub("(?i)ROCm\\s*", "ROCm ", ver_tokens, perl = TRUE)
  if (length(ver_tokens) == 0) {
    ver_tokens <- "ROCm"
  }

  runtime_raw <- tibble(
    vendor = "AMD",
    field = "runtime",
    value = ver_tokens,
    source_url = comp_res$url,
    sha256 = comp_res$sha
  )

  # For runtime_requirements, map each GPU model (from gpu_raw) to min ROCm version
  # Simplest heuristic: if model name appears in the compatibility page’s cell along with a ROCm version, that model is supported from that version.
  reqs <- list()
  for (m in gpu_raw$value) {
    # find compatibility lines mentioning this model
    matches <- comp_cells[grepl(m, comp_cells, ignore.case = TRUE)]
    if (length(matches) == 0) {
      next
    }
    # From those matching cells, attempt to extract ROCm version tokens
    vms <- regmatches(
      matches,
      gregexpr(
        "ROCm\\s*[0-9]+(\\.[0-9]+)?",
        matches,
        perl = TRUE,
        ignore.case = TRUE
      )
    ) |>
      list_flatten() |>
      unique()
    vms <- gsub("(?i)ROCm\\s*", "ROCm ", vms, perl = TRUE)
    min_v <- if (length(vms)) vms[1] else NA_character_
    reqs[[length(reqs) + 1]] <- tibble(
      vendor = "AMD",
      model_name = m,
      runtime_name = "ROCm",
      min_version = min_v,
      max_version = NA_character_,
      notes = ifelse(is.na(min_v), "no version found", glue(">= {min_v}")),
      source_url = comp_res$url,
      sha256 = comp_res$sha
    )
  }
  runtime_requirements_raw <- if (length(reqs) > 0) {
    bind_rows(reqs) |>
      distinct(vendor, model_name, .keep_all = TRUE)
  } else {
    tibble(
      vendor = character(),
      model_name = character(),
      runtime_name = character(),
      min_version = character(),
      max_version = character(),
      notes = character(),
      source_url = character(),
      sha256 = character()
    )
  }

  # 3. (Optional) you might parse language hints from compatibility page body
  language_raw <- tibble() # skip or leave empty, unless needed

  list(
    gpu_raw = gpu_raw,
    runtime_raw = runtime_raw,
    runtime_requirements_raw = runtime_requirements_raw,
    language_raw = language_raw
  )
}
