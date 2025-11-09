library(dplyr)
library(purrr)
library(readr)
library(rlang)
library(rvest)
library(xml2)
library(tibble)
library(tidyr)
library(janitor)
library(glue)

source("R/fetch_with_retry.R")

# Quiet NSE notes
if (getRversion() >= "2.15.1") {
  utils::globalVariables(c(
    "model_name_raw",
    "compute_capability",
    "model_name"
  ))
}

scrape_nvidia <- function() {
  urls <- list(
    main = "https://developer.nvidia.com/cuda-gpus",
    legacy = "https://developer.nvidia.com/cuda-legacy-gpus",
    archive = "https://developer.nvidia.com/cuda-toolkit-archive",
    docs = "https://docs.nvidia.com/cuda/"
  )

  # --- Helpers ------------------------------------------------------------
  clean_txt <- \(x) gsub("\\s+", " ", gsub("[\r\n]", " ", trimws(x)))

  remove_br_in_xml <- \(tbl) {
    xml_find_all(tbl, ".//br") |> walk(~ xml_add_sibling(.x, "text", "|"))
    xml_find_all(tbl, ".//br") |> walk(xml_remove)
    tbl
  }

  fetch_html <- \(url) {
    res <- try(fetch_with_retry(url), silent = TRUE)
    if (inherits(res, "try-error")) {
      abort(glue("Failed to fetch URL: {url}"))
    }
    doc <- try(read_html(res$path), silent = TRUE)
    if (inherits(doc, "try-error")) {
      abort(glue("Failed to parse HTML for {url}"))
    }
    list(doc = doc, sha = res$sha256, url = url)
  }

  parse_gpu_tables <- \(doc, url, sha) {
    doc |>
      html_elements("table") |>
      map(remove_br_in_xml) |>
      map(html_table, fill = TRUE) |>
      map(clean_names) |>
      keep(~ "compute_capability" %in% names(.x)) |>
      map_dfr(\(tbl) {
        tbl |>
          pivot_longer(
            cols = -compute_capability,
            names_to = "model_category",
            values_to = "model_name_raw"
          ) |>
          transmute(
            model_name = clean_txt(model_name_raw),
            compute_capability = as.character(compute_capability),
            source_url = url,
            sha256 = sha
          )
      }) |>
      separate_rows(model_name, sep = "\\|") |>
      filter(model_name != "")
  }

  extract_models <- \(txt) {
    patterns <- c(
      "GeForce RTX [2-9][0-9]{2,3}(?: (?:Ti|SUPER))?(?: [A-Za-z]+)?",
      "GeForce GTX [0-9]{3,4}(?:M| Ti| SUPER)?",
      "\\bGTX [0-9]{3,4}(?:M| Ti| SUPER)?\\b",
      "GeForce [0-9]{3,4}M",
      "\\bRTX [0-9]{4}(?: Ti)?\\b",
      "\\bTITAN RTX\\b",
      "Quadro RTX [0-9]{4}",
      "\\bRTX A[0-9]{3,4}\\b",
      "\\bA[0-9]{2,4}\\b",
      "\\b(H100|L4|T4)\\b",
      "Jetson (?:AGX|Orin|Xavier|Nano)(?: [A-Za-z0-9]+)*"
    )

    patterns |>
      map(\(p) {
        regmatches(txt, gregexpr(p, txt, perl = TRUE, ignore.case = TRUE))
      }) |>
      list_flatten() |>
      gsub("^NVIDIA\\s+", "", x = _, ignore.case = TRUE) |>
      clean_txt() |>
      unique() |>
      (\(x) x[nchar(x) > 0])()
  }

  # --- GPU MODELS ---------------------------------------------------------
  main_doc <- fetch_html(urls$main)
  extract_text <- \(doc, selectors) {
    selectors |>
      map(\(s) doc |> html_elements(s) |> html_text2()) |>
      list_flatten()
  }

  candidates <- extract_text(
    main_doc$doc,
    c(
      "main table td, main table th",
      "main ul li, main ol li",
      ".region-content table td, .region-content table th"
    )
  )

  # Add legacy page if available
  legacy_doc <- try(fetch_html(urls$legacy), silent = TRUE)
  if (!inherits(legacy_doc, "try-error")) {
    legacy_sel <- c(
      "main table td, main table th",
      "main ul li, main ol li",
      "h1, h2, h3, h4, h5, h6"
    )
    candidates <- c(candidates, extract_text(legacy_doc$doc, legacy_sel))
  }

  candidates <- clean_txt(candidates) |> unique() |> (\(x) x[nchar(x) > 0])()

  models <- extract_models(candidates) |>
    gsub("(?i)geforce rtx rtx", "GeForce RTX", x = _, perl = TRUE) |>
    unique()

  if (length(models) < 5) {
    abort("No NVIDIA GPU models parsed from DOM tables.")
  }

  gpu_raw <- tibble(
    vendor = "NVIDIA",
    field = "model_name",
    value = models,
    source_url = urls$main,
    sha256 = main_doc$sha
  )

  # --- COMPUTE CAPABILITY -------------------------------------------------
  gpu_cc_raw <- c(urls$main, urls$legacy) |>
    map(\(u) tryCatch(fetch_html(u), error = function(e) NULL)) |>
    discard(is.null) |>
    map_dfr(\(res) parse_gpu_tables(res$doc, res$url, res$sha)) |>
    mutate(
      compute_capability = case_when(
        grepl("^[0-9]+\\.[0-9]$", compute_capability) ~ compute_capability,
        grepl("^[0-9]+$", compute_capability) ~ paste0(
          compute_capability,
          ".0"
        ),
        TRUE ~ NA_character_
      ),
      model_name = clean_txt(gsub("\u00A0", " ", model_name))
    ) |>
    filter(!is.na(compute_capability)) |>
    distinct(model_name, .keep_all = TRUE)

  # --- CUDA TOOLKIT RUNTIMES ---------------------------------------------
  archive_doc <- fetch_html(urls$archive)
  cuda_vals <- archive_doc$doc |>
    html_elements("a") |>
    html_text2() |>
    clean_txt() |>
    map(\(x) {
      regmatches(
        x,
        gregexpr(
          "CUDA( Toolkit)? [0-9]+(\\.[0-9]+)?",
          x,
          perl = TRUE,
          ignore.case = TRUE
        )
      )
    }) |>
    list_flatten() |>
    gsub("(?i)CUDA( Toolkit)?\\s+", "CUDA ", x = _, perl = TRUE) |>
    unique()

  if (length(cuda_vals) == 0) {
    cuda_vals <- "CUDA"
  }

  runtime_raw <- tibble(
    vendor = "NVIDIA",
    field = "runtime",
    value = cuda_vals,
    source_url = urls$archive,
    sha256 = archive_doc$sha
  )

  # --- LANGUAGE DETECTION -------------------------------------------------
  language_raw <- try(
    {
      docs_doc <- fetch_html(urls$docs)
      text <- docs_doc$doc |> html_elements("body") |> html_text2()

      find_langs <- \(pattern, prefix) {
        regmatches(
          text,
          gregexpr(pattern, text, perl = TRUE, ignore.case = TRUE)
        ) |>
          list_flatten() |>
          unique() |>
          (\(x) {
            tibble(
              language = paste0(prefix, ": ", tolower(trimws(x))),
              source_url = urls$docs,
              sha256 = docs_doc$sha
            )
          })()
      }

      bind_rows(
        find_langs("Python[[:space:]]*[0-9]+(\\.[0-9]+)?(\\+)?", "python"),
        find_langs("C\\+\\+[[:space:]]*[0-9]{2,4}", "c++")
      )
    },
    silent = TRUE
  )

  if (inherits(language_raw, "try-error")) {
    language_raw <- tibble()
  }

  # --- RETURN -------------------------------------------------------------
  list(
    gpu_raw = gpu_raw,
    gpu_cc_raw = gpu_cc_raw,
    runtime_raw = runtime_raw,
    language_raw = language_raw
  )
}
