library(rvest)
library(tibble)
library(dplyr)
library(purrr)
source('R/fetch_with_retry.R')
source('R/scraping/frameworks/scrape_utils.R')

scrape_jax <- function() {

  jax_urls <- c(
    'https://jax.readthedocs.io/en/latest/installation.html',
    'https://storage.googleapis.com/jax-releases/jax_cuda_releases.html'
  )
  jax_lang_base <- c()
  jax_lang_versioned <- c()
  jax_runtime_tokens <- c()
  jax_sha <- NA_character_
  jax_src <- jax_urls[1]

  jax_rt_versions_list <- list()
  for (u in jax_urls) {
    res <- try(fetch_with_retry(u), silent = TRUE)
    if (inherits(res, 'try-error')) next
    doc <- try(read_html(res$path), silent = TRUE)
    if (inherits(doc, 'try-error')) next
    jax_sha <- res$sha256
    jax_src <- u
    
    cells <- c(doc |> html_elements('table td, table th, ul li, ol li, p, code') |> html_text2())
    cells <- unique(clean_txt(cells))
    
    # Extract language information
    if (any(grepl('Python', cells, ignore.case = TRUE))) jax_lang_base <- c(jax_lang_base, 'python')
    if (any(grepl('C\\+\\+', cells, ignore.case = TRUE))) jax_lang_base <- c(jax_lang_base, 'c++')
    py <- unique(unlist(regmatches(
      cells,
      gregexpr(
        'Python[[:space:]]*[0-9]+(\\.[0-9]+)?(\\+)?',
        cells,
        perl = TRUE,
        ignore.case = TRUE
      )
    )))
    if (length(py) > 0) jax_lang_versioned <- c(jax_lang_versioned, tolower(paste0('python: ', trimws(py))))
    
    # Extract runtime tokens
    if (any(grepl('CUDA', cells, ignore.case = TRUE))) jax_runtime_tokens <- c(jax_runtime_tokens, 'CUDA')
    if (any(grepl('ROCm', cells, ignore.case = TRUE))) jax_runtime_tokens <- c(jax_runtime_tokens, 'ROCm')
    
    # Extract version information
    jax_rt_versions_list <- extract_table_versions(doc, 'jax', c('jax'), u, res$sha256, jax_rt_versions_list)
    
    cu <- unique(unlist(regmatches(cells, gregexpr('CUDA[[:space:]]*[0-9]+(\\.[0-9]+)?', cells, perl = TRUE, ignore.case = TRUE))))
    ro <- unique(unlist(regmatches(cells, gregexpr('ROCm[[:space:]]*[0-9]+(\\.[0-9]+)?', cells, perl = TRUE, ignore.case = TRUE))))
    pyv <- unique(unlist(regmatches(cells, gregexpr('Python[[:space:]]*[0-9]+(\\.[0-9]+)+', cells, perl = TRUE, ignore.case = TRUE))))
    fwv <- unique(unlist(regmatches(cells, gregexpr('(?i)jax[^0-9]*([0-9]+(\\.[0-9]+)+)', cells, perl = TRUE))))

    jax_rt_versions_list <- extract_runtime_versions(cu, 'CUDA', '(?i)cuda', 'jax', fwv, pyv, u, res$sha256, jax_rt_versions_list)
    jax_rt_versions_list <- extract_runtime_versions(ro, 'ROCM', '(?i)rocm', 'jax', fwv, pyv, u, res$sha256, jax_rt_versions_list)
  }
  
  framework_raw <- tibble(
    framework = 'jax',
    source_url = jax_src,
    sha256 = jax_sha,
    languages = collapse_uniq(jax_lang_base),
    runtimes = collapse_uniq(jax_runtime_tokens)
  )
  
  jax_rt_versions <- if (length(jax_rt_versions_list) > 0) bind_rows(jax_rt_versions_list) else tibble()

  framework_matrix_raw <- jax_rt_versions |> distinct(framework, runtime_name, runtime_version, python_version, framework_version, .keep_all = TRUE)

  language_raw <- add_lang_rows(jax_lang_versioned, jax_src, jax_sha)
  if (nrow(language_raw) == 0) {
    language_raw <- tibble(language = 'python: python 3.8+', source_url = framework_raw$source_url[1], sha256 = framework_raw$sha256[1])
  }

  list(
    framework_raw = framework_raw,
    language_raw = language_raw,
    framework_matrix_raw = framework_matrix_raw
  )
