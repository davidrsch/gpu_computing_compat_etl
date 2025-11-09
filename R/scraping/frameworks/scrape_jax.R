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

  for (u in jax_urls) {
    res <- try(fetch_with_retry(u), silent = TRUE)
    if (inherits(res, 'try-error')) next
    doc <- try(read_html(res$path), silent = TRUE)
    if (inherits(doc, 'try-error')) next
    jax_sha <- res$sha256
    jax_src <- u
    cells <- c(doc |> html_elements('table td, table th, ul li, ol li, p, code') |> html_text2())
    cells <- unique(clean_txt(cells))
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
    if (any(grepl('CUDA', cells, ignore.case = TRUE))) jax_runtime_tokens <- c(jax_runtime_tokens, 'CUDA')
    if (any(grepl('ROCm', cells, ignore.case = TRUE))) jax_runtime_tokens <- c(jax_runtime_tokens, 'ROCm')
  }
  framework_raw <- tibble(
    framework = 'jax',
    source_url = jax_src,
    sha256 = jax_sha,
    languages = collapse_uniq(jax_lang_base),
    runtimes = collapse_uniq(jax_runtime_tokens)
  )

  jax_rt_versions_list <- list()
  for (u in jax_urls) {
    res <- try(fetch_with_retry(u), silent = TRUE)
    if (inherits(res, 'try-error')) next
    doc <- try(read_html(res$path), silent = TRUE)
    if (inherits(doc, 'try-error')) next

    try({
      tables <- doc |> html_elements('table')
      for (tbl in tables) {
        headers <- tbl |> html_elements('thead th, tr:first-child th') |> html_text2() |> clean_txt()
        if (length(headers) == 0) next
        has_fw <- any(grepl('jax', headers, ignore.case = TRUE))
        has_cuda <- any(grepl('cuda', headers, ignore.case = TRUE))
        has_rocm <- any(grepl('rocm', headers, ignore.case = TRUE))
        has_py <- any(grepl('python', headers, ignore.case = TRUE))
        if (!((has_cuda || has_rocm) && has_py)) next
        fw_ix <- if (has_fw) which(grepl('jax', headers, ignore.case = TRUE))[1] else NA_integer_
        cu_ix <- if (has_cuda) which(grepl('cuda', headers, ignore.case = TRUE))[1] else NA_integer_
        ro_ix <- if (has_rocm) which(grepl('rocm', headers, ignore.case = TRUE))[1] else NA_integer_
        py_ix <- which(grepl('python', headers, ignore.case = TRUE))[1]
        rows <- tbl |> html_elements('tbody tr')
        if (length(rows) == 0) rows <- tbl |> html_elements('tr')
        for (r in rows) {
          cells <- r |> html_elements('th, td') |> html_text2() |> clean_txt()
          if (length(cells) < max(na.omit(c(cu_ix, ro_ix, py_ix)))) next
          fwv <- if (!is.na(fw_ix)) cells[fw_ix] else NA_character_
          fwv_num <- NA_character_
          if (!is.na(fwv)) {
            mm <- regmatches(fwv, regexpr('([0-9]+(\\.[0-9]+)+)', fwv, perl = TRUE))
            if (length(mm) > 0) fwv_num <- mm
          }
          pyv <- if (!is.na(py_ix)) cells[py_ix] else NA_character_
          pyv_num <- if (!is.na(pyv)) trimws(gsub('(?i)python', '', pyv, perl = TRUE)) else NA_character_
          if (!is.na(cu_ix)) {
            cuv <- cells[cu_ix]
            cuv_num <- trimws(gsub('(?i)cuda', '', cuv, perl = TRUE))
            if (nchar(cuv_num) > 0) jax_rt_versions_list[[length(jax_rt_versions_list) + 1]] <- tibble(framework='jax',framework_version=fwv_num,runtime_name='CUDA',runtime_version=cuv_num,python_version=pyv_num,source_url=u,sha256=res$sha256)
          }
          if (!is.na(ro_ix)) {
            rov <- cells[ro_ix]
            rov_num <- trimws(gsub('(?i)rocm', '', rov, perl = TRUE))
            if (nchar(rov_num) > 0) jax_rt_versions_list[[length(jax_rt_versions_list) + 1]] <- tibble(framework='jax',framework_version=fwv_num,runtime_name='ROCM',runtime_version=rov_num,python_version=pyv_num,source_url=u,sha256=res$sha256)
          }
        }
      }
    }, silent = TRUE)

    cells <- c(doc |> html_elements('table td, table th, ul li, ol li, p, code') |> html_text2())
    cells <- unique(clean_txt(cells))
    cu <- unique(unlist(regmatches(cells, gregexpr('CUDA[[:space:]]*[0-9]+(\\.[0-9]+)?', cells, perl = TRUE, ignore.case = TRUE))))
    ro <- unique(unlist(regmatches(cells, gregexpr('ROCm[[:space:]]*[0-9]+(\\.[0-9]+)?', cells, perl = TRUE, ignore.case = TRUE))))
    pyv <- unique(unlist(regmatches(cells, gregexpr('Python[[:space:]]*[0-9]+(\\.[0-9]+)+', cells, perl = TRUE, ignore.case = TRUE))))
    fwv <- unique(unlist(regmatches(cells, gregexpr('(?i)jax[^0-9]*([0-9]+(\\.[0-9]+)+)', cells, perl = TRUE))))

    jax_rt_versions_list <- extract_runtime_versions(cu, 'CUDA', '(?i)cuda', 'jax', fwv, pyv, u, res$sha256, jax_rt_versions_list)
    jax_rt_versions_list <- extract_runtime_versions(ro, 'ROCM', '(?i)rocm', 'jax', fwv, pyv, u, res$sha256, jax_rt_versions_list)
  }
  
  jax_rt_versions <- if (length(jax_rt_versions_list) > 0) bind_rows(jax_rt_versions_list) else tibble()

  framework_matrix_raw <- jax_rt_versions |> distinct(framework, runtime_name, runtime_version, python_version, framework_version, .keep_all = TRUE)

  language_raw_list <- list()
  add_lang_rows <- function(tokens, src, sh) {
    if (length(tokens) == 0) return(list())
    lapply(unique(tokens), function(tk) {
      tibble(language = tolower(trimws(tk)), source_url = src, sha256 = sh)
    })
  }
  language_raw_list <- c(language_raw_list, add_lang_rows(jax_lang_versioned, jax_src, jax_sha))
  language_raw <- if (length(language_raw_list) > 0) bind_rows(language_raw_list) else tibble()
  if (nrow(language_raw) == 0) {
    language_raw <- tibble(language = 'python: python 3.8+', source_url = framework_raw$source_url[1], sha256 = framework_raw$sha256[1])
  }

  list(
    framework_raw = framework_raw,
    language_raw = language_raw,
    framework_matrix_raw = framework_matrix_raw
  )
