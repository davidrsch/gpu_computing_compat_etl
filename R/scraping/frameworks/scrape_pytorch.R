library(rvest)
library(tibble)
library(dplyr)
library(purrr)
source('R/fetch_with_retry.R')

scrape_pytorch <- function() {
  clean_txt <- function(x) {
    x <- gsub('\n|\r', ' ', x)
    x <- gsub('\\s+', ' ', trimws(x))
    x
  }
  collapse_uniq <- function(x) {
    x <- unique(trimws(x))
    x <- x[nchar(x) > 0]
    if (length(x) == 0) NA_character_ else paste(x, collapse = ';')
  }

  pt_urls <- c(
    'https://pytorch.org/get-started/previous-versions/',
    'https://pytorch.org/get-started/locally/'
  )
  pt_lang_base <- c()
  pt_lang_versioned <- c()
  pt_runtime_tokens <- c()
  pt_sha <- NA_character_
  pt_src <- pt_urls[1]

  for (u in pt_urls) {
    res <- try(fetch_with_retry(u), silent = TRUE)
    if (inherits(res, 'try-error')) next
    doc <- try(read_html(res$path), silent = TRUE)
    if (inherits(doc, 'try-error')) next
    pt_sha <- res$sha256
    pt_src <- u
    cells <- c(doc |> html_elements('table td, table th, ul li, ol li') |> html_text2())
    cells <- unique(clean_txt(cells))

    if (any(grepl('Python', cells, ignore.case = TRUE))) pt_lang_base <- c(pt_lang_base, 'python')
    if (any(grepl('C\\+\\+', cells, ignore.case = TRUE))) pt_lang_base <- c(pt_lang_base, 'c++')

    py <- unique(unlist(regmatches(
      cells,
      gregexpr(
        'Python[[:space:]]*[0-9]+(\\.[0-9]+)?(\\+)?',
        cells,
        perl = TRUE,
        ignore.case = TRUE
      )
    )))
    if (length(py) > 0) pt_lang_versioned <- c(pt_lang_versioned, tolower(paste0('python: ', trimws(py))))

    cpp <- unique(unlist(regmatches(
      cells,
      gregexpr(
        'C\\+\\+[[:space:]]*[0-9]{2,4}',
        cells,
        perl = TRUE,
        ignore.case = TRUE
      )
    )))
    if (length(cpp) > 0) pt_lang_versioned <- c(pt_lang_versioned, tolower(paste0('c++: ', trimws(cpp))))

    if (any(grepl('CUDA', cells, ignore.case = TRUE))) pt_runtime_tokens <- c(pt_runtime_tokens, 'CUDA')
    if (any(grepl('ROCm', cells, ignore.case = TRUE))) pt_runtime_tokens <- c(pt_runtime_tokens, 'ROCm')
  }
  pt_lang_base <- unique(pt_lang_base)
  pt_runtime_tokens <- unique(pt_runtime_tokens)

  framework_raw <- tibble(
    framework = 'pytorch',
    source_url = pt_src,
    sha256 = pt_sha,
    languages = collapse_uniq(pt_lang_base),
    runtimes = collapse_uniq(pt_runtime_tokens)
  )

  pt_rt_versions_list <- list()
  for (u in pt_urls) {
    res <- try(fetch_with_retry(u), silent = TRUE)
    if (inherits(res, 'try-error')) next
    doc <- try(read_html(res$path), silent = TRUE)
    if (inherits(doc, 'try-error')) next

    # Tables with PyTorch/CUDA/ROCm/Python headers
    try({
      tables <- doc |> html_elements('table')
      for (tbl in tables) {
        headers <- tbl |> html_elements('thead th, tr:first-child th') |> html_text2() |> clean_txt()
        if (length(headers) == 0) next
        has_fw <- any(grepl('pytorch|torch', headers, ignore.case = TRUE))
        has_cuda <- any(grepl('cuda', headers, ignore.case = TRUE))
        has_rocm <- any(grepl('rocm', headers, ignore.case = TRUE))
        has_py <- any(grepl('python', headers, ignore.case = TRUE))
        if (!(has_fw && (has_cuda || has_rocm) && has_py)) next
        fw_ix <- which(grepl('pytorch|torch', headers, ignore.case = TRUE))[1]
        cu_ix <- if (has_cuda) which(grepl('cuda', headers, ignore.case = TRUE))[1] else NA_integer_
        ro_ix <- if (has_rocm) which(grepl('rocm', headers, ignore.case = TRUE))[1] else NA_integer_
        py_ix <- which(grepl('python', headers, ignore.case = TRUE))[1]
        rows <- tbl |> html_elements('tbody tr')
        if (length(rows) == 0) rows <- tbl |> html_elements('tr')
        for (r in rows) {
          cells <- r |> html_elements('th, td') |> html_text2() |> clean_txt()
          if (length(cells) < max(na.omit(c(fw_ix, cu_ix, ro_ix, py_ix)))) next
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
            if (nchar(cuv_num) > 0) pt_rt_versions_list[[length(pt_rt_versions_list) + 1]] <- tibble(framework='pytorch',framework_version=fwv_num,runtime_name='CUDA',runtime_version=cuv_num,python_version=pyv_num,source_url=u,sha256=res$sha256)
          }
          if (!is.na(ro_ix)) {
            rov <- cells[ro_ix]
            rov_num <- trimws(gsub('(?i)rocm', '', rov, perl = TRUE))
            if (nchar(rov_num) > 0) pt_rt_versions_list[[length(pt_rt_versions_list) + 1]] <- tibble(framework='pytorch',framework_version=fwv_num,runtime_name='ROCM',runtime_version=rov_num,python_version=pyv_num,source_url=u,sha256=res$sha256)
          }
        }
      }
    }, silent = TRUE)

    cells <- c(doc |> html_elements('table td, table th, ul li, ol li, p, code') |> html_text2())
    cells <- unique(clean_txt(cells))
    cu <- unique(unlist(regmatches(cells, gregexpr('CUDA[[:space:]]*[0-9]+(\\.[0-9]+)?', cells, perl = TRUE, ignore.case = TRUE))))
    ro <- unique(unlist(regmatches(cells, gregexpr('ROCm[[:space:]]*[0-9]+(\\.[0-9]+)?', cells, perl = TRUE, ignore.case = TRUE))))
    pyv <- unique(unlist(regmatches(cells, gregexpr('Python[[:space:]]*[0-9]+(\\.[0-9]+)+', cells, perl = TRUE, ignore.case = TRUE))))
    fwv <- unique(unlist(regmatches(cells, gregexpr('(?i)(torch|pytorch)[^0-9]*([0-9]+(\\.[0-9]+)+)', cells, perl = TRUE))))

    extract_runtime_versions <- function(runtime_list, runtime_name, runtime_pattern, framework, fwv, pyv, u, sha256, pt_rt_versions_list) {
      for (x in runtime_list) {
        v <- trimws(gsub(runtime_pattern, '', x, perl = TRUE))
        pyv_clean <- unique(trimws(gsub('(?i)python', '', pyv, perl = TRUE)))
        fwv_num <- NA_character_
        if (length(fwv) > 0) {
          m <- regexec('([0-9]+(\\.[0-9]+)+)', fwv[1])
          mm <- regmatches(fwv[1], m)[[1]]
          if (length(mm) >= 2) fwv_num <- mm[2]
        }
        if (nchar(v) > 0) {
          if (length(pyv_clean) > 0) {
            for (pv in pyv_clean) pt_rt_versions_list[[length(pt_rt_versions_list) + 1]] <- tibble(framework=framework,framework_version=fwv_num,runtime_name=runtime_name,runtime_version=v,python_version=trimws(pv),source_url=u,sha256=sha256)
          } else {
            pt_rt_versions_list[[length(pt_rt_versions_list) + 1]] <- tibble(framework=framework,framework_version=fwv_num,runtime_name=runtime_name,runtime_version=v,python_version=NA_character_,source_url=u,sha256=sha256)
          }
        }
      }
      pt_rt_versions_list
    }

    pt_rt_versions_list <- extract_runtime_versions(cu, "CUDA", "(?i)cuda", "pytorch", fwv, pyv, u, res$sha256, pt_rt_versions_list)
    pt_rt_versions_list <- extract_runtime_versions(ro, "ROCM", "(?i)rocm", "pytorch", fwv, pyv, u, res$sha256, pt_rt_versions_list)
  }

  if (length(pt_rt_versions_list) > 0) {
    pt_rt_versions <- bind_rows(pt_rt_versions_list)
  } else {
    pt_rt_versions <- tibble(
      framework = character(),
      framework_version = character(),
      runtime_name = character(),
      runtime_version = character(),
      python_version = character(),
      source_url = character(),
      sha256 = character()
    )
  }

  framework_matrix_raw <- pt_rt_versions |> distinct(framework, runtime_name, runtime_version, python_version, framework_version, .keep_all = TRUE)

  add_lang_rows <- function(tokens, src, sh) {
    if (length(tokens) == 0) return(tibble())
    lang_list <- list()
    for (tk in unique(tokens)) {
      lang_list[[length(lang_list) + 1]] <- tibble(language = tolower(trimws(tk)), source_url = src, sha256 = sh)
    }
    bind_rows(lang_list)
  }
  language_raw <- add_lang_rows(pt_lang_versioned, pt_src, pt_sha)
  if (nrow(language_raw) == 0) {
    language_raw <- tibble(language = 'python: python 3.8+', source_url = framework_raw$source_url[1], sha256 = framework_raw$sha256[1])
  }

  list(
    framework_raw = framework_raw,
    language_raw = language_raw,
    framework_matrix_raw = framework_matrix_raw
  )
}
