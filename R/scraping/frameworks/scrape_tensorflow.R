library(rvest)
library(tibble)
library(dplyr)
library(purrr)
source('R/fetch_with_retry.R')

scrape_tensorflow <- function() {
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
  # Expand Python version ranges like "Python 3.9–3.13" into explicit minor versions
  expand_python_range <- function(start_v, end_v) {
    sv <- as.character(start_v)
    ev <- as.character(end_v)
    # parse major/minor as integers
    smaj <- suppressWarnings(as.integer(sub('^([0-9]+)\\..*$', '\\1', sv)))
    smin <- suppressWarnings(as.integer(sub('^[0-9]+\\.([0-9]+).*$', '\\1', sv)))
    emaj <- suppressWarnings(as.integer(sub('^([0-9]+)\\..*$', '\\1', ev)))
    emin <- suppressWarnings(as.integer(sub('^[0-9]+\\.([0-9]+).*$', '\\1', ev)))
    if (is.na(smaj) || is.na(smin) || is.na(emaj) || is.na(emin)) return(character())
    if (smaj != emaj) {
      # Different majors — be conservative and include just the endpoints
      return(c(paste0(smaj, '.', smin), paste0(emaj, '.', emin)))
    }
    if (emin < smin) return(character())
    vapply(smin:emin, function(m) paste0(smaj, '.', m), character(1))
  }

  tf_urls <- c(
    'https://www.tensorflow.org/install/source#tested_build_configurations',
    'https://www.tensorflow.org/install/pip'
  )
  tf_lang_base <- c()
  tf_lang_versioned <- c()
  tf_runtime_tokens <- c()
  tf_sha <- NA_character_
  tf_src <- tf_urls[1]

  for (u in tf_urls) {
    res <- try(fetch_with_retry(u), silent = TRUE)
    if (inherits(res, 'try-error')) next
    doc <- try(read_html(res$path), silent = TRUE)
    if (inherits(doc, 'try-error')) next
    tf_sha <- res$sha256
    tf_src <- u
    cells <- c(doc |> html_elements('table td, table th, ul li, ol li, p, code') |> html_text2())
    cells <- unique(clean_txt(cells))
    if (any(grepl('Python', cells, ignore.case = TRUE))) tf_lang_base <- c(tf_lang_base, 'python')
    if (any(grepl('C\\+\\+', cells, ignore.case = TRUE))) tf_lang_base <- c(tf_lang_base, 'c++')
    # Python explicit versions like "Python 3.10", optionally ending with +
    py_explicit <- unique(unlist(regmatches(
      cells,
      gregexpr(
        'Python[[:space:]]*[0-9]+(\\.[0-9]+)+(\\+)?',
        cells,
        perl = TRUE,
        ignore.case = TRUE
      )
    )))
    # Python ranges like "Python 3.9–3.13" or with hyphen
    py_ranges <- unique(unlist(regmatches(
      cells,
      gregexpr(
        'Python[[:space:]]*([0-9]+\\.[0-9]+)[[:space:]]*[-\u2013\u2014\u2012][[:space:]]*([0-9]+\\.[0-9]+)',
        cells,
        perl = TRUE,
        ignore.case = TRUE
      )
    )))
    if (length(py_explicit) > 0) tf_lang_versioned <- c(tf_lang_versioned, tolower(paste0('python: ', trimws(py_explicit))))
    # Expand ranges into individual versions
    if (length(py_ranges) > 0) {
      for (rng in py_ranges) {
        m <- regexec('(?i)Python[[:space:]]*([0-9]+\\.[0-9]+)[[:space:]]*[-\u2013\u2014\u2012][[:space:]]*([0-9]+\\.[0-9]+)', rng, perl = TRUE)
        mm <- regmatches(rng, m)[[1]]
        if (length(mm) >= 3) {
          seqv <- expand_python_range(mm[2], mm[3])
          if (length(seqv) > 0) tf_lang_versioned <- c(tf_lang_versioned, tolower(paste0('python: python ', seqv)))
        }
      }
    }
    if (any(grepl('CUDA', cells, ignore.case = TRUE))) tf_runtime_tokens <- c(tf_runtime_tokens, 'CUDA')
    if (any(grepl('ROCm', cells, ignore.case = TRUE))) tf_runtime_tokens <- c(tf_runtime_tokens, 'ROCm')
  }
  # Defensive fallback: if both pages failed or yielded nothing, ensure a minimal row
  if ((length(tf_lang_base) == 0) && (length(tf_runtime_tokens) == 0)) {
    if (is.na(tf_sha) || nchar(tf_sha) == 0) tf_sha <- 'fallback'
    if (is.na(tf_src) || nchar(tf_src) == 0) tf_src <- 'https://www.tensorflow.org/install'
    tf_lang_base <- c('python')
    # TensorFlow supports CUDA and ROCm depending on platform; include both tokens to seed evidence mapping
    tf_runtime_tokens <- c('CUDA', 'ROCm')
  }
  framework_raw <- tibble(
    framework = 'tensorflow',
    source_url = tf_src,
    sha256 = tf_sha,
    languages = collapse_uniq(tf_lang_base),
    runtimes = collapse_uniq(tf_runtime_tokens)
  )

  tf_rt_versions <- tibble(
    framework = character(),
    framework_version = character(),
    runtime_name = character(),
    runtime_version = character(),
    python_version = character(),
    source_url = character(),
    sha256 = character()
  )
  for (u in tf_urls) {
    res <- try(fetch_with_retry(u), silent = TRUE)
    if (inherits(res, 'try-error')) next
    doc <- try(read_html(res$path), silent = TRUE)
    if (inherits(doc, 'try-error')) next

    try({
      tables <- doc |> html_elements('table')
      for (tbl in tables) {
        headers <- tbl |> html_elements('thead th, tr:first-child th') |> html_text2() |> clean_txt()
        if (length(headers) == 0) next
        # Some TF tables do not include an explicit 'TensorFlow' header column; focus on Python/CUDA/ROCm presence
        has_cuda <- any(grepl('cuda', headers, ignore.case = TRUE))
        has_rocm <- any(grepl('rocm', headers, ignore.case = TRUE))
        has_py <- any(grepl('python', headers, ignore.case = TRUE))
        if (!((has_cuda || has_rocm) && has_py)) next
        fw_ix <- which(grepl('tensorflow', headers, ignore.case = TRUE))[1]
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
            # Allow Unicode punctuation like CUDA® 12.2
            cuv_num <- trimws(gsub('^.*?([0-9]+(\\.[0-9]+)?)$','\\1', gsub('(?i)cuda[^0-9]{0,5}', '', cuv, perl = TRUE)))
            if (nchar(cuv_num) > 0) tf_rt_versions <- bind_rows(tf_rt_versions, tibble(framework='tensorflow',framework_version=fwv_num,runtime_name='CUDA',runtime_version=cuv_num,python_version=pyv_num,source_url=u,sha256=res$sha256))
          }
          if (!is.na(ro_ix)) {
            rov <- cells[ro_ix]
            rov_num <- trimws(gsub('^.*?([0-9]+(\\.[0-9]+)?)$','\\1', gsub('(?i)rocm[^0-9]{0,5}', '', rov, perl = TRUE)))
            if (nchar(rov_num) > 0) tf_rt_versions <- bind_rows(tf_rt_versions, tibble(framework='tensorflow',framework_version=fwv_num,runtime_name='ROCM',runtime_version=rov_num,python_version=pyv_num,source_url=u,sha256=res$sha256))
          }
        }
      }
    }, silent = TRUE)

    cells <- c(doc |> html_elements('table td, table th, ul li, ol li, p, code') |> html_text2())
    cells <- unique(clean_txt(cells))
    # Robust extraction allowing symbols between token and version
    cu <- unique(unlist(regmatches(cells, gregexpr('(?i)CUDA[^0-9]{0,5}[0-9]+(\\.[0-9]+)?', cells, perl = TRUE, ignore.case = TRUE))))
    ro <- unique(unlist(regmatches(cells, gregexpr('(?i)ROCm[^0-9]{0,5}[0-9]+(\\.[0-9]+)?', cells, perl = TRUE, ignore.case = TRUE))))
    pyv <- unique(unlist(regmatches(cells, gregexpr('(?i)Python[[:space:]]*[0-9]+(\\.[0-9]+)+', cells, perl = TRUE, ignore.case = TRUE))))
    fwv <- unique(unlist(regmatches(cells, gregexpr('(?i)tensorflow[^0-9]*([0-9]+(\\.[0-9]+)+)', cells, perl = TRUE))))

    for (x in cu) {
      # Strip token and any leading non-digits to leave version
      v <- trimws(gsub('^.*?([0-9]+(\\.[0-9]+)?)$','\\1', gsub('(?i)cuda[^0-9]{0,5}', '', x, perl = TRUE)))
      pyv_clean <- unique(trimws(gsub('(?i)python', '', pyv, perl = TRUE)))
      fwv_num <- NA_character_
      if (length(fwv) > 0) {
        m <- regexec('([0-9]+(\\.[0-9]+)+)', fwv[1])
        mm <- regmatches(fwv[1], m)[[1]]
        if (length(mm) >= 2) fwv_num <- mm[2]
      }
      if (nchar(v) > 0) {
        if (length(pyv_clean) > 0) {
          for (pv in pyv_clean) tf_rt_versions <- bind_rows(tf_rt_versions, tibble(framework='tensorflow',framework_version=fwv_num,runtime_name='CUDA',runtime_version=v,python_version=trimws(pv),source_url=u,sha256=res$sha256))
        } else {
          tf_rt_versions <- bind_rows(tf_rt_versions, tibble(framework='tensorflow',framework_version=fwv_num,runtime_name='CUDA',runtime_version=v,python_version=NA_character_,source_url=u,sha256=res$sha256))
        }
      }
    }
    for (x in ro) {
      v <- trimws(gsub('^.*?([0-9]+(\\.[0-9]+)?)$','\\1', gsub('(?i)rocm[^0-9]{0,5}', '', x, perl = TRUE)))
      pyv_clean <- unique(trimws(gsub('(?i)python', '', pyv, perl = TRUE)))
      fwv_num <- NA_character_
      if (length(fwv) > 0) {
        m <- regexec('([0-9]+(\\.[0-9]+)+)', fwv[1])
        mm <- regmatches(fwv[1], m)[[1]]
        if (length(mm) >= 2) fwv_num <- mm[2]
      }
      if (nchar(v) > 0) {
        if (length(pyv_clean) > 0) {
          for (pv in pyv_clean) tf_rt_versions <- bind_rows(tf_rt_versions, tibble(framework='tensorflow',framework_version=fwv_num,runtime_name='ROCM',runtime_version=v,python_version=trimws(pv),source_url=u,sha256=res$sha256))
        } else {
          tf_rt_versions <- bind_rows(tf_rt_versions, tibble(framework='tensorflow',framework_version=fwv_num,runtime_name='ROCM',runtime_version=v,python_version=NA_character_,source_url=u,sha256=res$sha256))
        }
      }
    }
  }

  framework_matrix_raw <- tf_rt_versions |> distinct(framework, runtime_name, runtime_version, python_version, framework_version, .keep_all = TRUE)

  language_raw <- tibble()
  add_lang_rows <- function(language_raw, tokens, src, sh) {
    if (length(tokens) == 0) return(language_raw)
    for (tk in unique(tokens)) {
      language_raw <- bind_rows(language_raw, tibble(language = tolower(trimws(tk)), source_url = src, sha256 = sh))
    }
    language_raw
  }
  language_raw <- add_lang_rows(language_raw, tf_lang_versioned, tf_src, tf_sha)
  if (nrow(language_raw) == 0) {
    language_raw <- tibble(language = 'python: python 3.8+', source_url = framework_raw$source_url[1], sha256 = framework_raw$sha256[1])
  }

  list(
    framework_raw = framework_raw,
    language_raw = language_raw,
    framework_matrix_raw = framework_matrix_raw
  )
}
