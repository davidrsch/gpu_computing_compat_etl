library(rvest)
library(tibble)
library(dplyr)
library(purrr)
source('R/fetch_with_retry.R')
source('R/scraping/frameworks/scrape_utils.R')

scrape_tensorflow <- function() {
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
  tf_rt_versions_list <- list()

  for (u in tf_urls) {
    res <- try(fetch_with_retry(u), silent = TRUE)
    if (inherits(res, 'try-error')) next
    doc <- try(read_html(res$path), silent = TRUE)
    if (inherits(doc, 'try-error')) next
    tf_sha <- res$sha256
    tf_src <- u
    
    cells <- c(doc |> html_elements('table td, table th, ul li, ol li, p, code') |> html_text2())
    cells <- unique(clean_txt(cells))
    
    # Extract language information
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
    
    # Extract runtime tokens
    if (any(grepl('CUDA', cells, ignore.case = TRUE))) tf_runtime_tokens <- c(tf_runtime_tokens, 'CUDA')
    if (any(grepl('ROCm', cells, ignore.case = TRUE))) tf_runtime_tokens <- c(tf_runtime_tokens, 'ROCm')
    
    # Extract version information
    tf_rt_versions_list <- extract_table_versions(doc, 'tensorflow', c('tensorflow'), u, res$sha256, tf_rt_versions_list)
    
    # Robust extraction allowing symbols between token and version
    cu <- unique(unlist(regmatches(cells, gregexpr('(?i)CUDA[^0-9]{0,5}[0-9]+(\\.[0-9]+)?', cells, perl = TRUE, ignore.case = TRUE))))
    ro <- unique(unlist(regmatches(cells, gregexpr('(?i)ROCm[^0-9]{0,5}[0-9]+(\\.[0-9]+)?', cells, perl = TRUE, ignore.case = TRUE))))
    pyv <- unique(unlist(regmatches(cells, gregexpr('(?i)Python[[:space:]]*[0-9]+(\\.[0-9]+)+', cells, perl = TRUE, ignore.case = TRUE))))
    fwv <- unique(unlist(regmatches(cells, gregexpr('(?i)tensorflow[^0-9]*([0-9]+(\\.[0-9]+)+)', cells, perl = TRUE))))

    tf_rt_versions_list <- extract_runtime_versions(cu, 'CUDA', '(?i)cuda[^0-9]{0,5}', 'tensorflow', fwv, pyv, u, res$sha256, tf_rt_versions_list)
    tf_rt_versions_list <- extract_runtime_versions(ro, 'ROCM', '(?i)rocm[^0-9]{0,5}', 'tensorflow', fwv, pyv, u, res$sha256, tf_rt_versions_list)
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
  
  tf_rt_versions <- if (length(tf_rt_versions_list) > 0) bind_rows(tf_rt_versions_list) else tibble(
    framework = character(),
    framework_version = character(),
    runtime_name = character(),
    runtime_version = character(),
    python_version = character(),
    source_url = character(),
    sha256 = character()
  )

  framework_matrix_raw <- tf_rt_versions |> distinct(framework, runtime_name, runtime_version, python_version, framework_version, .keep_all = TRUE)

  language_raw_list <- list()
  # add_lang_rows is now defined in scrape_utils.R and returns a bound tibble
  language_raw_list <- c(language_raw_list, add_lang_rows(tf_lang_versioned, tf_src, tf_sha))
  language_raw <- if (length(language_raw_list) > 0) bind_rows(language_raw_list) else tibble()
  if (nrow(language_raw) == 0) {
    language_raw <- tibble(language = 'python: python 3.8+', source_url = framework_raw$source_url[1], sha256 = framework_raw$sha256[1])
  }

  list(
    framework_raw = framework_raw,
    language_raw = language_raw,
    framework_matrix_raw = framework_matrix_raw
  )
}
