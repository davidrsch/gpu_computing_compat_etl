library(rvest)
library(tibble)
library(dplyr)
library(purrr)
source('R/fetch_with_retry.R')
source('R/scraping/frameworks/scrape_utils.R')

scrape_pytorch <- function() {

  pt_urls <- c(
    'https://pytorch.org/get-started/previous-versions/',
    'https://pytorch.org/get-started/locally/'
  )
  pt_lang_base <- c()
  pt_lang_versioned <- c()
  pt_runtime_tokens <- c()
  pt_sha <- NA_character_
  pt_src <- pt_urls[1]
  pt_rt_versions_list <- list()

  for (u in pt_urls) {
    res <- try(fetch_with_retry(u), silent = TRUE)
    if (inherits(res, 'try-error')) next
    doc <- try(read_html(res$path), silent = TRUE)
    if (inherits(doc, 'try-error')) next
    pt_sha <- res$sha256
    pt_src <- u
    
    # Extract cells for language and runtime token detection
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

    # Extract runtime versions from the same document
    # Tables with PyTorch/CUDA/ROCm/Python headers
    pt_rt_versions_list <- extract_table_versions(doc, 'pytorch', c('pytorch', 'torch'), u, res$sha256, pt_rt_versions_list)

    # Extract cells for runtime version detection (includes additional elements)
    cells_extended <- c(doc |> html_elements('table td, table th, ul li, ol li, p, code') |> html_text2())
    cells_extended <- unique(clean_txt(cells_extended))
    cu <- unique(unlist(regmatches(cells_extended, gregexpr('CUDA[[:space:]]*[0-9]+(\\.[0-9]+)?', cells_extended, perl = TRUE, ignore.case = TRUE))))
    ro <- unique(unlist(regmatches(cells_extended, gregexpr('ROCm[[:space:]]*[0-9]+(\\.[0-9]+)?', cells_extended, perl = TRUE, ignore.case = TRUE))))
    pyv <- unique(unlist(regmatches(cells_extended, gregexpr('Python[[:space:]]*[0-9]+(\\.[0-9]+)+', cells_extended, perl = TRUE, ignore.case = TRUE))))
    fwv <- unique(unlist(regmatches(cells_extended, gregexpr('(?i)(torch|pytorch)[^0-9]*([0-9]+(\\.[0-9]+)+)', cells_extended, perl = TRUE))))

    pt_rt_versions_list <- extract_runtime_versions(cu, "CUDA", "(?i)cuda", "pytorch", fwv, pyv, u, res$sha256, pt_rt_versions_list)
    pt_rt_versions_list <- extract_runtime_versions(ro, "ROCM", "(?i)rocm", "pytorch", fwv, pyv, u, res$sha256, pt_rt_versions_list)
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
