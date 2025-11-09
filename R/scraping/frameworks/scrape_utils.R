# Utility functions for framework scrapers

# Normalize whitespace in text strings
# Replaces newlines and carriage returns with spaces,
# then collapses multiple spaces into one
# @param x Character vector to clean
# @return Character vector with normalized whitespace
clean_txt <- function(x) {
  x <- gsub('\n|\r', ' ', x)
  x <- gsub('\\s+', ' ', trimws(x))
  x
}

# De-duplicate and collapse vector into semicolon-separated string
# Removes duplicates, trims whitespace, filters out empty strings,
# and joins remaining values with semicolons
# @param x Character vector to process
# @return Single string with semicolon-separated unique values, or NA if none remain
collapse_uniq <- function(x) {
  x <- unique(trimws(x))
  x <- x[nchar(x) > 0]
  if (length(x) == 0) NA_character_ else paste(x, collapse = ';')
}

# Create tibble rows for language information
# Processes language tokens and creates a tibble with one row per unique token
# @param tokens Character vector of language tokens (e.g., "python: python 3.8")
# @param src Source URL
# @param sh SHA256 hash of the source
# @return Tibble with columns: language, source_url, sha256
add_lang_rows <- function(tokens, src, sh) {
  if (length(tokens) == 0) return(tibble())
  lang_list <- list()
  for (tk in unique(tokens)) {
    lang_list[[length(lang_list) + 1]] <- tibble(language = tolower(trimws(tk)), source_url = src, sha256 = sh)
  }
  bind_rows(lang_list)
}

# Extract version number from framework version strings
# Returns the first numeric version pattern found (e.g., "1.2.3")
# NOTE: Only processes the first element of fwv vector. If multiple framework
# versions are found, only the first will be used for all runtime combinations.
# @param fwv Character vector of framework version strings
# @return Single version number string or NA_character_
extract_version_number <- function(fwv) {
  fwv_num <- NA_character_
  if (length(fwv) > 0) {
    m <- regexpr('[0-9]+(\\.[0-9]+)*', fwv[1], perl = TRUE)
    if (m[1] != -1) fwv_num <- regmatches(fwv[1], m)
  }
  fwv_num
}

# Extract runtime versions (CUDA/ROCm) and create tibble rows
# This function handles the common pattern across framework scrapers of:
# 1. Extracting runtime version from text
# 2. Combining with framework version and Python versions
# 3. Creating tibble rows for each combination
#
# @param runtime_list Vector of runtime strings (e.g., "CUDA 11.2", "ROCm 5.0")
# @param runtime_name Name of the runtime (e.g., "CUDA", "ROCM")
# @param runtime_pattern Regex pattern to remove from runtime strings to extract version
# @param framework Name of the framework (e.g., "jax", "tensorflow", "pytorch")
# @param fwv Framework version strings extracted from cells
# @param pyv Python version strings extracted from cells
# @param url Source URL
# @param sha256 SHA256 hash of the source
# @param existing_list Existing list to append to
# @return Updated list with new tibble entries
extract_runtime_versions <- function(runtime_list, runtime_name, runtime_pattern, 
                                      framework, fwv, pyv, url, sha256, existing_list) {
  for (x in runtime_list) {
    v <- trimws(gsub(runtime_pattern, '', x, perl = TRUE))
    # Handle potential edge cases with complex patterns or Unicode symbols in version strings
    m <- regexpr('[0-9]+(\\.[0-9]+)*', v, perl = TRUE)
    if (m[1] != -1) {
      v <- trimws(regmatches(v, m))
    } else {
      v <- NA_character_
    }
    
    pyv_clean <- unique(trimws(gsub('(?i)python', '', pyv, perl = TRUE)))
    pyv_clean <- pyv_clean[nchar(pyv_clean) > 0]
    fwv_num <- extract_version_number(fwv)
    
    if (!is.na(v) && nchar(v) > 0) {
      if (length(pyv_clean) > 0) {
        for (pv in pyv_clean) {
          existing_list[[length(existing_list) + 1]] <- tibble(
            framework = framework,
            framework_version = fwv_num,
            runtime_name = runtime_name,
            runtime_version = v,
            python_version = trimws(pv),
            source_url = url,
            sha256 = sha256
          )
        }
      } else {
        existing_list[[length(existing_list) + 1]] <- tibble(
          framework = framework,
          framework_version = fwv_num,
          runtime_name = runtime_name,
          runtime_version = v,
          python_version = NA_character_,
          source_url = url,
          sha256 = sha256
        )
      }
    }
  }
  existing_list
}

# Extract runtime version data from HTML tables
# This function handles the common pattern across framework scrapers of parsing
# tables that contain framework version, runtime (CUDA/ROCm), Python version columns.
#
# NOTE: Errors during table extraction are silently caught with try(..., silent = TRUE)
# to allow scraping to continue even if one table has formatting issues. This is
# intentional for robustness during web scraping, though it may make debugging harder.
# If tables fail to parse, existing_list is returned unchanged.
#
# @param doc HTML document object from read_html()
# @param framework Name of the framework (e.g., "jax", "tensorflow", "pytorch")
# @param framework_patterns Vector of regex patterns to match framework column headers
# @param url Source URL
# @param sha256 SHA256 hash of the source
# @param existing_list Existing list to append to
# @return Updated list with new tibble entries
extract_table_versions <- function(doc, framework, framework_patterns, url, sha256, existing_list) {
  try({
    tables <- doc |> html_elements('table')
    for (tbl in tables) {
      headers <- tbl |> html_elements('thead th, tr:first-child th') |> html_text2() |> clean_txt()
      if (length(headers) == 0) next
      
      # Check for required columns
      has_fw <- any(sapply(framework_patterns, function(p) any(grepl(p, headers, ignore.case = TRUE))))
      has_cuda <- any(grepl('cuda', headers, ignore.case = TRUE))
      has_rocm <- any(grepl('rocm', headers, ignore.case = TRUE))
      has_py <- any(grepl('python', headers, ignore.case = TRUE))
      
      # For JAX/PyTorch: require framework column OR just runtime+python
      # For TensorFlow: just runtime+python is sufficient
      if (!(has_fw || ((has_cuda || has_rocm) && has_py))) next
      
      # Find column indices
      fw_ix <- NA_integer_
      if (has_fw) {
        for (p in framework_patterns) {
          matches <- which(grepl(p, headers, ignore.case = TRUE))
          if (length(matches) > 0) {
            fw_ix <- matches[1]
            break
          }
        }
      }
      cu_ix <- if (has_cuda) which(grepl('cuda', headers, ignore.case = TRUE))[1] else NA_integer_
      ro_ix <- if (has_rocm) which(grepl('rocm', headers, ignore.case = TRUE))[1] else NA_integer_
      py_ix <- if (has_py) which(grepl('python', headers, ignore.case = TRUE))[1] else NA_integer_
      
      # Extract rows
      rows <- tbl |> html_elements('tbody tr')
      if (length(rows) == 0) rows <- tbl |> html_elements('tr:not(thead tr)')
      
      for (r in rows) {
        cells <- r |> html_elements('th, td') |> html_text2() |> clean_txt()
        if (length(cells) < max(na.omit(c(fw_ix, cu_ix, ro_ix, py_ix)))) next
        
        # Extract framework version
        fwv <- if (!is.na(fw_ix)) cells[fw_ix] else NA_character_
        fwv_num <- NA_character_
        if (!is.na(fwv)) {
          mm <- regmatches(fwv, regexpr('[0-9]+(\\.[0-9]+)*', fwv, perl = TRUE))
          if (length(mm) > 0) fwv_num <- mm
        }
        
        # Extract Python version
        pyv <- if (!is.na(py_ix)) cells[py_ix] else NA_character_
        pyv_num <- if (!is.na(pyv)) trimws(gsub('(?i)python', '', pyv, perl = TRUE)) else NA_character_
        if (!is.na(pyv_num) && nchar(pyv_num) == 0) pyv_num <- NA_character_
        if (!is.na(pyv_num) && nchar(pyv_num) == 0) pyv_num <- NA_character_
        
        # Extract CUDA version if present
        if (!is.na(cu_ix)) {
          cuv <- cells[cu_ix]
          # Handle different cleaning patterns (TensorFlow has Unicode symbols)
          if (framework == 'tensorflow') {
            cuv_num <- trimws(gsub('(?i)cuda\\P{N}{0,5}', '', cuv, perl = TRUE))
          } else {
            cuv_num <- trimws(gsub('(?i)cuda', '', cuv, perl = TRUE))
          }
          if (nchar(cuv_num) > 0) {
            existing_list[[length(existing_list) + 1]] <- tibble(
              framework = framework,
              framework_version = fwv_num,
              runtime_name = 'CUDA',
              runtime_version = cuv_num,
              python_version = pyv_num,
              source_url = url,
              sha256 = sha256
            )
          }
        }
        
        # Extract ROCm version if present
        if (!is.na(ro_ix)) {
          rov <- cells[ro_ix]
          # Handle different cleaning patterns (TensorFlow has Unicode symbols)
          if (framework == 'tensorflow') {
            rov_num <- trimws(gsub('(?i)rocm\\P{N}{0,5}', '', rov, perl = TRUE))
          } else {
            rov_num <- trimws(gsub('(?i)rocm', '', rov, perl = TRUE))
          }
          if (nchar(rov_num) > 0) {
            existing_list[[length(existing_list) + 1]] <- tibble(
              framework = framework,
              framework_version = fwv_num,
              runtime_name = 'ROCM',
              runtime_version = rov_num,
              python_version = pyv_num,
              source_url = url,
              sha256 = sha256
            )
          }
        }
      }
    }
  }, silent = TRUE)
  
  existing_list
}
