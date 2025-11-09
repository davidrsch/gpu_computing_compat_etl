# Utility functions for framework scrapers

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

# Extract version number from framework version strings
# Returns the first numeric version pattern found (e.g., "1.2.3")
extract_version_number <- function(fwv) {
  fwv_num <- NA_character_
  if (length(fwv) > 0) {
    m <- regexec('([0-9]+(\\.[0-9]+)*)', fwv[1])
    mm <- regmatches(fwv[1], m)[[1]]
    if (length(mm) >= 2) fwv_num <- mm[2]
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
    # Handle potential edge cases with complex patterns (e.g., TensorFlow's Unicode symbols)
    v <- trimws(gsub('^.*?([0-9]+(\\.[0-9]+)?)$', '\\1', v))
    
    pyv_clean <- unique(trimws(gsub('(?i)python', '', pyv, perl = TRUE)))
    fwv_num <- extract_version_number(fwv)
    
    if (nchar(v) > 0) {
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
      if (!((has_cuda || has_rocm) && has_py)) next
      
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
      py_ix <- which(grepl('python', headers, ignore.case = TRUE))[1]
      
      # Extract rows
      rows <- tbl |> html_elements('tbody tr')
      if (length(rows) == 0) rows <- tbl |> html_elements('tr')
      
      for (r in rows) {
        cells <- r |> html_elements('th, td') |> html_text2() |> clean_txt()
        if (length(cells) < max(na.omit(c(fw_ix, cu_ix, ro_ix, py_ix)))) next
        
        # Extract framework version
        fwv <- if (!is.na(fw_ix)) cells[fw_ix] else NA_character_
        fwv_num <- NA_character_
        if (!is.na(fwv)) {
          mm <- regmatches(fwv, regexpr('([0-9]+(\\.[0-9]+)*)', fwv, perl = TRUE))
          if (length(mm) > 0) fwv_num <- mm
        }
        
        # Extract Python version
        pyv <- if (!is.na(py_ix)) cells[py_ix] else NA_character_
        pyv_num <- if (!is.na(pyv)) trimws(gsub('(?i)python', '', pyv, perl = TRUE)) else NA_character_
        
        # Extract CUDA version if present
        if (!is.na(cu_ix)) {
          cuv <- cells[cu_ix]
          # Handle different cleaning patterns (TensorFlow has Unicode symbols)
          if (framework == 'tensorflow') {
            cuv_num <- trimws(gsub('^.*?([0-9]+(\\.[0-9]+)?)$','\\1', gsub('(?i)cuda\\P{N}{0,5}', '', cuv, perl = TRUE)))
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
            rov_num <- trimws(gsub('^.*?([0-9]+(\\.[0-9]+)+)$','\\1', gsub('(?i)rocm\\P{N}{0,5}', '', rov, perl = TRUE)))
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
