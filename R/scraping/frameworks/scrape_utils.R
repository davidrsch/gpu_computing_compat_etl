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
    m <- regexec('([0-9]+(\\.[0-9]+)+)', fwv[1])
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
