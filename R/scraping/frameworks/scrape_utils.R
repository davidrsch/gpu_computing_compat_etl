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
