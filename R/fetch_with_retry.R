library(httr2)
library(digest)
library(jsonlite)
library(glue)

#' Fetch a URL with polite retry/backoff and cache raw + meta
#' @param url URL to fetch
#' @param cache_dir directory to store cache files
#' @param refresh if FALSE and identical sha exists, return cached meta
#' @param max_retries max retries for 429/503
#' @param rate_limit seconds to wait between requests (per-call)
#' @return list(path, sha256, fetched_at, url, status_code)
fetch_with_retry <- function(
  url,
  cache_dir = 'cache',
  refresh = FALSE,
  max_retries = 5,
  rate_limit = 1
) {
  dir.create(cache_dir, showWarnings = FALSE, recursive = TRUE)
  url_key <- tolower(trimws(url))
  sha <- digest(url_key, algo = 'sha256', serialize = FALSE)
  raw_path <- file.path(cache_dir, paste0(sha, '.raw'))
  meta_path <- file.path(cache_dir, paste0(sha, '.meta.json'))

  if (!refresh && file.exists(meta_path) && file.exists(raw_path)) {
    meta <- fromJSON(meta_path)
    return(list(
      path = raw_path,
      sha256 = meta$sha256,
      fetched_at = meta$fetched_at,
      url = meta$url,
      status_code = meta$status_code
    ))
  }

  attempt <- 1
  wait <- 1
  while (attempt <= max_retries) {
    if (attempt > 1) Sys.sleep(rate_limit)
    resp <- try(
      {
        req <- request(url) |>
          req_user_agent('gpu-compat-scraper/0.1') |>
          req_timeout(seconds = 30)
        req_perform(req)
      },
      silent = TRUE
    )
    fetched_at <- format(Sys.time(), tz = 'UTC', usetz = TRUE)

    if (inherits(resp, 'try-error')) {
      err_path <- file.path(cache_dir, paste0('error-', sha, '.raw'))
      writeChar(as.character(resp), err_path, eos = NULL)
      stop(glue('Network error fetching {url}: {resp}'))
    }

    status <- resp$status
    content_type <- resp_header(resp, 'content-type') %||% ''
    body <- resp_body_raw(resp)
    content_length <- length(body)

    writeBin(body, raw_path)

    meta <- list(
      sha256 = sha,
      fetched_at = format(Sys.time(), tz = 'UTC', usetz = TRUE),
      url = url,
      status_code = status,
      content_type = content_type,
      content_length = content_length
    )
    write(toJSON(meta, auto_unbox = TRUE, pretty = TRUE), meta_path)

    if (status %in% c(429, 503)) {
      if (attempt == max_retries) {
        err_raw <- file.path(cache_dir, paste0('error-', sha, '.raw'))
        writeBin(body, err_raw)
        stop(glue('Received {status} from {url} after {attempt} attempts'))
      }
      Sys.sleep(wait)
      wait <- wait * 2
      attempt <- attempt + 1
      next
    }

    return(list(
      path = raw_path,
      sha256 = sha,
      fetched_at = meta$fetched_at,
      url = url,
      status_code = status
    ))
  }
  stop(glue("Failed to fetch {url}: all retry attempts exhausted or unexpected error."))
}

# fallback operator
`%||%` <- function(a, b) if (!is.null(a)) a else b
