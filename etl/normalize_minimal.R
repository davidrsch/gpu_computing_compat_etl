library(digest)
library(dplyr)
library(tibble)
library(DBI)
library(RSQLite)
library(jsonlite)

canonical_vendor <- function(name) {
  nm <- tolower(trimws(name))
  if (grepl('nvidia', nm)) {
    return('NVIDIA')
  }
  if (grepl('amd', nm) || grepl('advanced micro devices', nm)) {
    return('AMD')
  }
  if (grepl('intel', nm)) {
    return('Intel')
  }
  toupper(name)
}

normalize_model_name <- function(model, vendor_name = NULL) {
  # Conservative model normalization
  if (is.na(model) || nchar(trimws(as.character(model))) == 0) {
    return(NA_character_)
  }
  m <- as.character(model)
  # strip HTML tags
  m <- gsub('<[^>]+>', ' ', m)
  # remove parenthetical content
  m <- gsub('\\([^)]*\\)', ' ', m)
  # lower, replace punctuation with space
  m <- tolower(m)
  m <- gsub('[^a-z0-9\\+]+', ' ', m)
  # collapse whitespace
  m <- gsub('\\s+', ' ', trimws(m))

  # remove obvious vendor tokens
  vendor_tokens <- c('nvidia', 'amd', 'intel', 'advanced', 'micro', 'devices', 'geforce')
  # family tokens that are noisy when repeated but often useful (we keep them)
  noisy_tokens <- c('graphics', 'gpu', 'series', 'edition', 'mobile', 'cards')
  toks <- unlist(strsplit(m, ' '))
  toks <- toks[!(toks %in% vendor_tokens)]
  toks <- toks[!(toks %in% noisy_tokens)]
  # If the first and last tokens repeat a family label (e.g., 'geforce ... geforce'),
  # drop the trailing duplicate to improve joins across sources.
  if (length(toks) >= 2) {
    last_tok <- tolower(toks[length(toks)])
    # Common GPU family labels that sometimes repeat at the end of scraped names
    family_labels <- c('geforce', 'quadro', 'tesla', 'nvs', 'rtx', 'gtx')
    if (last_tok %in% family_labels) {
      earlier <- tolower(toks[-length(toks)])
      if (any(earlier == last_tok)) {
        toks <- toks[-length(toks)]
      }
    }
  }
  # collapse repeated tokens
  if (length(toks) == 0) {
    return(NA_character_)
  }
  dedup <- toks[c(TRUE, toks[-1] != toks[-length(toks)])]
  out <- paste(dedup, collapse = ' ')
  # trim trailing vendor-like tokens (e.g., trailing 'series' or vendor name remnants)
  out <- gsub('\\b(nvidia|amd|intel|series|graphics)\\b$', '', out, perl = TRUE)
  out <- gsub('\\s+', ' ', trimws(out))
  if (nchar(out) == 0) {
    return(NA_character_)
  }
  out
}

sha256_of <- function(key) {
  digest(key, algo = 'sha256', serialize = FALSE)
}

# Produce a sortable version key from a version string (e.g., 12.3 -> 00012.00003.00000)
mk_version_key <- function(v) {
  if (is.null(v) || is.na(v) || nchar(as.character(v)) == 0) {
    return(NA_character_)
  }
  s <- as.character(v)
  # Extract numeric parts; keep up to 3 components
  parts <- unlist(strsplit(s, '\\.'))
  parts <- parts[parts != '']
  parts <- parts[seq_len(min(length(parts), 3))]
  # Left-pad numbers to fixed width for lexicographic sort; handle non-numeric gracefully
  fmt <- function(x) {
    xi <- suppressWarnings(as.integer(x))
    if (is.na(xi)) {
      # Try to strip non-digits
      xd <- gsub('[^0-9]', '', x)
      xi <- suppressWarnings(as.integer(xd))
    }
    if (is.na(xi)) {
      return(sprintf('%05d', 0L))
    }
    sprintf('%05d', xi)
  }
  p <- vapply(parts, fmt, character(1))
  if (length(p) < 3) {
    p <- c(p, rep(sprintf('%05d', 0L), 3 - length(p)))
  }
  paste(p, collapse = '.')
}

# For NVIDIA: derive supported CUDA Toolkit version bounds from compute capability
# Based on NVIDIA's "CUDA Toolkit, Driver, and Architecture Matrix":
# https://docs.nvidia.com/datacenter/tesla/drivers/cuda-toolkit-driver-and-architecture-matrix.html
# Returns a list(min_version, max_version) where versions are strings like "11.8" or "12.x".
# If a strict upper-bound minor exists (e.g., Kepler 3.0/3.2 -> 10.2), max_version is that exact version.
# Otherwise, max_version is a major band like "12.x".
.nvidia_cuda_bounds <- function(cc_num) {
  if (is.na(cc_num)) return(list(min_version = NA_character_, max_version = NA_character_))
  # Map CC to architecture families
  if (cc_num >= 10.0) {
    # Blackwell: introduced CUDA 12.8, supports 12.x
    return(list(min_version = '12.8', max_version = '12.x'))
  } else if (cc_num >= 9.0) {
    # Hopper: CUDA 11.8 and 12.x
    return(list(min_version = '11.8', max_version = '12.x'))
  } else if (cc_num >= 8.9) {
    # Ada: CUDA 11.8 and 12.x
    return(list(min_version = '11.8', max_version = '12.x'))
  } else if (cc_num >= 8.0) {
    # Ampere: CUDA 11.0 and 12.x
    return(list(min_version = '11.0', max_version = '12.x'))
  } else if (cc_num >= 7.5) {
    # Turing: CUDA 10.0 and 12.x
    return(list(min_version = '10.0', max_version = '12.x'))
  } else if (cc_num >= 7.0) {
    # Volta: CUDA 9.0 and 12.x
    return(list(min_version = '9.0', max_version = '12.x'))
  } else if (cc_num >= 6.0) {
    # Pascal: CUDA 8.0 and 12.x
    return(list(min_version = '8.0', max_version = '12.x'))
  } else if (cc_num >= 5.0) {
    # Maxwell: CUDA 6.5 and 12.x
    return(list(min_version = '6.5', max_version = '12.x'))
  } else if (cc_num >= 3.5) {
    # Kepler (GK110/210): CUDA 6.0 through 11.x
    return(list(min_version = '6.0', max_version = '11.x'))
  } else if (cc_num >= 3.0) {
    # Kepler (GK10x): CUDA 6.0 through 10.2
    return(list(min_version = '6.0', max_version = '10.2'))
  } else if (cc_num >= 2.0) {
    # Fermi: up to CUDA 8.0
    return(list(min_version = '3.0', max_version = '8.0'))
  }
  list(min_version = NA_character_, max_version = NA_character_)
}

normalize_and_export <- function(raw_list, out_dir = 'data/processed') {
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

  # raw_list expected to contain lists from scrapers keyed by vendor
  # Collect vendors
  vendors <- tibble()
  for (src in names(raw_list)) {
    if (!is.null(raw_list[[src]]$gpu_raw)) {
      vendor_name <- unique(raw_list[[src]]$gpu_raw$vendor)[1]
      v_can <- canonical_vendor(vendor_name)
      vendor_key <- paste0('vendor:', tolower(v_can))
      vendor_id <- sha256_of(vendor_key)
      vendors <- bind_rows(
        vendors,
        tibble(
          vendor_id = vendor_id,
          name = v_can,
          source_url = unique(raw_list[[src]]$gpu_raw$source_url)[1],
          fetched_at = if ("fetched_at" %in% names(raw_list[[src]]$gpu_raw)) unique(raw_list[[src]]$gpu_raw$fetched_at)[1] else NA,
          sha256 = unique(raw_list[[src]]$gpu_raw$sha256)[1]
        )
      )
    }
  }
  vendors <- distinct(vendors, vendor_id, .keep_all = TRUE)

  # GPUs
  gpus <- tibble()
  for (src in names(raw_list)) {
    gpu_raw <- raw_list[[src]]$gpu_raw
    if (is.null(gpu_raw)) {
      next
    }
    vendor_name <- unique(gpu_raw$vendor)[1]
    v_can <- canonical_vendor(vendor_name)
    vendor_key <- paste0('vendor:', tolower(v_can))
    vendor_id <- sha256_of(vendor_key)
    for (i in seq_len(nrow(gpu_raw))) {
      model <- as.character(gpu_raw$value[i])
      if (is.na(model) || nchar(trimws(model)) == 0) {
        next
      }
      model_norm <- normalize_model_name(model, vendor_name)
      key <- paste('gpu', vendor_id, model_norm, sep = '|')
      gpu_id <- sha256_of(key)
      gpus <- bind_rows(
        gpus,
        tibble(
          gpu_id = gpu_id,
          vendor_id = vendor_id,
          model_name = trimws(model),
          model_normalized = model_norm,
          codename = NA_character_,
          architecture = NA_character_,
          compute_capability = NA_character_,
          device_id = NA_character_,
          source_url = gpu_raw$source_url[i],
          fetched_at = if ("fetched_at" %in% names(gpu_raw)) gpu_raw$fetched_at[i] else Sys.time(),
          sha256 = gpu_raw$sha256[i]
        )
      )
    }
  }
  gpus <- distinct(gpus, gpu_id, .keep_all = TRUE)

  # Merge optional NVIDIA compute capability mappings if provided by scrapers (including authoritative scraper)
  gpu_cc_map <- tibble(
    model_name = character(),
    compute_capability = character(),
    source_url = character(),
    sha256 = character()
  )
  for (src in names(raw_list)) {
    try(
      {
        cc_raw <- raw_list[[src]]$gpu_cc_raw
        if (!is.null(cc_raw) && nrow(cc_raw) > 0) {
          # keep only plausible CC tokens like '5.2', '6.1'
          cc_raw$compute_capability <- as.character(cc_raw$compute_capability)
          cc_raw <- cc_raw |>
            filter(grepl('^[0-9]+\\.[0-9]+$', compute_capability))
          # ensure required columns
          if (is.null(cc_raw$source_url)) {
            cc_raw$source_url <- NA_character_
          }
          if (is.null(cc_raw$sha256)) {
            cc_raw$sha256 <- NA_character_
          }
          gpu_cc_map <- bind_rows(
            gpu_cc_map,
            cc_raw |> select(model_name, compute_capability, source_url, sha256)
          )
        }
      },
      silent = TRUE
    )
  }
  # Apply GPU alias expansions to CC map to improve joins
  alias_path <- tryCatch(
    normalizePath(
      file.path('data', 'overrides', 'gpu_aliases.csv'),
      mustWork = FALSE
    ),
    error = function(e) file.path('data', 'overrides', 'gpu_aliases.csv')
  )
  if (file.exists(alias_path)) {
    try(
      {
        als <- read.csv(alias_path, stringsAsFactors = FALSE)
        if (
          nrow(als) > 0 &&
            all(c('alias', 'canonical') %in% names(als)) &&
            nrow(gpu_cc_map) > 0
        ) {
          # duplicate CC rows for alias names when canonical matches
          cc_alias <- gpu_cc_map |>
            inner_join(als, by = c('model_name' = 'canonical')) |>
            mutate(model_name = alias) |>
            select(model_name, compute_capability, source_url, sha256)
          gpu_cc_map <- bind_rows(gpu_cc_map, cc_alias)
        }
      },
      silent = TRUE
    )
  }
  # Apply curated overrides if available
  override_path <- tryCatch(
    normalizePath(
      file.path('data', 'overrides', 'nvidia_cc_overrides.csv'),
      mustWork = FALSE
    ),
    error = function(e) {
      file.path('data', 'overrides', 'nvidia_cc_overrides.csv')
    }
  )
  if (file.exists(override_path)) {
    try(
      {
        cc_over <- read.csv(override_path, stringsAsFactors = FALSE)
        if (
          nrow(cc_over) > 0 &&
            all(c('model_name', 'compute_capability') %in% names(cc_over))
        ) {
          cc_over$source_url <- if ('note' %in% names(cc_over)) {
            paste0('override:', cc_over$note)
          } else {
            'override'
          }
          cc_over$sha256 <- 'override'
          gpu_cc_map <- bind_rows(
            gpu_cc_map,
            cc_over |>
              select(model_name, compute_capability, source_url, sha256)
          )
        }
      },
      silent = TRUE
    )
  }

  if (nrow(gpu_cc_map) > 0) {
    gpu_cc_map <- gpu_cc_map |> distinct(model_name, .keep_all = TRUE)
    # 1) Direct join on raw model_name for exact matches
    gpus <- gpus |>
      left_join(
        gpu_cc_map |> select(model_name, compute_capability),
        by = c('model_name' = 'model_name'),
        suffix = c('', '.new')
      ) |>
      mutate(
        compute_capability = coalesce(
          compute_capability.new,
          compute_capability
        )
      ) |>
      select(-compute_capability.new)
    # 2) Fallback join on normalized model names to tolerate minor naming differences (e.g., trailing vendor tokens)
    try(
      {
        gpu_cc_map_norm <- gpu_cc_map |>
          mutate(
            model_normalized = normalize_model_name(model_name, 'NVIDIA')
          ) |>
          distinct(model_normalized, .keep_all = TRUE)
        gpus <- gpus |>
          left_join(
            gpu_cc_map_norm |> select(model_normalized, compute_capability),
            by = c('model_normalized' = 'model_normalized'),
            suffix = c('', '.new')
          ) |>
          mutate(
            compute_capability = coalesce(
              compute_capability.new,
              compute_capability
            )
          ) |>
          select(-compute_capability.new)
      },
      silent = TRUE
    )

    # 3) NVIDIA-specific pragmatic fallback: match on family+number token (e.g., 'rtx 5090', 't4', 'l4')
    try(
      {
        nv_key <- function(n) {
          if (is.null(n) || is.na(n)) return(NA_character_)
          s <- tolower(as.character(n))
          s <- gsub('[^a-z0-9 ]+', ' ', s)
          s <- gsub('\\s+', ' ', trimws(s))
          # First, special-case well-known names without trailing numbers (e.g., 'titan rtx')
          if (grepl('titan\\s*rtx', s, perl = TRUE)) {
            return('titan rtx')
          }
          # Then, general family+number capture including single-digit models (e.g., 't4', 'l4')
          m <- regmatches(s, regexpr('(rtx|gtx|a|h|l|t)\\s*[0-9]{1,4}', s, perl = TRUE))
          k <- if (length(m) > 0 && nchar(m[1]) > 0) {
            # normalize any internal whitespace
            gsub('[\\x{00A0}]', ' ', gsub('\\s+', ' ', tolower(m[1])), perl = TRUE)
          } else {
            NA_character_
          }
          k
        }
        gpu_cc_map_nv <- gpu_cc_map |>
          mutate(nv_key = vapply(model_name, nv_key, character(1))) |>
          filter(!is.na(nv_key)) |>
          distinct(nv_key, .keep_all = TRUE)
        gpus <- gpus |>
          mutate(nv_key = vapply(model_name, nv_key, character(1))) |>
          left_join(
            gpu_cc_map_nv |> select(nv_key, compute_capability),
            by = 'nv_key',
            suffix = c('', '.new')
          ) |>
          mutate(
            compute_capability = coalesce(
              compute_capability.new,
              compute_capability
            )
          ) |>
          select(-compute_capability.new, -nv_key)
      },
      silent = TRUE
    )

    # 4) Final conservative fallback for NVIDIA: substring-based matching
    #    This helps when scraped model names are concatenated or lack vendor prefixes
    #    (e.g., 'L4' vs 'NVIDIA L4', 'TITAN RTX' vs 'NVIDIA TITAN RTX',
    #     'Jetson AGX Orin Jetson Orin NX Jetson Orin Nano').
    try(
      {
        if (nrow(gpu_cc_map) > 0) {
          cc_tbl <- gpu_cc_map |>
            mutate(model_lower = tolower(trimws(as.character(model_name)))) |>
            distinct(model_lower, .keep_all = TRUE)
          if (!'model_lower' %in% names(cc_tbl)) cc_tbl$model_lower <- tolower(cc_tbl$model_name)

          # Work only on NVIDIA rows still missing CC
          idx_na <- which(is.na(gpus$compute_capability) & gpus$vendor_id == sha256_of('vendor:nvidia'))
          if (length(idx_na) > 0) {
            g_model_lower <- tolower(trimws(as.character(gpus$model_name)))
            pick_cc <- function(s) {
              if (is.na(s) || nchar(s) == 0) return(NA_character_)
              # Find cc rows where cc name is contained in GPU name OR vice versa
              hit1 <- which(vapply(cc_tbl$model_lower, function(x) grepl(x, s, fixed = TRUE), logical(1)))
              hit2 <- which(vapply(cc_tbl$model_lower, function(x) grepl(s, x, fixed = TRUE), logical(1)))
              hits <- unique(c(hit1, hit2))
              if (length(hits) == 0) return(NA_character_)
              # Prefer the longest model_lower among hits to avoid overly generic matches
              best <- hits[which.max(nchar(cc_tbl$model_lower[hits]))][1]
              cc <- cc_tbl$compute_capability[best]
              if (is.na(cc) || nchar(as.character(cc)) == 0) return(NA_character_)
              as.character(cc)
            }
            # Apply and merge back
            cc_from_sub <- vapply(g_model_lower[idx_na], pick_cc, character(1))
            if (length(cc_from_sub) > 0) {
              # only fill where we actually found a value
              fill_idx <- idx_na[which(!is.na(cc_from_sub) & nchar(cc_from_sub) > 0)]
              if (length(fill_idx) > 0) {
                gpus$compute_capability[fill_idx] <- coalesce(cc_from_sub[which(!is.na(cc_from_sub) & nchar(cc_from_sub) > 0)], gpus$compute_capability[fill_idx])
              }
            }
          }
        }
      },
      silent = TRUE
    )
  }

  # Runtimes
  runtimes <- tibble()
  for (src in names(raw_list)) {
    rt_raw <- raw_list[[src]]$runtime_raw
    if (is.null(rt_raw)) {
      next
    }
    vendor_name <- unique(rt_raw$vendor)[1]
    v_can <- canonical_vendor(vendor_name)
    vendor_key <- paste0('vendor:', tolower(v_can))
    vendor_id <- sha256_of(vendor_key)
    for (i in seq_len(nrow(rt_raw))) {
      val <- rt_raw$value[i]
      if (is.na(val) || nchar(trimws(as.character(val))) == 0) {
        # default runtime per vendor
        if (v_can == 'NVIDIA') {
          val <- 'CUDA'
        }
        if (v_can == 'AMD') {
          val <- 'ROCm'
        }
        if (v_can == 'Intel') val <- 'oneAPI'
      }
      val <- as.character(val)
      # canonicalize runtime family and extract version when present
      fam <- NA_character_
      if (grepl('cuda', val, ignore.case = TRUE)) {
        fam <- 'CUDA'
      } else if (grepl('rocm', val, ignore.case = TRUE)) {
        fam <- 'ROCm'
      } else if (
        grepl('oneapi', val, ignore.case = TRUE) ||
          grepl('one api', val, ignore.case = TRUE)
      ) {
        fam <- 'oneAPI'
      } else {
        # fallback: first word
        wf <- tolower(trimws(sub(' .*', '', val)))
        fam <- toupper(wf)
      }
      # find first numeric version like 12 or 12.2 or 2023.2
      ver_match <- regmatches(
        val,
        regexec('([0-9]{2,4}(\\.[0-9]+)+|[0-9]+(\\.[0-9]+)*)', val)
      )[[1]]
      version <- if (length(ver_match) >= 2) ver_match[1] else NA_character_
      name <- fam
      key <- paste(
        'runtime',
        name,
        ifelse(is.na(version), '', version),
        sep = '|'
      )
      runtime_id <- sha256_of(key)
      version_key <- mk_version_key(version)
      runtimes <- bind_rows(
        runtimes,
        tibble(
          runtime_id = runtime_id,
          vendor_id = vendor_id,
          name = name,
          version = version,
          version_key = version_key,
          source_url = rt_raw$source_url[i],
          fetched_at = if ("fetched_at" %in% names(rt_raw)) rt_raw$fetched_at[i] else Sys.time(),
          sha256 = rt_raw$sha256[i]
        )
      )
    }
  }
  runtimes <- distinct(runtimes, runtime_id, .keep_all = TRUE)

  # Collect runtime requirements from scrapers (e.g., AMD ROCm support, Intel ONEAPI)
  runtime_requirements <- tibble(
    vendor = character(),
    model_name = character(),
    runtime_name = character(),
    min_version = character(),
    max_version = character(),
    notes = character(),
    source_url = character(),
    sha256 = character()
  )
  for (src in names(raw_list)) {
    rr <- NULL
    try(
      {
        rr <- raw_list[[src]]$runtime_requirements_raw
      },
      silent = TRUE
    )
    if (is.null(rr) || nrow(rr) == 0) {
      next
    }
    # normalize casing for runtime_name
    rr$runtime_name <- toupper(trimws(rr$runtime_name))
    runtime_requirements <- bind_rows(runtime_requirements, rr)
  }
  # Apply alias expansions for runtime requirements
  if (file.exists(alias_path) && nrow(runtime_requirements) > 0) {
    try(
      {
        als <- read.csv(alias_path, stringsAsFactors = FALSE)
        if (nrow(als) > 0 && all(c('alias', 'canonical') %in% names(als))) {
          rr_alias <- runtime_requirements |>
            inner_join(als, by = c('model_name' = 'canonical')) |>
            mutate(model_name = alias) |>
            select(names(runtime_requirements))
          runtime_requirements <- bind_rows(runtime_requirements, rr_alias)
        }
      },
      silent = TRUE
    )
  }
  runtime_requirements <- runtime_requirements |>
    distinct(vendor, model_name, runtime_name, .keep_all = TRUE)

  # Languages: collect from any scraper that emitted language_raw (frameworks and runtimes)
  languages <- tibble()
  for (src in names(raw_list)) {
    lr <- NULL
    try(
      {
        lr <- raw_list[[src]]$language_raw
      },
      silent = TRUE
    )
    if (is.null(lr)) {
      next
    }
    if (!is.null(lr) && nrow(lr) > 0) {
      for (i in seq_len(nrow(lr))) {
        lang_raw <- tolower(trimws(lr$language[i]))
        if (is.na(lang_raw) || nchar(lang_raw) == 0) {
          next
        }
        # strip version suffix if present: 'python:python 3.10' -> 'python'
        base <- tolower(trimws(sub(':.*$', '', lang_raw)))
        # normalize repeated '+' sequences to '++' (fix noisy 'c++++++' cases)
        base <- gsub('\\++', '++', base, perl = TRUE)
        key <- paste0('language:', base)
        language_id <- sha256_of(key)
        languages <- bind_rows(
          languages,
          tibble(
            language_id = language_id,
            name = base,
            source_url = lr$source_url[i],
            fetched_at = lr$fetched_at[i],
            sha256 = lr$sha256[i]
          )
        )
      }
    }
  }
  languages <- distinct(languages, language_id, .keep_all = TRUE)

  # Frameworks
  frameworks <- tibble()
  if (!is.null(raw_list$frameworks)) {
    fr <- raw_list$frameworks$framework_raw
    for (i in seq_len(nrow(fr))) {
      name <- tolower(trimws(fr$framework[i]))
      key <- paste0('framework:', name)
      framework_id <- sha256_of(key)
      frameworks <- bind_rows(
        frameworks,
        tibble(
          framework_id = framework_id,
          name = name,
          source_url = fr$source_url[i],
          fetched_at = if ("fetched_at" %in% names(fr)) fr$fetched_at[i] else Sys.time(),
          sha256 = fr$sha256[i]
        )
      )
    }
    # Also ingest framework matrices if provided (runtime versions per framework)
    fm <- NULL
    try(
      {
        fm <- raw_list$frameworks$framework_matrix_raw
      },
      silent = TRUE
    )
    framework_matrices <- tibble()
    if (!is.null(fm) && nrow(fm) > 0) {
      for (i in seq_len(nrow(fm))) {
        fname <- tolower(trimws(fm$framework[i]))
        rname <- toupper(trimws(fm$runtime_name[i]))
        rver <- as.character(fm$runtime_version[i])
        fver <- if ('framework_version' %in% names(fm)) {
          as.character(fm$framework_version[i])
        } else {
          NA_character_
        }
        pyver <- if ('python_version' %in% names(fm)) {
          as.character(fm$python_version[i])
        } else {
          NA_character_
        }
        framework_matrices <- bind_rows(
          framework_matrices,
          tibble(
            framework = fname,
            framework_version = fver,
            runtime_name = rname,
            runtime_version = rver,
            python_version = pyver,
            source_url = fm$source_url[i],
            sha256 = fm$sha256[i]
          )
        )
      }
      # Ensure expected columns exist
      if (!'framework_version' %in% names(framework_matrices)) {
        framework_matrices$framework_version <- NA_character_
      }
      if (!'python_version' %in% names(framework_matrices)) {
        framework_matrices$python_version <- NA_character_
      }
      framework_matrices <- distinct(
        framework_matrices,
        framework,
        runtime_name,
        runtime_version,
        python_version,
        framework_version,
        .keep_all = TRUE
      )
    }
  }
  frameworks <- distinct(frameworks, framework_id, .keep_all = TRUE)

  # gpu_runtime_compat: map each gpu to each runtime discovered for its vendor
  gpu_runtime <- tibble()
  for (i in seq_len(nrow(gpus))) {
    g <- gpus[i, ]
    # find runtimes for same vendor
    rts <- runtimes |> filter(vendor_id == g$vendor_id)
    if (nrow(rts) == 0) {
      # fallback: any runtime with vendor matching name
      rts <- runtimes
    }
    if (nrow(rts) == 0) {
      # create a default mapping to vendor runtime name
      # make runtime entry
      next
    }
    for (j in seq_len(nrow(rts))) {
      rt <- rts[j, ]
      # Apply NVIDIA CUDA gating by compute capability to avoid overstating support
      allow <- TRUE
      score <- 5L
      notes <- NA_character_
      # NVIDIA CUDA gating only applies to NVIDIA vendor and CUDA runtime
      if (toupper(rt$name) == 'CUDA' && g$vendor_id == sha256_of('vendor:nvidia')) {
        # parse runtime version and compute capability
        rt_ver <- coalesce(rt$version, NA_character_)
        cc_num <- suppressWarnings(as.numeric(coalesce(g$compute_capability, NA_character_)))
        # If runtime has no version, keep mapping but reduce score
        if (is.na(rt_ver) || nchar(rt_ver) == 0) {
          score <- min(score, 3L)
        } else {
          # Determine allowed CUDA version band for this CC
          bounds <- .nvidia_cuda_bounds(cc_num)
          min_v <- bounds$min_version
          max_v <- bounds$max_version
          # If CC unknown, be conservative: allow up to CUDA 11.x with lower score; exclude 12.x
          if (is.na(cc_num)) {
            vmaj <- suppressWarnings(as.integer(sub('\\..*$', '', rt_ver)))
            if (!is.na(vmaj) && vmaj >= 12) {
              allow <- FALSE
              notes <- 'Excluded: Unknown compute capability for CUDA 12.x'
            } else {
              score <- min(score, 3L)
              notes <- 'Compute capability unknown; allowing <= CUDA 11.x with lower score'
            }
          } else {
            # Enforce lower bound
            if (!is.na(min_v) && nchar(min_v) > 0) {
              if (mk_version_key(rt_ver) < mk_version_key(min_v)) {
                allow <- FALSE
                notes <- paste0('Excluded: requires CUDA >= ', min_v, ' for CC ', cc_num)
              }
            }
            # Enforce upper bound (major-only like '11.x' or exact like '10.2')
            if (allow && !is.na(max_v) && nchar(max_v) > 0) {
              if (grepl('^([0-9]+)\\.x$', max_v)) {
                max_major <- as.integer(sub('\\.x$', '', max_v))
                vmaj <- suppressWarnings(as.integer(sub('\\..*$', '', rt_ver)))
                if (!is.na(vmaj) && vmaj > max_major) {
                  allow <- FALSE
                  notes <- paste0('Excluded: CUDA > ', max_v, ' not supported for CC ', cc_num)
                }
              } else {
                # exact version upper bound, e.g., '10.2'
                if (mk_version_key(rt_ver) > mk_version_key(max_v)) {
                  allow <- FALSE
                  notes <- paste0('Excluded: CUDA > ', max_v, ' not supported for CC ', cc_num)
                }
              }
            }
          }
        }
      }
      # Apply AMD ROCm gating if we have requirements
      if (
        toupper(rt$name) == 'ROCM' && g$vendor_id == sha256_of('vendor:amd')
      ) {
        if (nrow(runtime_requirements) > 0) {
          # Try to find a requirement that matches the model name
          req_rows <- runtime_requirements |>
            filter(
              toupper(runtime_name) == 'ROCM' &
                (grepl(g$model_name, model_name, fixed = TRUE) |
                  grepl(model_name, g$model_name, fixed = TRUE))
            )
          if (nrow(req_rows) == 0) {
            # try relaxed match using normalized model name
            req_rows <- runtime_requirements |>
              filter(
                toupper(runtime_name) == 'ROCM' &
                  (grepl(
                    g$model_normalized,
                    tolower(model_name),
                    fixed = TRUE
                  ) |
                    grepl(
                      tolower(model_name),
                      g$model_normalized,
                      fixed = TRUE
                    ))
              )
          }
          if (nrow(req_rows) > 0) {
            # If min_version exists, ensure rt.version >= min_version
            req <- req_rows[1, ]
            if (!is.na(req$min_version) && !is.na(rt$version)) {
              if (
                mk_version_key(rt$version) < mk_version_key(req$min_version)
              ) {
                allow <- FALSE
                notes <- paste0(
                  'Excluded: ROCm < ',
                  req$min_version,
                  ' for ',
                  g$model_name
                )
              }
            }
          } else {
            # If we have any AMD ROCm requirements collected but no match for this model, be conservative and lower score
            score <- 3L
          }
        }
      }

      # Apply Intel ONEAPI gating: require presence in Intel feature matrix-derived requirements
      if (
        toupper(rt$name) == 'ONEAPI' && g$vendor_id == sha256_of('vendor:intel')
      ) {
        if (nrow(runtime_requirements) > 0) {
          req_rows <- runtime_requirements |>
            filter(
              toupper(runtime_name) == 'ONEAPI' &
                (grepl(g$model_name, model_name, fixed = TRUE) |
                  grepl(model_name, g$model_name, fixed = TRUE))
            )
          if (nrow(req_rows) == 0) {
            # not listed -> exclude to avoid overstating support
            allow <- FALSE
            notes <- paste0(
              'Excluded: not listed in Intel dGPU feature matrix for ONEAPI — ',
              g$model_name
            )
          } else {
            # if a min_version exists, enforce it
            req <- req_rows[1, ]
            if (!is.na(req$min_version) && !is.na(rt$version)) {
              if (
                mk_version_key(rt$version) < mk_version_key(req$min_version)
              ) {
                allow <- FALSE
                notes <- paste0(
                  'Excluded: oneAPI < ',
                  req$min_version,
                  ' for ',
                  g$model_name
                )
              }
            }
          }
        }
      }

      if (!allow) {
        next
      }
      key <- paste('gpu_runtime', g$gpu_id, rt$runtime_id, sep = '|')
      gpu_runtime_id <- sha256_of(key)
      min_runtime_version <- NA_character_
      max_runtime_version <- NA_character_
      evidence_url <- rt$source_url
      gpu_runtime <- bind_rows(
        gpu_runtime,
        tibble(
          gpu_runtime_id = gpu_runtime_id,
          gpu_id = g$gpu_id,
          runtime_id = rt$runtime_id,
          min_runtime_version = min_runtime_version,
          max_runtime_version = max_runtime_version,
          evidence_url = evidence_url,
          score = score,
          notes = notes
        )
      )
    }
  }
  gpu_runtime <- distinct(gpu_runtime, gpu_runtime_id, .keep_all = TRUE)

  # runtime_language: link runtimes to languages when there is evidence
  runtime_language <- tibble()
  # Use evidence from framework pages: if a framework page mentions both a runtime and a language,
  # create runtime->language mapping with the framework page as evidence.
  if (!is.null(raw_list$frameworks)) {
    fr_raw <- raw_list$frameworks$framework_raw
    if (!is.null(fr_raw) && nrow(fr_raw) > 0) {
      for (i in seq_len(nrow(fr_raw))) {
        fr <- fr_raw[i, ]
        if (!is.na(fr$runtimes) && !is.na(fr$languages)) {
          rnames <- trimws(unlist(strsplit(fr$runtimes, ';')))
          lnames <- tolower(trimws(unlist(strsplit(fr$languages, ';'))))
          # match runtime names to runtime ids
          for (rn in rnames) {
            # find runtime rows with matching name (case-insensitive)
            matched_rts <- runtimes |> filter(toupper(name) == toupper(rn))
            if (nrow(matched_rts) == 0) {
              # try partial match
              matched_rts <- runtimes |>
                filter(grepl(rn, name, ignore.case = TRUE))
            }
            for (rr in seq_len(nrow(matched_rts))) {
              rt_row <- matched_rts[rr, ]
              for (ln in lnames) {
                # ln may be 'python' or 'python:Python 3.8+' or 'python:3.8+'
                parts <- unlist(strsplit(ln, ':'))
                lang_name <- tolower(trimws(parts[1]))
                lang_ver <- if (length(parts) > 1) {
                  trimws(parts[2])
                } else {
                  NA_character_
                }
                lang_row <- languages |> filter(name == lang_name)
                if (nrow(lang_row) == 0) {
                  next
                }
                key <- paste(
                  'runtime_lang',
                  rt_row$runtime_id,
                  lang_row$language_id,
                  sep = '|'
                )
                rl_id <- sha256_of(key)
                min_lang_ver <- if (!is.na(lang_ver)) {
                  lang_ver
                } else {
                  NA_character_
                }
                # derive a normalized numeric portion and key for sorting
                lv_numeric <- if (
                  !is.na(min_lang_ver) && nchar(min_lang_ver) > 0
                ) {
                  sub('[^0-9\\.].*$', '', min_lang_ver)
                } else {
                  NA_character_
                }
                lv_key <- mk_version_key(lv_numeric)
                runtime_language <- bind_rows(
                  runtime_language,
                  tibble(
                    runtime_language_id = rl_id,
                    runtime_id = rt_row$runtime_id,
                    language_id = lang_row$language_id,
                    min_language_version = min_lang_ver,
                    min_language_version_key = lv_key,
                    max_language_version = NA_character_,
                    evidence_url = fr$source_url,
                    score = 4
                  )
                )
              }
            }
          }
        }
      }
    }
  }
  if (nrow(runtime_language) == 0) {
    runtime_language <- tibble(
      runtime_language_id = character(),
      runtime_id = character(),
      language_id = character(),
      min_language_version = character(),
      min_language_version_key = character(),
      max_language_version = character(),
      evidence_url = character(),
      score = integer()
    )
  } else {
    runtime_language <- distinct(
      runtime_language,
      runtime_language_id,
      .keep_all = TRUE
    )
  }

  # Also create runtime->language mappings when runtime scrapers themselves
  # emitted language_raw evidence (e.g., CUDA/ROCm/oneAPI pages mentioning Python/C++).
  for (src in names(raw_list)) {
    lr <- NULL
    try(
      {
        lr <- raw_list[[src]]$language_raw
      },
      silent = TRUE
    )
    if (is.null(lr) || nrow(lr) == 0) {
      next
    }
    for (i in seq_len(nrow(lr))) {
      lang_raw <- tolower(trimws(lr$language[i]))
      parts <- unlist(strsplit(lang_raw, ':'))
      base <- tolower(trimws(parts[1]))
      base <- gsub('\\++', '++', base, perl = TRUE)
      # canonicalize common variants
      if (base %in% c('py', 'python3', 'python2', 'python')) {
        base <- 'python'
      }
      if (base %in% c('cpp', 'c++', 'cxx')) {
        base <- 'c++'
      }
      if (base %in% c('r', 'R')) {
        base <- 'r'
      }
      if (base %in% c('golang', 'go')) {
        base <- 'go'
      }
      if (base %in% c('fortran', 'f90', 'f95')) {
        base <- 'fortran'
      }
      lang_row <- languages |> filter(name == base)
      if (nrow(lang_row) == 0) {
        next
      }

      # Prefer runtimes fetched from the same page (matching sha256) or same source_url
      matched_rts <- runtimes |>
        filter(sha256 == lr$sha256[i] | source_url == lr$source_url[i])
      # If nothing matched by page, try vendor-level runtimes by comparing hostnames
      if (nrow(matched_rts) == 0) {
        # try coarse vendor match: if lr$source_url contains vendor name
        if (grepl('nvidia', lr$source_url[i], ignore.case = TRUE)) {
          matched_rts <- runtimes |>
            filter(vendor_id == sha256_of('vendor:nvidia'))
        }
        if (grepl('amd', lr$source_url[i], ignore.case = TRUE)) {
          matched_rts <- runtimes |>
            filter(vendor_id == sha256_of('vendor:amd'))
        }
        if (grepl('intel', lr$source_url[i], ignore.case = TRUE)) {
          matched_rts <- runtimes |>
            filter(vendor_id == sha256_of('vendor:intel'))
        }
      }

      if (nrow(matched_rts) == 0) {
        next
      }
      # try to extract version from language evidence like 'python: Python 3.10' or 'python:3.10+'
      lang_ver <- NA_character_
      if (length(parts) > 1) {
        lang_ver <- trimws(parts[2])
      }
      lang_ver_clean <- NA_character_
      if (!is.na(lang_ver) && nchar(lang_ver) > 0) {
        lang_ver_clean <- lang_ver
      }
      for (rr in seq_len(nrow(matched_rts))) {
        rt_row <- matched_rts[rr, ]
        key <- paste(
          'runtime_lang',
          rt_row$runtime_id,
          lang_row$language_id[1],
          sep = '|'
        )
        rl_id <- sha256_of(key)
        # derive language version key
        lv_numeric <- if (!is.na(lang_ver_clean) && nchar(lang_ver_clean) > 0) {
          sub('[^0-9\\.].*$', '', lang_ver_clean)
        } else {
          NA_character_
        }
        lv_key <- mk_version_key(lv_numeric)
        runtime_language <- bind_rows(
          runtime_language,
          tibble(
            runtime_language_id = rl_id,
            runtime_id = rt_row$runtime_id,
            language_id = lang_row$language_id[1],
            min_language_version = lang_ver_clean,
            min_language_version_key = lv_key,
            max_language_version = NA_character_,
            evidence_url = lr$source_url[i],
            score = 4
          )
        )
      }
    }
  }

  # framework_compat: map frameworks to runtime+language combos
  # If framework matrices exist (framework_matrices), restrict to listed runtime versions; else use coarse evidence
  framework_compat <- tibble()
  if (!is.null(raw_list$frameworks)) {
    fr_raw <- raw_list$frameworks$framework_raw
    if (!is.null(fr_raw) && nrow(fr_raw) > 0) {
      for (i in seq_len(nrow(fr_raw))) {
        fr <- fr_raw[i, ]
        fw_name <- tolower(trimws(fr$framework))
        fw_id <- sha256_of(paste0('framework:', fw_name))
        if (!is.na(fr$runtimes) && !is.na(fr$languages)) {
          rnames <- trimws(unlist(strsplit(fr$runtimes, ';')))
          lnames <- tolower(trimws(unlist(strsplit(fr$languages, ';'))))
          for (rn in rnames) {
            # If a matrix exists for this framework and runtime, filter runtimes by the exact versions listed
            if (exists('framework_matrices') && nrow(framework_matrices) > 0) {
              fm_rows <- framework_matrices |>
                filter(framework == fw_name & runtime_name == toupper(rn))
              if (nrow(fm_rows) > 0) {
                matched_rts <- runtimes |>
                  filter(
                    toupper(name) == toupper(rn) &
                      (!is.na(version) & version %in% fm_rows$runtime_version)
                  )
              } else {
                matched_rts <- runtimes |> filter(toupper(name) == toupper(rn))
              }
            } else {
              matched_rts <- runtimes |> filter(toupper(name) == toupper(rn))
            }
            if (nrow(matched_rts) == 0) {
              matched_rts <- runtimes |>
                filter(grepl(rn, name, ignore.case = TRUE))
            }
            for (rr in seq_len(nrow(matched_rts))) {
              rt_row <- matched_rts[rr, ]
              for (ln in lnames) {
                lang_row <- languages |> filter(name == ln)
                if (nrow(lang_row) == 0) {
                  next
                }
                key <- paste(
                  'framework_compat',
                  fw_id,
                  rt_row$runtime_id,
                  lang_row$language_id,
                  sep = '|'
                )
                fc_id <- sha256_of(key)
                # If a framework matrix row exists for this framework/runtime version, carry its framework_version and python_version
                mfv <- NA_character_
                mlv <- NA_character_
                if (
                  exists('framework_matrices') &&
                    nrow(framework_matrices) > 0 &&
                    !is.na(rt_row$version)
                ) {
                  fm_hit <- framework_matrices |>
                    filter(
                      framework == fw_name &
                        runtime_name == toupper(rn) &
                        runtime_version == rt_row$version
                    )
                  if (nrow(fm_hit) > 0) {
                    mfv <- fm_hit$framework_version[which(
                      nchar(fm_hit$framework_version) > 0
                    )][1]
                    # only apply python min language version when language is python
                    if (ln == 'python') {
                      mlv <- fm_hit$python_version[which(
                        nchar(fm_hit$python_version) > 0
                      )][1]
                    }
                  }
                }
                # derive version keys
                mfv_key <- mk_version_key(mfv)
                mlv_num <- if (!is.na(mlv) && nchar(as.character(mlv)) > 0) {
                  sub('[^0-9\\.].*$', '', as.character(mlv))
                } else {
                  NA_character_
                }
                mlv_key <- mk_version_key(mlv_num)
                framework_compat <- bind_rows(
                  framework_compat,
                  tibble(
                    framework_compat_id = fc_id,
                    framework_id = fw_id,
                    runtime_id = rt_row$runtime_id,
                    language_id = lang_row$language_id,
                    min_framework_version = mfv,
                    min_framework_version_key = mfv_key,
                    max_framework_version = NA_character_,
                    min_language_version = mlv,
                    min_language_version_key = mlv_key,
                    evidence_url = fr$source_url,
                    score = 4,
                    notes = NA_character_
                  )
                )
              }
            }
          }
        }
      }
    }
  }
  if (nrow(framework_compat) == 0) {
    framework_compat <- tibble(
      framework_compat_id = character(),
      framework_id = character(),
      runtime_id = character(),
      language_id = character(),
      min_framework_version = character(),
      max_framework_version = character(),
      evidence_url = character(),
      score = integer(),
      notes = character()
    )
  } else {
    framework_compat <- distinct(
      framework_compat,
      framework_compat_id,
      .keep_all = TRUE
    )
  }

  # framework_gpu_compat: map frameworks to GPU models when the framework is
  # compatible with a runtime and the GPU is known to support that runtime.
  # This implements: "all models of the vendor which have a runtime version
  # compatible with a version compatible with the framework should be included".
  framework_gpu_compat <- tibble()
  if (nrow(framework_compat) > 0 && nrow(gpu_runtime) > 0) {
    for (i in seq_len(nrow(framework_compat))) {
      fc <- framework_compat[i, ]
      # find GPU->runtime rows that reference the same runtime_id
      matched_gr <- gpu_runtime |> filter(runtime_id == fc$runtime_id)
      if (nrow(matched_gr) == 0) {
        next
      }
      for (j in seq_len(nrow(matched_gr))) {
        gr <- matched_gr[j, ]
        key <- paste(
          'framework_gpu',
          fc$framework_id,
          gr$gpu_id,
          fc$runtime_id,
          sep = '|'
        )
        fg_id <- sha256_of(key)
        # evidence: prefer framework page as primary evidence, but include gpu/runtime evidence
        evidence_url <- paste(
          unique(na.omit(c(fc$evidence_url, gr$evidence_url))),
          collapse = ' | '
        )
        # score: conservative min of evidence scores (if present), else framework score
        score <- NA_integer_
        if (!is.null(fc$score) && !is.null(gr$score)) {
          score <- min(as.integer(fc$score), as.integer(gr$score), na.rm = TRUE)
        } else if (!is.null(fc$score)) {
          score <- as.integer(fc$score)
        } else if (!is.null(gr$score)) {
          score <- as.integer(gr$score)
        }
        framework_gpu_compat <- bind_rows(
          framework_gpu_compat,
          tibble(
            framework_gpu_compat_id = fg_id,
            framework_id = fc$framework_id,
            gpu_id = gr$gpu_id,
            runtime_id = fc$runtime_id,
            evidence_url = evidence_url,
            score = score
          )
        )
      }
    }
  }
  if (nrow(framework_gpu_compat) == 0) {
    framework_gpu_compat <- tibble(
      framework_gpu_compat_id = character(),
      framework_id = character(),
      gpu_id = character(),
      runtime_id = character(),
      evidence_url = character(),
      score = integer()
    )
  } else {
    framework_gpu_compat <- distinct(
      framework_gpu_compat,
      framework_gpu_compat_id,
      .keep_all = TRUE
    )
  }

  # Write CSVs per required outputs
  write.csv(
    vendors,
    file = file.path(out_dir, 'vendors.csv'),
    row.names = FALSE
  )
  write.csv(
    gpus,
    file = file.path(out_dir, 'gpu_models.csv'),
    row.names = FALSE
  )
  write.csv(
    runtimes,
    file = file.path(out_dir, 'runtimes.csv'),
    row.names = FALSE
  )
  write.csv(
    languages,
    file = file.path(out_dir, 'languages.csv'),
    row.names = FALSE
  )
  write.csv(
    frameworks,
    file = file.path(out_dir, 'frameworks.csv'),
    row.names = FALSE
  )
  write.csv(
    gpu_runtime,
    file = file.path(out_dir, 'gpu_runtime_compat.csv'),
    row.names = FALSE
  )
  write.csv(
    runtime_language,
    file = file.path(out_dir, 'runtime_language.csv'),
    row.names = FALSE
  )
  write.csv(
    framework_compat,
    file = file.path(out_dir, 'framework_compat.csv'),
    row.names = FALSE
  )
  write.csv(
    framework_gpu_compat,
    file = file.path(out_dir, 'framework_gpu_compat.csv'),
    row.names = FALSE
  )

  # Write SQLite
  db_path <- file.path(out_dir, 'gpu_compat.sqlite')
  con <- dbConnect(RSQLite::SQLite(), db_path)
  dbWriteTable(con, 'vendors', vendors, overwrite = TRUE)
  dbWriteTable(con, 'gpu_models', gpus, overwrite = TRUE)
  dbWriteTable(con, 'runtimes', runtimes, overwrite = TRUE)
  dbWriteTable(con, 'languages', languages, overwrite = TRUE)
  dbWriteTable(con, 'frameworks', frameworks, overwrite = TRUE)
  dbWriteTable(con, 'gpu_runtime_compat', gpu_runtime, overwrite = TRUE)
  dbWriteTable(con, 'runtime_language', runtime_language, overwrite = TRUE)
  dbWriteTable(con, 'framework_compat', framework_compat, overwrite = TRUE)
  dbWriteTable(
    con,
    'framework_gpu_compat',
    framework_gpu_compat,
    overwrite = TRUE
  )
  if (nrow(runtime_requirements) > 0) {
    dbWriteTable(
      con,
      'gpu_runtime_requirements',
      runtime_requirements,
      overwrite = TRUE
    )
    try(
      {
        dbExecute(
          con,
          "CREATE INDEX IF NOT EXISTS idx_runtime_reqs_model ON gpu_runtime_requirements(model_name)"
        )
        dbExecute(
          con,
          "CREATE INDEX IF NOT EXISTS idx_runtime_reqs_runtime ON gpu_runtime_requirements(runtime_name)"
        )
      },
      silent = TRUE
    )
  }
  # Persist authoritative NVIDIA CC mapping if available
  if (nrow(gpu_cc_map) > 0) {
    dbWriteTable(
      con,
      'nvidia_compute_capabilities',
      gpu_cc_map,
      overwrite = TRUE
    )
    try(
      {
        dbExecute(
          con,
          "CREATE INDEX IF NOT EXISTS idx_nv_cc_model ON nvidia_compute_capabilities(model_name)"
        )
      },
      silent = TRUE
    )
  }
  # Schema versioning table
  try(
    {
      schema_tbl <- tibble(
        version = '1.1',
        created_at = as.character(Sys.time())
      )
      dbWriteTable(con, 'schema_version', schema_tbl, overwrite = TRUE)
    },
    silent = TRUE
  )
  # create helpful indices
  try(
    {
      dbExecute(
        con,
        "CREATE INDEX IF NOT EXISTS idx_gpu_models_gpu_id ON gpu_models(gpu_id)"
      )
      dbExecute(
        con,
        "CREATE INDEX IF NOT EXISTS idx_gpu_models_vendor_id ON gpu_models(vendor_id)"
      )
      dbExecute(
        con,
        "CREATE INDEX IF NOT EXISTS idx_gpu_models_model_name ON gpu_models(model_name)"
      )
      dbExecute(
        con,
        "CREATE INDEX IF NOT EXISTS idx_runtimes_name_version ON runtimes(name, version)"
      )
      dbExecute(
        con,
        "CREATE INDEX IF NOT EXISTS idx_runtimes_name ON runtimes(name)"
      )
      dbExecute(
        con,
        "CREATE INDEX IF NOT EXISTS idx_framework_gpu_compat_runtime_id ON framework_gpu_compat(runtime_id)"
      )
      dbExecute(
        con,
        "CREATE INDEX IF NOT EXISTS idx_framework_gpu_compat_framework_id ON framework_gpu_compat(framework_id)"
      )
      dbExecute(
        con,
        "CREATE INDEX IF NOT EXISTS idx_framework_gpu_compat_gpu_id ON framework_gpu_compat(gpu_id)"
      )
      dbExecute(
        con,
        "CREATE INDEX IF NOT EXISTS idx_gpu_runtime_compat_pair ON gpu_runtime_compat(gpu_id, runtime_id)"
      )
      dbExecute(
        con,
        "CREATE INDEX IF NOT EXISTS idx_runtime_language_runtime_lang ON runtime_language(runtime_id, language_id)"
      )
      dbExecute(
        con,
        "CREATE INDEX IF NOT EXISTS idx_framework_compat_fw_rt_lang ON framework_compat(framework_id, runtime_id, language_id)"
      )

      # create compat_map view that flattens framework<->gpu<->runtime<->language relationships
      dbExecute(con, "DROP VIEW IF EXISTS compat_map")
      dbExecute(
        con,
        "CREATE VIEW compat_map AS
      SELECT
        v.name AS vendor,
        gm.model_normalized AS model_normalized,
        gm.model_name AS model_name,
        gm.compute_capability AS compute_capability,
        rt.name AS runtime_name,
        rt.version AS runtime_version,
        rt.version_key AS runtime_version_key,
        lg.name AS language_name,
        COALESCE(fc.min_language_version, rl.min_language_version) AS min_language_version,
        COALESCE(fc.min_language_version_key, rl.min_language_version_key) AS language_version_key,
        fw.name AS framework_name,
        fc.min_framework_version AS min_framework_version,
        fc.min_framework_version_key AS framework_version_key,
        COALESCE(fg.evidence_url, '') || CASE WHEN COALESCE(fc.evidence_url, '') <> '' THEN ' | ' || fc.evidence_url ELSE '' END AS evidence_urls,
        COALESCE(fg.score, fc.score, 0) AS score
      FROM framework_gpu_compat fg
      JOIN gpu_models gm ON fg.gpu_id = gm.gpu_id
      JOIN vendors v ON gm.vendor_id = v.vendor_id
      JOIN frameworks fw ON fg.framework_id = fw.framework_id
      LEFT JOIN framework_compat fc ON fc.framework_id = fw.framework_id AND fc.runtime_id = fg.runtime_id
      LEFT JOIN runtimes rt ON rt.runtime_id = fg.runtime_id
      LEFT JOIN runtime_language rl ON rl.runtime_id = fg.runtime_id AND rl.language_id = COALESCE(fc.language_id, rl.language_id)
      LEFT JOIN languages lg ON lg.language_id = COALESCE(fc.language_id, rl.language_id)"
      )
    },
    silent = TRUE
  )
  dbDisconnect(con)

  # Also export a flattened compat_map CSV from the SQLite view (if available)
  # Attempt to read the compat_map view with version keys; if it fails, try a fallback without keys.
  compat_csv <- file.path(out_dir, 'compat_map.csv')
  try(
    {
      con2 <- dbConnect(RSQLite::SQLite(), db_path)
      qry <- "SELECT vendor, model_normalized, model_name, compute_capability, runtime_name, runtime_version, runtime_version_key, language_name, COALESCE(min_language_version, '') AS min_language_version, language_version_key, framework_name, COALESCE(min_framework_version, '') AS min_framework_version, framework_version_key, evidence_urls, score FROM compat_map"
      cm <- dbGetQuery(con2, qry)
      dbDisconnect(con2)
      write.table(
        cm,
        file = compat_csv,
        sep = ",",
        row.names = FALSE,
        col.names = TRUE,
        quote = FALSE
      )
    },
    silent = TRUE
  )

  list(
    vendors = vendors,
    gpu_models = gpus,
    runtimes = runtimes,
    languages = languages,
    frameworks = frameworks,
    gpu_runtime = gpu_runtime,
    runtime_language = runtime_language,
    framework_compat = framework_compat,
    framework_gpu_compat = framework_gpu_compat,
    sqlite = db_path
  )
}
