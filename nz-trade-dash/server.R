## Server for financial benchmarking dashboard (NZ trade logic removed)

library(shiny)
library(shinyjs)
library(shinydashboard)
library(dplyr)
library(magrittr)
library(purrr)
library(tidyr)
library(stringr)
library(readr)
library(readxl)
library(httr)
library(xml2)
library(plotly)
library(prophet)
library(janitor)
library(jsonlite)
library(scales)
library(stringi)
library(zip)
library(tibble)

utils::globalVariables(c(
  "corp_name", "stock_code", "year", "sales", "inventory",
  "net_income", "total_assets", "cogs", "ds", "yhat"
))

## Financial benchmarking helpers (ported from app.R with fin_ prefix)
fin_load_api_key <- function() {
  key <- ""
  pick_env <- function() {
    for (nm in c("DART_API_KEY", "DART_KEY", "DART_API")) {
      val <- Sys.getenv(nm)
      if (nzchar(val)) return(val)
    }
    NULL
  }
  read_env_key <- function(path) {
    if (!file.exists(path)) return(NULL)
    lines <- readLines(path, warn = FALSE)
    lines <- trimws(lines)
    lines <- lines[nzchar(lines) & !startsWith(lines, "#")]
    if (length(lines) == 0) return(NULL)
    kv <- strsplit(lines, "=", fixed = TRUE)
    kv <- Filter(function(x) length(x) == 2, kv)
    if (length(kv) == 0) return(NULL)
    clean_val <- function(v) {
      v <- trimws(v)
      sub("^['\\\"](.*)['\\\"]$", "\\1", v)
    }
    env_list <- setNames(
      vapply(kv, function(x) clean_val(x[[2]]), character(1)),
      trimws(vapply(kv, `[`, character(1), 1))
    )
    for (nm in c("DART_API_KEY", "DART_KEY", "DART_API")) {
      if (!is.null(env_list[[nm]]) && nzchar(env_list[[nm]])) return(env_list[[nm]])
    }
    NULL
  }

  key <- pick_env()
  if (!is.null(key) && nzchar(key)) return(key)

  if (requireNamespace("dotenv", quietly = TRUE)) {
    try(dotenv::load_dotenv(), silent = TRUE)
    key <- pick_env()
    if (!is.null(key) && nzchar(key)) return(key)
  }

  for (p in c(".env", file.path("nz-trade-dash", ".env"))) {
    key <- read_env_key(p)
    if (!is.null(key) && nzchar(key)) return(key)
  }
  NULL
}

fin_http_get_with_retry <- function(url, params = list(), timeout_sec = 40) {
  resp <- httr::RETRY(
    "GET",
    url,
    query = params,
    httr::timeout(timeout_sec),
    times = 3,
    pause_base = 1,
    pause_cap = 4
  )
  httr::stop_for_status(resp)
  resp
}

fin_fetch_corp_codes_cached <- function(api_key) {
  url <- "https://opendart.fss.or.kr/api/corpCode.xml"
  resp <- fin_http_get_with_retry(url, params = list(crtfc_key = api_key))
  ctype <- headers(resp)[["content-type"]]
  if (!ctype %in% c("application/zip", "application/x-zip-compressed")) {
    stop("DART API 응답이 ZIP 파일이 아닙니다. API KEY를 확인하세요.")
  }
  tf <- tempfile(fileext = ".zip")
  writeBin(content(resp, "raw"), tf)
  files <- basename(zip::zip_list(tf)$filename)
  pick <- files[str_detect(tolower(files), "corpcode")]
  xml_file <- if (length(pick)) pick[[1]] else "CORPCODE.xml"
  xml_path <- tempfile(fileext = ".xml")
  zip::unzip(tf, files = xml_file, exdir = dirname(xml_path))
  tree <- read_xml(file.path(dirname(xml_path), xml_file))
  nodes <- xml_find_all(tree, ".//list")
  df <- tibble(
    corp_code = xml_text(xml_find_all(nodes, "./corp_code")),
    corp_name = xml_text(xml_find_all(nodes, "./corp_name")),
    stock_code = xml_text(xml_find_all(nodes, "./stock_code"))
  )
  write_csv(df, "corp_codes.csv")
  df
}

fin_get_corp_codes <- function(api_key, path = "corp_codes.csv") {
  if (file.exists(path)) {
    try({
      df <- read_csv(path, show_col_types = FALSE, locale = locale(encoding = "UTF-8"))
      return(df %>%
        mutate(across(everything(), ~trimws(as.character(.)))))
    }, silent = TRUE)
  }
  if (!nzchar(api_key)) stop("API Key 없음")
  fin_fetch_corp_codes_cached(api_key)
}

fin_demo_corp_codes <- function() {
  tibble(
    corp_code = c("00000001", "00000002", "00000003"),
    corp_name = c("한섬", "신세계인터내셔날", "코오롱인더스트리"),
    stock_code = c("020000", "031430", "120110")
  )
}

fin_norm_name <- function(s) {
  if (is.null(s)) return("")
  s <- stringi::stri_trans_nfkc(as.character(s))
  s <- tolower(gsub("[\\s\\-\\_\\.\\&\\/\\(\\)\\[\\],\\+]+", "", s))
  s <- str_replace_all(s, "주식회사|\\(주\\)|㈜", "")
  s
}

fin_alias_map <- c(
  "f&f" = "에프앤에f",
  "ff" = "에프앤에f",
  "fnf" = "에프앤에프",
  "thehandsome" = "한섬",
  "handsome" = "한섬",
  "shinsegaeinternational" = "신세계인터내셔날",
  "shinsegaeintl" = "신세계인터내셔날",
  "kolonindustries" = "코오롱인더스트리"
)

fin_make_corp_choices <- function(df) {
  if (is.null(df) || nrow(df) == 0) return(setNames(character(), character()))
  labels <- ifelse(
    !is.na(df$stock_code) & nzchar(df$stock_code),
    paste0(df$corp_name, " (", df$stock_code, ")"),
    df$corp_name
  )
  setNames(df$corp_code, labels)
}

<<<<<<< HEAD
fin_search_corp_smart <- function(corp_df, query, limit = 30) {
  if (is.null(query) || query == "") return(head(corp_df, limit))
  q <- trimws(query)
  if (!nzchar(q)) return(head(corp_df, limit))
  q_norm <- fin_norm_name(q)
  if (str_detect(q, "^\\d{6}$")) {
    hit <- corp_df %>% filter(.data$stock_code == q)
    if (nrow(hit)) return(head(hit, limit))
  }
  alias <- if (q_norm %in% names(fin_alias_map)) fin_alias_map[[q_norm]] else NULL
  tokens <- if (!is.null(alias)) {
    fin_norm_name(alias)
  } else {
    tks <- str_split(q, "\\s+")[[1]]
    tks <- tks[nzchar(tks)]
    if (length(tks) == 0) q_norm else map_chr(tks, fin_norm_name)
  }
  norm_series <- corp_df %>% mutate(norm_name = map_chr(.data$corp_name, fin_norm_name))
  mask <- map_lgl(
    norm_series$norm_name,
    function(x) all(vapply(tokens, function(t) str_detect(x, fixed(t)), logical(1)))
  )
  res <- norm_series[mask, ]
  if (nrow(res)) return(head(select(res, -norm_name), limit))
  loose <- corp_df %>% filter(
    str_detect(.data$corp_name, regex(q, ignore_case = TRUE)) |
      str_detect(.data$stock_code, q)
  )
  head(loose, limit)
=======
fin_search_corp_smart <- function(corp_df, query, limit = 200) {
   if (is.null(query) || query == "") return(head(corp_df, limit))
   q <- trimws(query)
   if (!nzchar(q)) return(head(corp_df, limit))
   if (str_detect(q, "^\\d{8}$")) {
      hit <- corp_df %>% filter(.data$corp_code == q)
      if (nrow(hit)) return(head(hit, limit))
   }
   q_norm <- fin_norm_name(q)
   if (str_detect(q, "^\\d{6}$")) {
      hit <- corp_df %>% filter(.data$stock_code == q)
      if (nrow(hit)) return(head(hit, limit))
   }
   alias <- if (q_norm %in% names(fin_alias_map)) fin_alias_map[[q_norm]] else NULL
   tokens <- if (!is.null(alias)) {
      fin_norm_name(alias)
   } else {
      tks <- str_split(q, "\\s+")[[1]]
      tks <- tks[nzchar(tks)]
      if (length(tks) == 0) q_norm else map_chr(tks, fin_norm_name)
   }
   norm_series <- corp_df %>% mutate(norm_name = map_chr(.data$corp_name, fin_norm_name))
   mask <- map_lgl(
      norm_series$norm_name,
      function(x) all(vapply(tokens, function(t) str_detect(x, fixed(t)), logical(1)))
   )
   res <- norm_series[mask, ]
   if (nrow(res)) return(head(select(res, -norm_name), limit))
   loose <- corp_df %>% filter(
      str_detect(.data$corp_name, regex(q, ignore_case = TRUE)) |
         str_detect(.data$stock_code, q)
   )
   head(loose, limit)
>>>>>>> main
}

fin_safe_num <- function(x) {
  if (is.null(x) || length(x) == 0) return(NA_real_)
  as.numeric(gsub("[^0-9\\.\\-]", "", x))
}

fin_extract_accounts <- function(acct_tbl) {
  if (is.null(acct_tbl) || nrow(acct_tbl) == 0) {
    return(tibble(sales = NA_real_, inventory = NA_real_, net_income = NA_real_, total_assets = NA_real_, cogs = NA_real_))
  }
  pick_first <- function(names) {
    hit <- acct_tbl %>% filter(.data$account_nm %in% names) %>% slice_head(n = 1)
    if (nrow(hit) == 0) return(NA_real_)
    fin_safe_num(hit$thstrm_amount)
  }
  tibble(
    sales = pick_first(c("매출액", "수익(매출액)", "수익")),
    inventory = pick_first(c("재고자산")),
    net_income = pick_first(c("당기순이익", "당기순이익(손실)")),
    total_assets = pick_first(c("자산총계")),
    cogs = pick_first(c("매출원가"))
  )
}

fin_dart_single_acnt <- function(api_key, corp_code, year, reprt_code = "11014", fs_div = "CFS") {
  corp_code <- trimws(as.character(corp_code))
  year <- as.integer(year)
  if (!nzchar(corp_code) || is.na(year)) stop("잘못된 corp_code/year 값")
  if (is.null(api_key) || !nzchar(api_key)) stop("API Key 없음")
  url <- "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"
  resp <- tryCatch(
    fin_http_get_with_retry(
      url,
      params = list(
        crtfc_key = api_key,
        corp_code = corp_code,
        bsns_year = year,
        reprt_code = reprt_code,
        fs_div = fs_div
      ),
      timeout_sec = 40
    ),
    error = function(e) {
      stop(paste0("HTTP 실패 (", year, "): ", conditionMessage(e)))
    }
  )
  txt <- content(resp, "text", encoding = "UTF-8")
  js <- jsonlite::fromJSON(txt, simplifyVector = TRUE)
  if (is.null(js$status) || js$status != "000") {
    msg <- if (!is.null(js$message)) js$message else "DART 응답 오류"
    stop(msg)
  }
  if (is.null(js$list) || length(js$list) == 0) stop("DART 응답에 계정 목록이 없습니다.")
  as_tibble(js$list)
}

fin_fetch_dart_financials <- function(api_key, corp_code, corp_name, years) {
  years <- years[!is.na(years)]
  years <- as.integer(unique(years))
  reprt_codes <- c("11011", "11014", "11012", "11013")
  fs_divs <- c("CFS", "OFS")
  outcomes <- purrr::map(years, function(y) {
    best <- NULL
    errs <- c()
    done <- FALSE
    for (rc in reprt_codes) {
      if (done) break
      for (fs in fs_divs) {
        tryCatch({
          accts <- fin_dart_single_acnt(api_key, corp_code, y, reprt_code = rc, fs_div = fs)
          vals <- fin_extract_accounts(accts)
          best <- mutate(vals, year = y, source = paste0(corp_name, " (", fs, "/", rc, ")"))
          done <- TRUE
          break
        }, error = function(e) {
          errs <<- c(errs, paste0(fs, "/", rc, ": ", conditionMessage(e)))
        })
      }
    }
    if (!is.null(best)) {
      list(data = best, error = NULL)
    } else {
      list(data = NULL, error = paste(unique(errs), collapse = " / "))
    }
  })
  data_list <- purrr::compact(purrr::map(outcomes, "data"))
  df <- if (length(data_list)) {
    bind_rows(data_list) %>%
      select(.data$year, .data$sales, .data$inventory, .data$net_income, .data$total_assets, .data$cogs, .data$source)
  } else {
    NULL
  }
  tibble(
    year = years,
    ok = purrr::map_lgl(outcomes, ~is.null(.x$error)),
    message = purrr::map_chr(outcomes, ~if (is.null(.x$error)) "" else .x$error)
  ) %>%
    mutate(year = as.integer(year)) %>%
    list(status = ., data = df)
}

fin_sample_dart_financials <- function(corp_name = "상장사", years = 2019:2023) {
  tibble(
    year = years,
    sales = seq(120, 220, length.out = length(years)) * 1e8,
    inventory = seq(30, 55, length.out = length(years)) * 1e7,
    net_income = seq(8, 18, length.out = length(years)) * 1e7,
    total_assets = seq(200, 240, length.out = length(years)) * 1e8,
    cogs = seq(70, 130, length.out = length(years)) * 1e8,
    source = corp_name
  )
}

fin_sample_my_company <- function(years = 2019:2023) {
  tibble(
    year = years,
    sales = seq(80, 160, length.out = length(years)) * 1e8,
    inventory = seq(25, 45, length.out = length(years)) * 1e7,
    net_income = seq(5, 12, length.out = length(years)) * 1e7,
    total_assets = seq(120, 180, length.out = length(years)) * 1e8,
    cogs = seq(50, 90, length.out = length(years)) * 1e8,
    source = "My Company"
  )
}

<<<<<<< HEAD
fin_safe_prophet <- function(df, horizon, return_full = FALSE) {
  req(nrow(df) > 2)
  n_chg <- max(0, min(5, nrow(df) - 1))
  m <- prophet(
    df %>% transmute(ds = as.Date(paste0(.data$year, "-12-31")), y = .data$sales),
    yearly.seasonality = TRUE,
    weekly.seasonality = FALSE,
    daily.seasonality = FALSE,
    n.changepoints = n_chg
  )
  future <- make_future_dataframe(m, periods = horizon, freq = "year")
  forecast <- predict(m, future)
  preds <- forecast %>%
    transmute(
      year = as.integer(format(.data$ds, "%Y")),
      yhat = .data$yhat,
      yhat_lower = .data$yhat_lower,
      yhat_upper = .data$yhat_upper
    ) %>%
    filter(.data$year > max(df$year, na.rm = TRUE))
  if (return_full) {
    list(model = m, forecast = forecast, preds = preds)
  } else {
    preds
  }
=======
fin_pick_source <- function(df_all) {
   if (is.null(df_all) || nrow(df_all) == 0 || !"source" %in% names(df_all)) return(NULL)
   my_rows <- df_all %>% filter(.data$source == "My Company")
   if (nrow(my_rows) > 0) return("My Company")
   non_my <- df_all %>% filter(.data$source != "My Company")
   if (nrow(non_my) > 0) return(non_my$source[[1]])
   df_all$source[[1]]
}

fin_safe_prophet <- function(df, horizon, source_name = NULL) {
   req(nrow(df) > 2)
   horizon <- max(1L, as.integer(horizon))
   src <- if (!is.null(source_name)) {
      source_name
   } else if ("source" %in% names(df) && length(df$source)) {
      df$source[[1]]
   } else {
      "Series"
   }
   df <- df %>% arrange(.data$year)
   m <- prophet(df %>% transmute(ds = as.Date(paste0(.data$year, "-12-31")), y = .data$sales))
   future <- make_future_dataframe(m, periods = horizon, freq = "year")
   preds <- predict(m, future) %>% mutate(year = as.integer(format(.data$ds, "%Y")))
   last_year <- max(df$year, na.rm = TRUE)
   forecast <- preds %>% filter(.data$year > last_year)
   fitted <- preds %>%
      filter(.data$year <= last_year) %>%
      select(.data$year, .data$yhat, .data$yhat_lower, .data$yhat_upper) %>%
      left_join(df %>% select(.data$year, sales), by = "year") %>%
      rename(actual = .data$sales) %>%
      mutate(resid = .data$actual - .data$yhat)
   list(
      source = src,
      model = m,
      forecast = forecast %>% select(.data$year, .data$yhat, .data$yhat_lower, .data$yhat_upper, .data$trend),
      fitted = fitted,
      full = preds %>% select(.data$year, .data$ds, .data$yhat, .data$yhat_lower, .data$yhat_upper, .data$trend),
      history = df,
      horizon = horizon
   )
>>>>>>> main
}

## Additional small helpers (file path / upload)
fin_find_corp_codes_path <- function() {
  for (p in c("corp_codes.csv", file.path("nz-trade-dash", "corp_codes.csv"))) {
    if (file.exists(p)) return(p)
  }
  NULL
}

fin_read_upload_df <- function(path) {
  ext <- tools::file_ext(path) %>% tolower()
  if (ext %in% c("xlsx", "xls")) return(read_excel(path) %>% clean_names())
  read_csv(path, show_col_types = FALSE) %>% clean_names()
}

fin_guess_col <- function(cols, patterns) {
  hit <- which(str_detect(cols, regex(paste(patterns, collapse = "|"), ignore_case = TRUE)))[1]
  if (length(hit) && !is.na(hit)) cols[[hit]] else cols[[1]]
}

## Server -----------------------------------------------------------------
server <- function(input, output, session) {
  ## Header navigation: icon -> Main Dashboard
  observeEvent(input$go_dashboard, {
    updateTabItems(session, "sidebar", "dashboard")
  })

  ## Financial benchmarking state
  fin_values <- reactiveValues(
    corp_df = NULL,
    df_dart = NULL,
    df_my = NULL,
    df_my_norm = NULL,
    corp_real_loaded = FALSE
  )
  pred_values <- reactiveValues(
    base = NULL,
    model = NULL,
    forecast = NULL,
    preds = NULL,
    horizon = NULL
  )

  fin_validate <- shiny::validate
  fin_need <- shiny::need

  fin_ensure_corp_df <- function() {
    if (!is.null(fin_values$corp_df) && nrow(fin_values$corp_df) > 0) return()
    corp_path <- fin_find_corp_codes_path()
    if (!is.null(corp_path)) {
      fin_values$corp_df <- tryCatch(
        fin_get_corp_codes("", corp_path),
        error = function(e) {
          showNotification("corp_codes.csv 읽기 실패: 데모 리스트로 대체합니다.", type = "error", duration = 6)
          fin_demo_corp_codes()
        }
      )
      fin_values$corp_real_loaded <- TRUE
    } else {
      fin_values$corp_df <- fin_demo_corp_codes()
    }
  }

  # 기본적으로 데모 데이터를 미리 채워서 화면이 비어 보이지 않도록 함
  observeEvent(TRUE, {
    if (is.null(fin_values$df_dart) && is.null(fin_values$df_my_norm)) {
      fin_values$df_dart <- fin_sample_dart_financials(corp_name = "상장사(데모)")
      fin_values$df_my_norm <- fin_sample_my_company()
    }
  }, once = TRUE)

  observe({
    if (is.null(fin_values$corp_df)) {
      corp_path <- fin_find_corp_codes_path()
      if (!is.null(corp_path)) {
        fin_values$corp_df <- tryCatch(
          fin_get_corp_codes("", corp_path),
          error = function(e) {
            showNotification("corp_codes.csv 읽기 실패: 데모 리스트로 대체합니다.", type = "error", duration = 6)
            fin_demo_corp_codes()
          }
        )
        fin_values$corp_real_loaded <- TRUE
      } else {
        fin_values$corp_df <- fin_demo_corp_codes()
      }
    }
  })

  observeEvent(fin_values$corp_df, {
    df <- fin_values$corp_df
    if (is.null(df) || nrow(df) == 0) return()
    choices <- fin_make_corp_choices(head(df, 15))
    updateSelectInput(session, "fin_corp_pick", choices = choices, selected = choices[[1]])
  })

  observeEvent(input$fin_corp_search, {
    fin_ensure_corp_df()
    query <- if (is.null(input$fin_corp_query)) "" else trimws(input$fin_corp_query)
    if (!nzchar(query)) {
      showNotification("검색어를 입력하세요.", type = "warning", duration = 3)
      return()
    }
    if (is.null(fin_values$corp_df) || nrow(fin_values$corp_df) == 0) {
      showNotification("기업 리스트가 준비되지 않았습니다. 잠시 후 다시 시도하거나 데모를 사용하세요.", type = "error", duration = 4)
      return()
    }
    withProgress(message = "검색 중...", value = 0.1, {
      if (!fin_values$corp_real_loaded) {
        corp_path <- fin_find_corp_codes_path()
        if (!is.null(corp_path)) {
          setProgress(0.2, detail = "로컬 corp_codes.csv 읽는 중")
          fin_values$corp_df <- tryCatch(
            fin_get_corp_codes("", corp_path),
            error = function(e) {
              showNotification("corp_codes.csv 읽기 실패: 데모 리스트로 대체합니다.", type = "error", duration = 6)
              fin_demo_corp_codes()
            }
<<<<<<< HEAD
          )
          fin_values$corp_real_loaded <- TRUE
        } else {
          api_key <- fin_load_api_key()
          if (!is.null(api_key)) {
            withProgress(message = "DART 기업 리스트 불러오는 중...", value = 0.3, {
              fin_values$corp_df <- tryCatch(
                fin_get_corp_codes(api_key),
                error = function(e) {
                  showNotification("DART corpCode 조회 실패: 데모 리스트로 대체합니다.", type = "error", duration = 6)
                  fin_demo_corp_codes()
                }
              )
              fin_values$corp_real_loaded <- TRUE
            })
          } else {
            showNotification("DART_API_KEY가 없어 데모 리스트를 사용합니다.", type = "warning", duration = 5)
          }
        }
      }
      df <- fin_values$corp_df
      if (is.null(df) || nrow(df) == 0) {
        showNotification("기업 리스트가 없습니다. 데모/키 설정을 확인하세요.", type = "error", duration = 5)
        return()
=======
         )
         
         ## 4.1 setup hs group table and related reative values ---------------
         rv_intelHS$ie <- input$rbtn_intel_by_hs  ## imports or exports?
         rv_intelHS$total_ie <- paste0('Total ', tolower(rv_intelHS$ie) ) ## Total exports or Total imports
         rv_intelHS$selected_hs_table <- concord_hs24[input$HSCodeTable_rows_selected,] ## selected table
         rv_intelHS$hs <- rv_intelHS$selected_hs_table$HS_codes  ## hs codes
         rv_intelHS$classification <- rv_intelHS$selected_hs_table$HS_description ## hs code desription
         rv_intelHS$hs_group <- 
            data.frame(HS_code = rv_intelHS$hs,
                       HS_group = rv_intelHS$classification) %>%
            arrange( HS_code )
         
         ## 4.2 commodity by country data ----------
         rv_intelHS$tmp_dtf_shiny_full <-
            dtf_shiny_full %>%
            filter( Type_ie == rv_intelHS$ie, 
                    Commodity %in% rv_intelHS$hs_group$HS_code ) %>%
            left_join( rv_intelHS$hs_group, by = c('Commodity' = 'HS_code') ) %>%
            left_join( concord_country_iso_latlon_raw, by = 'Country' ) %>%
            group_by( Year, Country, Type_ie, Type_gs, HS_group, ISO2, lat, lon, Note ) %>%
            summarise( Value = sum(Value, na.rm=T) ) %>%
            ungroup
         
         ## 4.3 commodity only data ------------
         rv_intelHS$tmp_dtf_shiny_full_commodity_only <-
            rv_intelHS$tmp_dtf_shiny_full %>%
            group_by( Year,  Type_ie, Type_gs, HS_group, Note ) %>%
            summarise( Value = sum(Value, na.rm=T) ) %>%
            ungroup %>%
            mutate( Country = 'World' )
         
         ## 4.4 Data for Build export/import value line chart ---------
         rv_intelHS$tmp_top_g <-
            rv_intelHS$tmp_dtf_shiny_full_commodity_only %>%
            filter( Year == max(Year)) %>% 
            arrange( -Value ) %>%
            dplyr::select( HS_group ) %>%
            as.matrix() %>%
            as.character
         
         ## top selected commodities and top 5services
         rv_intelHS$tmp_top <- c( rv_intelHS$tmp_top_g) #, tmp_top_s_ex)
         
         ## data frame to plot
         rv_intelHS$tmp_dtf_key_line <- 
            rv_intelHS$tmp_dtf_shiny_full_commodity_only %>%
            filter( HS_group %in% rv_intelHS$tmp_top,
                    Year >=2007) %>%
            mutate( Value = round(Value/10^6),
                    HS_group = factor(HS_group, levels = rv_intelHS$tmp_top)
            ) %>%
            arrange( HS_group )
         
         ## 4.5 Data for build export/import percent line chart ---------
         rv_intelHS$tmp_tot <-
            dtf_shiny_full %>%
            filter( Country == 'World',
                    Type_ie == rv_intelHS$ie,
                    Year >= 2007 )  %>%
            mutate( Value = round(Value/10^6) ) %>%
            group_by( Year, Country, Type_ie ) %>%
            summarize( Value = sum(Value, na.rm=T) ) %>%
            ungroup %>%
            mutate( HS_group =  rv_intelHS$total_ie )
         
         rv_intelHS$tmp_dtf_percent_line <-
            rv_intelHS$tmp_dtf_key_line %>%
            bind_rows( rv_intelHS$tmp_tot ) %>%
            group_by( Year, Country, Type_ie ) %>%
            mutate( Share = Value/Value[HS_group == rv_intelHS$total_ie ], #'Total exports' ],
                    Value = Share*100 ) %>%
            ungroup %>%
            filter( HS_group != rv_intelHS$total_ie) %>% #'Total exports' ) %>%
            mutate( HS_group = factor(HS_group, levels = rv_intelHS$tmp_top ) ) %>%
            arrange( HS_group )
         
         ## 4.6 Data for build export value change table ----------------
         rv_intelHS$tmp_dtf_key_tab <- 
            rv_intelHS$tmp_dtf_shiny_full_commodity_only %>%
            filter( HS_group %in% rv_intelHS$tmp_top) %>%
            mutate( HS_group = factor(HS_group, levels = rv_intelHS$tmp_top) ) %>%
            arrange( HS_group )
         
         rv_intelHS$tmp_tab_nohs <-
            rv_intelHS$tmp_dtf_key_tab %>%
            mutate( Name =  HS_group ) %>%
            group_by( Name) %>%
            do(CAGR1 = CAGR( .$Value[.$Year == max(.$Year)]/
                                .$Value[.$Year == (max(.$Year)-1)], 1)/100,
               CAGR5 = CAGR( .$Value[.$Year == max(.$Year)]/
                                .$Value[.$Year == (max(.$Year)-5)], 5)/100,
               CAGR10 = CAGR( .$Value[.$Year == max(.$Year)]/
                                 .$Value[.$Year == (max(.$Year)-10)], 10)/100 ,
               ABS5 = .$Value[.$Year == max(.$Year)] - .$Value[.$Year == (max(.$Year)-5)],
               ABS10 = .$Value[.$Year == max(.$Year)] - .$Value[.$Year == (max(.$Year)-10)]
            ) %>%
            ungroup %>%
            mutate( CAGR1 = as.numeric(CAGR1),
                    CAGR5 = as.numeric(CAGR5),
                    CAGR10 = as.numeric(CAGR10),
                    ABS5 = as.numeric(ABS5),
                    ABS10 = as.numeric(ABS10)
            ) %>%
            left_join( rv_intelHS$tmp_dtf_key_tab , 
                       by =c('Name'='HS_group') ) %>%
            left_join( rv_intelHS$tmp_dtf_percent_line %>% dplyr::select( -Value) %>% rename(Name = HS_group) ) %>%
            filter( Year == max(Year) ) %>%
            mutate( Value = Value/10^6, ABS5 = ABS5/10^6, ABS10 = ABS10/10^6 ) %>%
            dplyr::select( Name, Value, Share, CAGR1, CAGR5, CAGR10, ABS5, ABS10) %>%
            mutate( Name = factor(Name, levels = rv_intelHS$tmp_top),
                    CAGR1 = ifelse(CAGR1 %in% c(Inf,-Inf), NA, CAGR1),
                    CAGR5 = ifelse(CAGR5 %in% c(Inf,-Inf), NA, CAGR5),
                    CAGR10 = ifelse(CAGR10 %in% c(Inf,-Inf), NA, CAGR10)
            ) %>%
            arrange( Name )
         
         ### join back to hs code
         rv_intelHS$hs_group_flat <- 
            rv_intelHS$hs_group %>%
            group_by( HS_group ) %>%
            summarise( HS_code = paste0(HS_code, collapse = '; ') ) %>%
            ungroup
         
         rv_intelHS$tmp_tab <- 
            rv_intelHS$tmp_tab_nohs %>%
            left_join( rv_intelHS$hs_group_flat, by = c("Name"= 'HS_group') ) %>%
            dplyr::select( HS_code, Name, Value, Share, CAGR1, CAGR5, CAGR10, ABS5, ABS10 )
         
         
         ## 4.7 Data Build exports/imports by country output groups -------------------
         ## The name of the selected commodity
         rv_intelHS$tmp_selected <- 
               input$select_comodity_for_market_analysis
         
         ## The HS codes of the selected commodity
         rv_intelHS$tmp_hs <- 
            rv_intelHS$hs_group$HS_code[rv_intelHS$hs_group$HS_group == rv_intelHS$tmp_selected ]
         
         ## The data from of the selected commodity by markets
         rv_intelHS$tmp_dtf_market <- 
               dtf_shiny_full %>%
                  filter( Commodity %in% rv_intelHS$tmp_hs, 
                          Year >= 2007,
                          Type_ie == rv_intelHS$ie ) %>%
                  left_join( concord_country_iso_latlon_raw, by = 'Country' ) %>%
                  left_join( rv_intelHS$hs_group, by = c('Commodity' = 'HS_code') ) %>%
                  group_by( Year, Country, Type_ie, Type_gs, Note, ISO2, lat, lon ) %>%
                  summarize( Value = sum(Value, na.rm=T) ) %>%
                  ungroup %>%
                  mutate( Commodity = as.character( rv_intelHS$tmp_selected ) )
         
         ## 4.8 Data for Value Line and Percentage line for selected commodities ----------------
         rv_intelHS$tmp_dtf_line_selected <-
            rv_intelHS$tmp_dtf_key_line %>%
                  filter( HS_group %in% as.character( rv_intelHS$tmp_selected ))
         
         ## percentage line
         rv_intelHS$tmp_dtf_percent_selected_line <-
            rv_intelHS$tmp_dtf_percent_line %>%
                  filter( HS_group %in% as.character( rv_intelHS$tmp_selected ) )
         
         ## 4.9 Data for build highchart map  ---------------------------
         rv_intelHS$tmp_dtf_market_map <- 
            rv_intelHS$tmp_dtf_market %>%
                  filter( Year == max(Year),
                          !is.na(lat) ) %>%
                  mutate( Value = Value/10^6,
                          z= Value,
                          name = Country)
         
         
         
         ## 4.10 Data for Top markets for selected commodity line chart ----------------
         rv_intelHS$tmp_top_country_selected <- 
               rv_intelHS$tmp_dtf_market %>%
                  filter( Year == max(Year),
                          Value > 0 , 
                          !Country %in% c("World", 
                                          "Destination Unknown - EU")
                  ) %>% ## 1 bn commodity
                  arrange( -Value ) %>%
                  dplyr::select( Country ) %>%
                  as.matrix() %>%
                  as.character
         
         ### only show top 10 countries 
         rv_intelHS$tmp_top10_country_selected <-
            rv_intelHS$tmp_top_country_selected[1:min(10,length( rv_intelHS$tmp_top_country_selected  ))]
         

         ### derive datafrom for the line plot
         rv_intelHS$tmp_dtf_market_line <- 
            rv_intelHS$tmp_dtf_market %>%
                  filter( Country %in%  as.character( rv_intelHS$tmp_top_country_selected ) ) %>%
                  mutate( Value = Value/10^6 ,
                          Country = factor(Country, levels = as.character( rv_intelHS$tmp_top_country_selected ) )
                  ) %>%
                  arrange(Country)
         
         
         rv_intelHS$tmp_dtf_market_line_percent <- 
            rv_intelHS$tmp_dtf_market_line %>%
                  group_by(Year, Type_ie, Type_gs, Note, Commodity) %>%
                  mutate( Share = Value/sum(Value, na.rm=T)) %>%
                  ungroup %>%
                  mutate( Value = Share*100 ) 
         
         
         ## 4.11 Data for Growth prospective tab ----------------------
         rv_intelHS$tmp_tab_growth <-
            rv_intelHS$tmp_dtf_market_line %>%
                  #filter( Country %in% as.character(tmp_top10_country_selected_ex()) ) %>%
                  mutate( Name =  Country ) %>%
                  group_by( Name) %>%
                  do( CAGR1 = CAGR( .$Value[.$Year == max(.$Year)]/
                                       .$Value[.$Year == (max(.$Year)-1)], 1)/100,
                      CAGR5 = CAGR( .$Value[.$Year == max(.$Year)]/
                                       .$Value[.$Year == (max(.$Year)-5)], 5)/100,
                      CAGR10 =  CAGR( .$Value[.$Year == max(.$Year)]/
                                         .$Value[.$Year == (max(.$Year)-10)], 10)/100,
                      ABS5 = .$Value[.$Year == max(.$Year)] - .$Value[.$Year == (max(.$Year)- 5)],
                      ABS10 = .$Value[.$Year == max(.$Year)] - .$Value[.$Year == (max(.$Year)- 10)]
                  ) %>%
                  ungroup %>%
                  mutate( CAGR1 = as.numeric(CAGR1), 
                          CAGR5 = as.numeric(CAGR5), 
                          CAGR10 = as.numeric(CAGR10),
                          ABS5 = as.numeric(ABS5),
                          ABS10 = as.numeric(ABS10) ) %>%
                  #filter( Year == max(Year) ) %>%
                  left_join( rv_intelHS$tmp_dtf_market_line %>% rename(Name = Country) %>% filter( Year == max(Year) )  ) %>%
                  left_join( rv_intelHS$tmp_dtf_market_line_percent %>% dplyr::select( -Value ) %>% rename( Name = Country) %>% filter( Year == max(Year) )  ) %>%
                  dplyr::select( Name, Value, Share, CAGR1, CAGR5, CAGR10, ABS5, ABS10) %>%
                  mutate( Name = factor(Name, levels = as.character( rv_intelHS$tmp_top_country_selected ) ) ) %>%
                  arrange( Name )
         
         ## 4.12 Data for global situation from UN comtrade (ONLY for Export analysis) ----------------
         if(  input$rbtn_intel_by_hs == 'Exports' ){
            print("--------- Building Reactive values for global analysis -------------")
            
            ## old code ----------------------
            # rv_intelHS$Fail_uncomtrade_country <- 
            #    try(
            #       rv_intelHS$tmp_global_by_country_raw <-
            #          #get.Comtrade(r="all", p="0", rg = "1,2"  ## 1 means imports; 2 means exports (3 is re-exports excluded here)
            #          #             , ps = paste0(tmp_un_comtrade_max_year, "," ,tmp_un_comtrade_max_year-5)
            #          #             , cc = paste0(rv_intelHS$tmp_hs, collapse = ','), fmt = 'csv' )$data #%>%
            #          # dplyr::select( yr, cmdCode, rgDesc, rtTitle, rt3ISO, ptTitle, qtDesc,  TradeQuantity, TradeValue) %>%
            #          # mutate_all( as.character ) %>%
            #          # mutate( yr = as.numeric(yr),
            #          #         TradeQuantity = as.numeric( TradeQuantity ),
            #          #         TradeValue = as.numeric( TradeValue )
            #          # ) %>%
            #          # rename( Year = yr, `Commodity.Code` = cmdCode ,
            #          #         `Trade.Flow` = rgDesc,
            #          #         Reporter = rtTitle,
            #          #         `Reporter.ISO` = rt3ISO,
            #          #         Partner = ptTitle,
            #          #         `Qty.Unit` = qtDesc,
            #          #         `Alt.Qty.Unit` = TradeQuantity,
            #          #         `Trade.Value..US..` = TradeValue )
            #       
            #       m_ct_search( reporters = "All", partners = 'World', trade_direction = c("imports", "exports"), freq = "annual",
            #                    commod_codes = as.character(rv_intelHS$tmp_hs),
            #                    start_date = tmp_un_comtrade_max_year ,
            #                    end_date = tmp_un_comtrade_max_year) %>%
            #          bind_rows( 
            #             m_ct_search( reporters = "All", partners = 'World', trade_direction = c("imports", "exports"), freq = "annual",
            #                          commod_codes = as.character(rv_intelHS$tmp_hs),
            #                          start_date = tmp_un_comtrade_max_year - 5 ,
            #                          end_date = tmp_un_comtrade_max_year - 5)
            #             ) %>%
            #          #filter( year >= tmp_un_comtrade_max_year-5 &
            #          #           year <= tmp_un_comtrade_max_year ) %>%
            #          dplyr::select( year, commodity_code, trade_flow, reporter, reporter_iso, partner, qty_unit,  qty, trade_value_usd) %>%
            #          rename( Year = year, 
            #                  `Commodity.Code` = commodity_code ,
            #                  `Trade.Flow` = trade_flow,
            #                  Reporter = reporter,
            #                  `Reporter.ISO` =  reporter_iso,
            #                  Partner = partner,
            #                  `Qty.Unit` = qty_unit,
            #                  `Alt.Qty.Unit` = qty,
            #                  `Trade.Value..US..` = trade_value_usd )
            #       
            #    )
            # ## 
            # if( class(rv_intelHS$Fail_uncomtrade) == "try-error" )
            # print(rv_intelHS$Fail_uncomtrade)
            
            ## new download code --------------
            print("----------- Download Uncomtrade trade by country --------------")
            rv_intelHS$Fail_uncomtrade_country <- 
               try(
                  rv_intelHS$tmp_global_by_country_raw_list <- 
                     lapply( as.character(rv_intelHS$tmp_hs) ,
                             function(i){
                                m_ct_search( reporters = "All", partners = 'World', trade_direction = c("imports", "exports"), freq = "annual",
                                             commod_codes = i,
                                             start_date = tmp_un_comtrade_max_year,
                                             end_date = tmp_un_comtrade_max_year ) %>%
                                   bind_rows(  m_ct_search( reporters = "All", partners = 'World', trade_direction = c("imports", "exports"), freq = "annual",
                                                            commod_codes = i,
                                                            start_date = tmp_un_comtrade_max_year - 5,
                                                            end_date = tmp_un_comtrade_max_year - 5 ) 
                                   )
                             } 
                     )
               )
            
            ## try get EU data
            print("----------- Download Uncomtrade trade by EU --------------")
            rv_intelHS$Fail_uncomtrade_eu <- 
               try(
                  rv_intelHS$tmp_global_by_eu_raw_list <- 
                     lapply( as.character(rv_intelHS$tmp_hs) ,
                             function(i){
                                m_ct_search( reporters = "EU-28", partners = 'World', trade_direction = c("imports", "exports"), freq = "annual",
                                             commod_codes = i,
                                             start_date = tmp_un_comtrade_max_year,
                                             end_date = tmp_un_comtrade_max_year )  %>%
                                   bind_rows(  m_ct_search( reporters = "EU-28", partners = 'World', trade_direction = c("imports", "exports"), freq = "annual",
                                                            commod_codes = i,
                                                            start_date = tmp_un_comtrade_max_year - 5,
                                                            end_date = tmp_un_comtrade_max_year - 5 ) 
                                   )
                             } 
                     )
               )
            
            
            ## then consolidate the list into dataframe
            if( class(rv_intelHS$Fail_uncomtrade_country) != 'try-error' ){
               print("----------- Success: Download Uncomtrade trade by country --------------")
               ## get list to data frame
               try(
                  rv_intelHS$tmp_global_by_country_raw1 <- 
                     do.call( rbind, rv_intelHS$tmp_global_by_country_raw_list )
               )
               
               ## change names
               try(
                  rv_intelHS$tmp_global_by_country_raw <-
                     rv_intelHS$tmp_global_by_country_raw1 %>%
                     dplyr::select( year, commodity_code, trade_flow, reporter, reporter_iso, partner, qty_unit,  qty, trade_value_usd) %>%
                     rename( Year = year,
                             `Commodity.Code` = commodity_code ,
                             `Trade.Flow` = trade_flow,
                             Reporter = reporter,
                             `Reporter.ISO` =  reporter_iso,
                             Partner = partner,
                             `Qty.Unit` = qty_unit,
                             `Alt.Qty.Unit` = qty,
                             `Trade.Value..US..` = trade_value_usd )
               )
            }
            
            
            if( class(rv_intelHS$Fail_uncomtrade_eu) != 'try-error' ){
               print("----------- Success: Download Uncomtrade trade by EU --------------")
               ## get list to data frame
               try(
                  rv_intelHS$tmp_global_by_eu_raw1 <- 
                     do.call( rbind, rv_intelHS$tmp_global_by_eu_raw_list )
               )
               
               ## change names
               try(
                  rv_intelHS$tmp_global_by_eu_raw <-
                     rv_intelHS$tmp_global_by_eu_raw1 %>%
                     dplyr::select( year, commodity_code, trade_flow, reporter, reporter_iso, partner, qty_unit,  qty, trade_value_usd) %>%
                     rename( Year = year,
                             `Commodity.Code` = commodity_code ,
                             `Trade.Flow` = trade_flow,
                             Reporter = reporter,
                             `Reporter.ISO` =  reporter_iso,
                             Partner = partner,
                             `Qty.Unit` = qty_unit,
                             `Alt.Qty.Unit` = qty,
                             `Trade.Value..US..` = trade_value_usd )
               )
            }
            
            ## 
            if( class(rv_intelHS$Fail_uncomtrade_country) == "try-error" )
               print(rv_intelHS$Fail_uncomtrade_country)
            
            if( class(rv_intelHS$Fail_uncomtrade_eu) == "try-error" )
               print(rv_intelHS$Fail_uncomtrade_eu)
            
         }
         
         ## when both data downloaded successfully then do
         if( class(rv_intelHS$Fail_uncomtrade_country) != "try-error" & 
             class(rv_intelHS$Fail_uncomtrade_eu) != "try-error" & 
             !is.null(rv_intelHS$tmp_global_by_country_raw) & 
             input$rbtn_intel_by_hs == 'Exports'  ){
            ## 1. format the data -----
            print("-------------- 1. Format uncomtrade country data  ------------------")
            ## global import and export of A commodity (sum over all HS code under this commodity) by country
            rv_intelHS$tmp_global_by_country <- 
               rv_intelHS$tmp_global_by_country_raw %>%
               dplyr::select( Year,`Commodity.Code` , `Trade.Flow`, Reporter, `Reporter.ISO`, Partner, `Qty.Unit`, `Alt.Qty.Unit`, `Trade.Value..US..`) %>%
               #group_by(Year, `Trade.Flow`, Reporter, `Reporter.ISO`, Partner, `Qty.Unit`) %>%
               group_by(Year, `Trade.Flow`, Reporter, `Reporter.ISO`, Partner ) %>%
               summarise( `Alt.Qty.Unit` = sum(`Alt.Qty.Unit`, na.rm=T),
                       `Trade.Value..US..` = sum(`Trade.Value..US..`, na.rm=T)
                       ) %>%
               ungroup %>%
               mutate( Price = `Trade.Value..US..`/ `Alt.Qty.Unit`) 
            
            print("-------------- 1.0 Format uncomtrade eu data  ------------------")
            ## EU import and export of A commodity from world
            rv_intelHS$tmp_eu_trade_extra_raw <- 
               rv_intelHS$tmp_global_by_eu_raw %>%
               dplyr::select( Year,`Commodity.Code` , `Trade.Flow`, Reporter, `Reporter.ISO`, Partner, `Qty.Unit`, `Alt.Qty.Unit`, `Trade.Value..US..`) %>%
               #group_by(Year, `Trade.Flow`, Reporter, `Reporter.ISO`, Partner, `Qty.Unit`) %>%
               group_by(Year, `Trade.Flow`, Reporter, `Reporter.ISO`, Partner ) %>%
               summarise( `Alt.Qty.Unit` = sum(`Alt.Qty.Unit`, na.rm=T),
                          `Trade.Value..US..` = sum(`Trade.Value..US..`, na.rm=T)
               ) %>%
               ungroup 
            
            ## 5 yr change in value and prices % and abs 
            rv_intelHS$tmp_global_by_country_change <-    
               rv_intelHS$tmp_global_by_country %>%
               #group_by( `Trade.Flow`, Reporter, `Reporter.ISO`, Partner, `Qty.Unit`) %>%
               group_by( `Trade.Flow`, Reporter, `Reporter.ISO`, Partner) %>%
               do( Value_per_change = CAGR(.$`Trade.Value..US..`[.$Year==tmp_un_comtrade_max_year]/
                                              .$`Trade.Value..US..`[.$Year== (tmp_un_comtrade_max_year)-5], 5)/100 ,
                   Value_abs_change = .$`Trade.Value..US..`[.$Year==tmp_un_comtrade_max_year] - .$`Trade.Value..US..`[.$Year== (tmp_un_comtrade_max_year)-5] ,
                   Price_per_change = CAGR(.$Price[.$Year==tmp_un_comtrade_max_year]/
                                              .$Price[.$Year== (tmp_un_comtrade_max_year)-5], 5)/100 ) %>%
               ungroup %>%
               mutate( Value_per_change = as.numeric(Value_per_change ),
                       Value_abs_change = as.numeric(Value_abs_change ),
                       Price_per_change = as.numeric(Price_per_change )
                       )
            
            ## data frame for producing highchart tables 
            rv_intelHS$tmp_global_by_country_all <- 
               rv_intelHS$tmp_global_by_country %>%
               filter( Year == tmp_un_comtrade_max_year ) %>%
               left_join( rv_intelHS$tmp_global_by_country_change ) %>%
               group_by( Year, Trade.Flow  ) %>%
               mutate( Share = as.numeric(`Trade.Value..US..`)/ sum(as.numeric(`Trade.Value..US..`), na.rm=T ) ) %>%
               ungroup %>%
               arrange( `Trade.Flow`, -`Trade.Value..US..`) 
            
            
            ## 1.1 formate data -- get Eu28 intra and extra trade for later use in table ------
            print("-------------- 1.1 Format uncomtrade eu data  ------------------")
            rv_intelHS$tmp_eu_trade_all <- 
               rv_intelHS$tmp_global_by_country %>%
               filter( Reporter.ISO %in% concord_eu28$ISO3 ) %>%
               #group_by( Year , `Trade.Flow`, Partner, `Qty.Unit` ) %>%
               group_by( Year , `Trade.Flow`, Partner ) %>%
               summarise(  `Alt.Qty.Unit` = sum( as.numeric(`Alt.Qty.Unit`), na.rm=T ),
                           `Trade.Value..US..` = sum( as.numeric(`Trade.Value..US..`), na.rm=T ) ) %>%
               ungroup %>%
               mutate( Reporter = "EU-28", Reporter.ISO = 'EU2'   )
            
            ## derive EU trade intra
            print("-------------- 1.1.2 derive EU trade intra  ------------------")
            rv_intelHS$tmp_eu_trade_intra_raw <-
               rv_intelHS$tmp_eu_trade_all %>%
               left_join( rv_intelHS$tmp_eu_trade_extra_raw,
                          #by = c("Year", "Trade.Flow","Reporter", "Reporter.ISO", "Partner","Qty.Unit" )
                          by = c("Year", "Trade.Flow","Reporter", "Reporter.ISO", "Partner" )
               ) %>%
               mutate( `Alt.Qty.Unit` = Alt.Qty.Unit.x - Alt.Qty.Unit.y, 
                       `Trade.Value..US..` =  `Trade.Value..US...x` - `Trade.Value..US...y` ) %>%
               dplyr::select( -Alt.Qty.Unit.x, -Alt.Qty.Unit.y, 
                              -`Trade.Value..US...x`,  -`Trade.Value..US...y`) #%>%
            #mutate( Partner = "EU-28") 
            
            ### formate data
            print("-------------- 1.1.3 derive EU trade extra  ------------------")
            rv_intelHS$tmp_eu_trade_intra <- 
               rv_intelHS$tmp_eu_trade_intra_raw %>%
               mutate( Reporter = 'EU-28-Intra', Reporter.ISO = 'EU2-intra' )
            
            rv_intelHS$tmp_eu_trade_extra <- 
               rv_intelHS$tmp_eu_trade_extra_raw %>%
               mutate( Reporter = 'EU-28-Extra', Reporter.ISO = 'EU2-extra' )
            
            ## join EU intra and extra back
            print("-------------- 1.1.4 join EU intra and extra back  ------------------")
            rv_intelHS$tmp_global_by_country_and_eu <-
               rv_intelHS$tmp_global_by_country_raw %>%
               filter( !Reporter.ISO %in% concord_eu28$ISO3 ) %>%
               dplyr::select( Year,`Commodity.Code` , `Trade.Flow`, Reporter, `Reporter.ISO`, Partner, `Qty.Unit`, `Alt.Qty.Unit`, `Trade.Value..US..`) %>%
               #group_by(Year, `Trade.Flow`, Reporter, `Reporter.ISO`, Partner, `Qty.Unit`) %>%
               group_by(Year, `Trade.Flow`, Reporter, `Reporter.ISO`, Partner) %>%
               summarise( `Alt.Qty.Unit` = sum(`Alt.Qty.Unit`, na.rm=T),
                          `Trade.Value..US..` = sum(`Trade.Value..US..`, na.rm=T)
               ) %>%
               ungroup %>%
               bind_rows( rv_intelHS$tmp_eu_trade_intra ) %>%
               bind_rows( rv_intelHS$tmp_eu_trade_extra  ) %>%
               mutate( Price = `Trade.Value..US..`/ `Alt.Qty.Unit`)
            
            ## 5 yr change in value and prices % and abs 
            print("-------------- 1.1.5 5 yr change in value and prices % and abs   ------------------")
            rv_intelHS$tmp_global_by_country_and_eu_change <-    
               rv_intelHS$tmp_global_by_country_and_eu %>%
               #group_by( `Trade.Flow`, Reporter, `Reporter.ISO`, Partner, `Qty.Unit`) %>%
               group_by( `Trade.Flow`, Reporter, `Reporter.ISO`, Partner) %>%
               do( Value_per_change = CAGR(.$`Trade.Value..US..`[.$Year==tmp_un_comtrade_max_year]/
                                              .$`Trade.Value..US..`[.$Year== (tmp_un_comtrade_max_year)-5], 5)/100 ,
                   Value_abs_change = .$`Trade.Value..US..`[.$Year==tmp_un_comtrade_max_year] - .$`Trade.Value..US..`[.$Year== (tmp_un_comtrade_max_year)-5] ,
                   Price_per_change = CAGR(.$Price[.$Year==tmp_un_comtrade_max_year]/
                                              .$Price[.$Year== (tmp_un_comtrade_max_year)-5], 5)/100 ) %>%
               ungroup %>%
               mutate( Value_per_change = as.numeric(Value_per_change ),
                       Value_abs_change = as.numeric(Value_abs_change ),
                       Price_per_change = as.numeric(Price_per_change ) )
            
            ## data frame for producing highchart tables 
            print("-------------- 1.1.6 data frame for producing highchart tables  ------------------")
            rv_intelHS$tmp_global_by_country_and_eu_all <- 
               rv_intelHS$tmp_global_by_country_and_eu %>%
               filter( Year == tmp_un_comtrade_max_year ) %>%
               left_join( rv_intelHS$tmp_global_by_country_and_eu_change ) %>%
               group_by( Year, Trade.Flow  ) %>%
               mutate( Share = as.numeric(`Trade.Value..US..`)/ sum(as.numeric(`Trade.Value..US..`), na.rm=T ) ) %>%
               ungroup %>%
               arrange( `Trade.Flow`, -`Trade.Value..US..`)
            ## 2. calculate values for later use ------------   
            print("-------------- 2 Calculate values for facts boxes  ------------------")
            ## Global market size -- value now
            rv_intelHS$tmp_global_size_value_now <- 
               rv_intelHS$tmp_global_by_country %>%
               group_by(Year, `Trade.Flow`,  Partner ) %>%
               summarise(`Trade.Value..US..` = sum(as.numeric(`Trade.Value..US..`), na.rm=T) ) %>%
               ungroup %>%
               filter( Year == tmp_un_comtrade_max_year,
                       `Trade.Flow` == 'Import') %>%
               dplyr::select( `Trade.Value..US..` ) %>%
               as.numeric()
            
            ## Global market size -- value 5 years ago
            rv_intelHS$tmp_global_size_value_pre <- 
               rv_intelHS$tmp_global_by_country %>%
               group_by(Year, `Trade.Flow`,  Partner ) %>%
               summarise(`Trade.Value..US..` = sum(as.numeric(`Trade.Value..US..`), na.rm=T) ) %>%
               ungroup %>%
               filter( Year == tmp_un_comtrade_max_year-5,
                       `Trade.Flow` == 'Import') %>%
               dplyr::select( `Trade.Value..US..` ) %>%
               as.numeric()
            
            ## Global market size -- value change %
            rv_intelHS$tmp_global_size_value_change <-
               CAGR( rv_intelHS$tmp_global_size_value_now/
                        rv_intelHS$tmp_global_size_value_pre, 5)/100
            
            ## Global market size -- value change abs
            rv_intelHS$tmp_global_size_value_change_abs <-
               rv_intelHS$tmp_global_size_value_now - rv_intelHS$tmp_global_size_value_pre 
            
            ## Top 3 importers share
            rv_intelHS$tmp_top3_importers_share <-
               rv_intelHS$tmp_global_by_country_all %>%
               filter( `Trade.Flow` == 'Import' ) %>%
               arrange( -Share ) %>%
               slice(1:3) %>%
               group_by(Year) %>%
               summarise( Share = sum(Share, na.rm=T) ) %>%
               ungroup %>%
               dplyr::select(Share) %>%
               as.numeric
            
            ## Top 10 importers share
            rv_intelHS$tmp_top10_importers_share <-
               rv_intelHS$tmp_global_by_country_all %>%
               filter( `Trade.Flow` == 'Import' ) %>%
               arrange( -Share ) %>%
               slice(1:10) %>%
               group_by(Year) %>%
               summarise( Share = sum(Share, na.rm=T) ) %>%
               ungroup %>%
               dplyr::select(Share) %>%
               as.numeric
            
            ##  of top 20 markets -- number of high growth market
            rv_intelHS$tmp_number_high_growth_importers <-
               nrow(
                  rv_intelHS$tmp_global_by_country_all %>%
                     filter( `Trade.Flow` == 'Import' ) %>%
                     arrange( -Share ) %>%
                     slice(1:20) %>%
                     filter( Value_per_change >= 0.1 )
               )
            
            ## Top 3 exporters share
            rv_intelHS$tmp_top3_exporters_share <-
               rv_intelHS$tmp_global_by_country_all %>%
               filter( `Trade.Flow` == 'Export' ) %>%
               arrange( -Share ) %>%
               slice(1:3) %>%
               group_by(Year) %>%
               summarise( Share = sum(Share, na.rm=T) ) %>%
               ungroup %>%
               dplyr::select(Share) %>%
               as.numeric
            
            ## Top 10 exporters share
            rv_intelHS$tmp_top10_exporters_share <-
               rv_intelHS$tmp_global_by_country_all %>%
               filter( `Trade.Flow` == 'Export' ) %>%
               arrange( -Share ) %>%
               slice(1:10) %>%
               group_by(Year) %>%
               summarise( Share = sum(Share, na.rm=T) ) %>%
               ungroup %>%
               dplyr::select(Share) %>%
               as.numeric
            
            ## NZ's share
            rv_intelHS$tmp_nz_share <-
               rv_intelHS$tmp_global_by_country_all %>%
               filter( `Trade.Flow` == 'Export' ) %>%
               filter( Reporter == 'New Zealand' ) %>%
               dplyr::select(Share) %>%
               as.numeric
            
            ## 3. build data for importers and exporter maps -------------------
            print("-------------- 3 Format uncomtrade data for im/ex maps  ------------------")
            rv_intelHS$tmp_un_comtrade_importer_map <- 
               rv_intelHS$tmp_global_by_country_all %>%
               filter( `Trade.Flow` == "Import" ) %>%
               left_join( concord_uncomtrade_country, by = c('Reporter.ISO' = 'ISO3') ) %>%
               filter( !is.na(lat) ) %>%
               mutate( Value = `Trade.Value..US..`/10^6,
                       z= Value,
                       name = Reporter)
            
            rv_intelHS$tmp_un_comtrade_exporter_map <- 
               rv_intelHS$tmp_global_by_country_all %>%
               filter( `Trade.Flow` == "Export" ) %>%
               left_join( concord_uncomtrade_country, by = c('Reporter.ISO' = 'ISO3') ) %>%
               filter( !is.na(lat) ) %>%
               mutate( Value = `Trade.Value..US..`/10^6,
                       z= Value,
                       name = Reporter)

            ## 4. Build data for the summary table -----------------
            print("-------------- 4 Format uncomtrade data for summary table  ------------------")
            ## import tab
            rv_intelHS$tmp_un_comtrade_import_summary_tab <- 
               rv_intelHS$tmp_global_by_country_and_eu_all %>%
               filter( `Trade.Flow` == 'Import' ) %>%
               dplyr::select( Reporter, Share, 
                              `Trade.Value..US..` ,Value_per_change, Value_abs_change,  
                              Price, Price_per_change ) %>%
               mutate( `Trade.Value..US..` = `Trade.Value..US..`/10^6,
                       Value_abs_change = Value_abs_change/10^6)
            
            ## export tab
            rv_intelHS$tmp_un_comtrade_export_summary_tab <- 
               rv_intelHS$tmp_global_by_country_and_eu_all %>%
               filter( `Trade.Flow` == 'Export' ) %>%
               dplyr::select( Reporter, Share, 
                              `Trade.Value..US..` ,Value_per_change, Value_abs_change,  
                              Price, Price_per_change ) %>%
               mutate( `Trade.Value..US..` = `Trade.Value..US..`/10^6,
                       Value_abs_change = Value_abs_change/10^6)
         }
      })
      
      # ## some tests on reative values -----
      # output$testHS <- renderDataTable({
      #    req(rv_intelHS$selected_hs_table)
      #    rv_intelHS$tmp_global_by_country_all
      # })
      # 
      # insertUI( selector = '#ci_intel_by_hs_toadd',
      #           ui = dataTableOutput("testHS") )
      
      ### 4.4 and 4.5 generating value and percent line plots --------------
      output$CIExportImportValueLine <-
         renderHighchart({
            if( is.null(input$HSCodeTable_rows_selected) )
               return(NULL)
            highchart() %>%
               hc_exporting(enabled = TRUE, formAttributes = list(target = "_blank")) %>%
               hc_xAxis( categories = c( unique( rv_intelHS$tmp_dtf_key_line$Year) ) ) %>%
               hc_yAxis( title = list(text = "$ million, NZD"),
                         labels = list( format = "${value:,.0f} m")  ) %>%
               hc_plotOptions(line = list(
                  dataLabels = list(enabled = F),
                  #stacking = "normal",
                  enableMouseTracking = T #,
                  #series = list(events = list(legendItemClick = sharelegend)) ,
                  #showInLegend = T
               )
               )%>%
               hc_tooltip(table = TRUE,
                          sort = TRUE,
                          pointFormat = paste0( '<br> <span style="color:{point.color}">\u25CF</span>',
                                                " {series.name}: ${point.y} m"),
                          headerFormat = '<span style="font-size: 13px">Year {point.key}</span>'
               ) %>%
               hc_legend( layout = 'vertical', align = 'left', verticalAlign = 'top', floating = T, x = 100, y = -15 ) %>%
               hc_add_series( data =  rv_intelHS$tmp_dtf_key_line %>% filter( Type_gs == 'Goods' ) ,
                              mapping = hcaes(  x = Year, y = Value, group = HS_group ),
                              type = 'line',
                              marker = list(symbol = 'circle') #,
                              #visible = c(T,rep(F,length(tmp_top_g_ex)-1))
               )
         })
      

      ### plot
      output$CIExportImportPercentLine <-
         renderHighchart({
            if( is.null(input$HSCodeTable_rows_selected) )
               return(NULL)
            highchart() %>%
               hc_exporting(enabled = TRUE, formAttributes = list(target = "_blank")) %>%
               hc_xAxis( categories = c( unique( rv_intelHS$tmp_dtf_percent_line$Year) ) ) %>%
               hc_yAxis( title = list(text = "Percentage (%)"),
                         labels = list( format = "{value:,.1f} %")  ) %>%
               hc_plotOptions(line = list(
                  dataLabels = list(enabled = F),
                  #stacking = "normal",
                  enableMouseTracking = T)
               )%>%
               hc_tooltip(table = TRUE,
                          sort = TRUE,
                          pointFormat = paste0( '<br> <span style="color:{point.color}">\u25CF</span>',
                                                " {series.name}: {point.y:,.1f} %"),
                          headerFormat = '<span style="font-size: 13px">Year {point.key}</span>'
               ) %>%
               hc_legend( layout = 'vertical', align = 'left', verticalAlign = 'top', floating = T, x = 100, y = -15 ) %>%
               hc_add_series( data =  rv_intelHS$tmp_dtf_percent_line %>% filter( Type_gs == 'Goods' ) ,
                              mapping = hcaes(  x = Year, y = Value, group = HS_group ),
                              type = 'line',
                              marker = list(symbol = 'circle') #,
                              #visible = c(T,rep(F,length(tmp_top_g_ex)-1))
               )
         })

      ## !!!!! try UI  commodities selected -----------
      output$H1_title_value_percent <-
         renderText({
            if( is.null(input$HSCodeTable_rows_selected) )
               return(NULL)
            paste0( rv_intelHS$ie ," for selected commodities")
         })
      
      output$H1_title_value_percent_note <-
         renderText({
            if( is.null(input$HSCodeTable_rows_selected) )
               return(NULL)
            paste0(  "Click on the commodity names in the legend area to show their trends" )
         })
      
      output$H4_title_value <-
         renderText({
            if( is.null(input$HSCodeTable_rows_selected) )
               return(NULL)
            paste0( rv_intelHS$ie ," values")
         })
      
      output$H4_title_percent <-
         renderText({
            if( is.null(input$HSCodeTable_rows_selected) )
               return(NULL)
            paste0( "As a percent of total ", tolower(rv_intelHS$ie) )
         })
         
      insertUI(
         selector = '#ci_intel_by_hs_toadd',
         ui =   div( id = 'ci_intel_by_hs_line_value_percent',
                     fluidRow(
                        h1( HTML(paste(textOutput("H1_title_value_percent"))) ),
                        p( HTML(paste(textOutput("H1_title_value_percent_note")))  ),
                        column(6, div(id = "ci_intel_by_hs_value",
                                      h4( HTML(paste(textOutput("H4_title_value"))) ),
                                      highchartOutput('CIExportImportValueLine') ) ),
                        column(6, div(id = "ci_intel_by_hs_percent",
                                      h4( HTML(paste(textOutput("H4_title_percent"))) ),
                                      highchartOutput('CIExportImportPercentLine') ) ))
         )
      )
      ## end Try UI insert --------##
      
      
      ### 4.6 Generating commodity change table -------------------
      output$GrowthTabSelected <- 
         renderDataTable({
            if( is.null(input$HSCodeTable_rows_selected) )
               return(NULL)
            datatable( rv_intelHS$tmp_tab,
                       rownames = F,
                       filter = c("top"),
                       extensions = c('Buttons' ),
                       options = list(dom = 'Bfltp', #'Bltp',# 'Bt',
                                      buttons = c('copy', 'csv', 'excel', 'pdf', 'print') #, pageLength = -1, 
                                      ,scrollX = TRUE
                                      #,fixedColumns = list(leftColumns = 2) 
                                      ,autoWidth = T
                                      ,pageLength = 10
                                      ,lengthMenu = list(c(10,  -1), list('10', 'All')) ,
                                      searchHighlight = TRUE,
                                      search = list(regex = TRUE, caseInsensitive = FALSE )
                                      
                       ) ,
                       colnames = c("HS codes", "Classification","Value ($m)", paste0("Share of total ", tolower(rv_intelHS$ie) ), 'CAGR1', 'CAGR5', 'CAGR10', 'ABS5', 'ABS10')
             ) %>%
               formatStyle(
                  c('CAGR1', 'CAGR5', 'CAGR10'),
                  background = styleColorBar( c(0, max(c(rv_intelHS$tmp_tab$CAGR1,rv_intelHS$tmp_tab$CAGR5, rv_intelHS$tmp_tab$CAGR10))*2, na.rm=T) , 'lightblue'),
                  backgroundSize = '100% 90%',
                  backgroundRepeat = 'no-repeat',
                  backgroundPosition = 'center'
               ) %>%
               formatStyle(c('CAGR1', 'CAGR5', 'CAGR10', 'ABS5', 'ABS10'),
                           color = JS("value < 0 ? 'darkred' : value > 0 ? 'darkgreen' : 'black'")) %>%
               formatPercentage( c('Share','CAGR1', 'CAGR5', 'CAGR10'),digit = 1 ) %>%
               formatStyle( columns = c('Name','Value', 'Share', 'CAGR1', 'CAGR5', 'CAGR10', 'ABS5', 'ABS10'), `font-size`= '115%' ) %>%
               formatCurrency( columns = c('Value', 'ABS5', 'ABS10'), mark = ' ', digits = 1)
         })
      
      ## !!!!! try UI insert: Commodity change table ----------- 
      output$H1_title_growth_tab <- 
         renderText({
            if( is.null(input$HSCodeTable_rows_selected) )
               return(NULL)
            paste0( "Short, medium, and long term growth for the selected commodities" )
         })
      
      output$H1_title_growth_tab_note <- 
         renderText({
            if( is.null(input$HSCodeTable_rows_selected) )
               return(NULL)
            paste0( "Compound annual growth rate (CAGR) for the past 1, 5, and 10 years. Absolute value change (ABS) for the past 5 and 10 years." )
         })
      
      ## insert ui here
      insertUI(
         selector = '#ci_intel_by_hs_toadd',
         ui =   div( id = 'ci_intel_by_hs_toadd_growth_tab',
                     fluidRow( h1( HTML(paste(textOutput("H1_title_growth_tab"))) ),
                               p( HTML(paste(textOutput("H1_title_growth_tab_note"))) ) ,
                               dataTableOutput('GrowthTabSelected')
                     )
         )
      )
      ## end Try UI insert --------##
      
      
      ## 4.7 Build exports/imports by country output groups -------------------
      ## create a selector for each selected commodity 
      output$Commodity_Selector_note <-
         renderText({
            if( is.null(input$HSCodeTable_rows_selected) )
               return(NULL)
            paste0( "Please select or search a commodity for its market analysis" )
         })
      
      output$H1_title_Commodity_Selector <-
         renderText({
            if( is.null(input$HSCodeTable_rows_selected) )
               return(NULL)
            paste0( gsub("s", "", rv_intelHS$ie) ," markets analysis for selected commodity" )
         })
      
      output$CISelectorByMarkets <- renderUI({
         if( is.null(input$HSCodeTable_rows_selected) )
            return(NULL)
         selectizeInput("select_comodity_for_market_analysis",
                        # tags$p( HTML(paste(textOutput("Commodity_Selector_note "))) ), 
                        # choices = rv_intelHS$tmp_tab$Name[input$GrowthTabSelected_rows_all], # tmp_top_ex, 
                        # selected = NULL, #tmp_top_ex[1], 
                        # width = "500px",
                        # multiple = F
                        tags$p("Please select or search a commodity for its market analysis"), 
                        choices =  c('Please select a commodity' = "" , 
                                     as.character(rv_intelHS$tmp_tab$Name)
                        ), #input$select_comodity_ex,
                        selected = "",  width = "500px",
                        multiple = F)
      })
      
      ### selcted commodity and service outputs
      output$Selected <- 
         renderText({
            if( is.null(input$HSCodeTable_rows_selected) )
               return(NULL)
            rv_intelHS$tmp_selected
         })
      
      ## !!!!! try UI insert for the selectors  ---------------
      insertUI(
         selector = '#ci_intel_by_hs_toadd',
         ui =   div( id = 'ci_intel_by_hs_toadd_markets_selector',
                     fluidRow(h1( HTML(paste0(textOutput("H1_title_Commodity_Selector"))) ),
                              uiOutput("CISelectorByMarkets") ),
                     fluidRow( shiny::span(h1( HTML(paste0(textOutput("Selected"))), align = "center" ), style = "color:darkblue" ) )
         )
      )
      ## end Try UI insert -----------##
      
      ## --- show loading message when select a commodity -------
      observe(
         try(
            if( !is.null(input$select_comodity_for_market_analysis) &&
                input$select_comodity_for_market_analysis!= "" ){
               shinyjs::show( id = "ci_intel_hs_loading_message_intl" )
            }
         )
      )

      ## 4.8 Plot Value Line and Percentage line for selected commodities ----------------
      output$CISelectedValueLine <- 
         renderHighchart({
            if( is.null(input$HSCodeTable_rows_selected) | input$select_comodity_for_market_analysis == "" )
               return(NULL)
            highchart() %>%
               hc_exporting(enabled = TRUE, formAttributes = list(target = "_blank")) %>%
               hc_xAxis( categories = c( unique( rv_intelHS$tmp_dtf_line_selected$Year) ) ) %>%
               hc_yAxis( title = list(text = "$ million, NZD"),
                         labels = list( format = "${value:,.0f} m")  ) %>%
               hc_plotOptions(line = list(
                  dataLabels = list(enabled = F),
                  #stacking = "normal",
                  enableMouseTracking = T #,
                  #series = list(events = list(legendItemClick = sharelegend)) ,
                  #showInLegend = T
               )
               )%>%
               hc_tooltip(table = TRUE,
                          sort = TRUE,
                          pointFormat = paste0( '<br> <span style="color:{point.color}">\u25CF</span>',
                                                " {series.name}: ${point.y} m"),
                          headerFormat = '<span style="font-size: 13px">Year {point.key}</span>'
               ) %>%
               hc_legend( layout = 'vertical', align = 'left', verticalAlign = 'top', floating = T, x = 100, y = -15 ) %>%
               hc_add_series( data =  rv_intelHS$tmp_dtf_line_selected %>% filter( Type_gs == 'Goods' ) ,
                              mapping = hcaes(  x = Year, y = Value, group = HS_group ),
                              type = 'line',
                              marker = list(symbol = 'circle') #,
                              #visible = c(T,rep(F,length(tmp_top_g_ex)-1))
               )
        })
      
      # ### plot
      output$CISelectedPercentLine <-
         renderHighchart({
            if( is.null(input$HSCodeTable_rows_selected) | input$select_comodity_for_market_analysis == "" )
               return(NULL)
            highchart() %>%
               hc_exporting(enabled = TRUE, formAttributes = list(target = "_blank")) %>%
               hc_xAxis( categories = c( unique( rv_intelHS$tmp_dtf_percent_selected_line$Year) ) ) %>%
               hc_yAxis( title = list(text = "Percentage (%)"),
                         labels = list( format = "{value:,.1f} %")  ) %>%
               hc_plotOptions(line = list(
                  dataLabels = list(enabled = F),
                  #stacking = "normal",
                  enableMouseTracking = T)
               )%>%
               hc_tooltip(table = TRUE,
                          sort = TRUE,
                          pointFormat = paste0( '<br> <span style="color:{point.color}">\u25CF</span>',
                                                " {series.name}: {point.y:,.1f} %"),
                          headerFormat = '<span style="font-size: 13px">Year {point.key}</span>'
               ) %>%
               hc_legend( layout = 'vertical', align = 'left', verticalAlign = 'top', floating = T, x = 100, y = -15 ) %>%
               hc_add_series( data =  rv_intelHS$tmp_dtf_percent_selected_line %>% filter( Type_gs == 'Goods' ) ,
                              mapping = hcaes(  x = Year, y = Value, group = HS_group ),
                              type = 'line',
                              marker = list(symbol = 'circle') #,
                              #visible = c(T,rep(F,length(tmp_top_g_ex)-1))
               )
         })
      
      ## !!!!! try UI insert: selected value and percnet line -----------
      output$H2_title_selected_value_percent_line <- 
         renderText({
            if( is.null(input$HSCodeTable_rows_selected) | input$select_comodity_for_market_analysis == "" )
               return(NULL)
            paste0( rv_intelHS$ie , " trends" )
         })
      
      output$H2_title_selected_value_percent_line_note <- 
         renderText({
            if( is.null(input$HSCodeTable_rows_selected) | input$select_comodity_for_market_analysis == "" )
               return(NULL)
            paste0( "Click on the commodity names in the legend area to show their trends" )
         })
      
      output$H4_title_selected_value_line <- 
         renderText({
            if( is.null(input$HSCodeTable_rows_selected) | input$select_comodity_for_market_analysis == "" )
               return(NULL)
            paste0( rv_intelHS$ie , " values" )
         })
      
      output$H4_title_selected_percent_line <- 
         renderText({
            if( is.null(input$HSCodeTable_rows_selected) | input$select_comodity_for_market_analysis == "" )
               return(NULL)
            paste0( "As a percent of total ", tolower(rv_intelHS$ie)  )
         })
      
      ## UI here 
      insertUI(
         selector = '#ci_intel_by_hs_toadd_intl',
         ui =   div( id = 'ci_intel_by_hs_toadd_selected_line_value_percent',
                     fluidRow( h2( HTML(paste0(textOutput("H2_title_selected_value_percent_line"))) ),
                               p( HTML(paste0(textOutput("H2_title_selected_value_percent_line_note"))) ),
                               column(6, div(id = "ci_intel_by_hs_value_selected", h4( HTML(paste0(textOutput("H4_title_selected_value_line"))) ), highchartOutput('CISelectedValueLine') ) ),
                               column(6, div(id = "ci_intel_by_hs_percent_selected", h4( HTML(paste0(textOutput("H4_title_selected_percent_line"))) ), highchartOutput('CISelectedPercentLine') ) )
                               )
         )
      )
      ## end Try UI insert --------##
      
      
      
      ## 4.9 Plot  for build highchart map  ---------------------------
      output$MapMarket <- 
         renderHighchart({
            if( is.null(input$HSCodeTable_rows_selected) | input$select_comodity_for_market_analysis == "")
               return(NULL)
            hcmap( data = rv_intelHS$tmp_dtf_market_map ,
                   value = 'Value',
                   joinBy = c('iso-a2','ISO2'), 
                   name= paste0( rv_intelHS$ie, " value"),
                   borderWidth = 1,
                   borderColor = "#fafafa",
                   nullColor = "lightgrey",
                   tooltip = list( table = TRUE,
                                   sort = TRUE,
                                   headerFormat = '<span style="font-size:13px">{series.name}</span><br/>',
                                   pointFormat = '{point.name}: <b>${point.value:,.1f} m</b>' )
            ) %>%
               hc_add_series(data =  rv_intelHS$tmp_dtf_market_map,
                             type = "mapbubble",
                             color  = hex_to_rgba("#f1c40f", 0.9),
                             minSize = 0,
                             name= paste0( rv_intelHS$ie," value"),
                             maxSize = 30,
                             tooltip = list(table = TRUE,
                                            sort = TRUE,
                                            headerFormat = '<span style="font-size:13px">{series.name}</span><br/>',
                                            pointFormat = '{point.name}: <b>${point.z:,.1f} m</b>')
               ) %>%
               hc_exporting(enabled = TRUE, formAttributes = list(target = "_blank")) %>%
               hc_legend( enabled=FALSE ) %>% 
               hc_mapNavigation(enabled = TRUE) 
         })
      
      ## !!!!! try UI insert: map of importer / exporters ----------- 
      output$H2_title_map_selected <-
         renderText({
            if( is.null(input$HSCodeTable_rows_selected) | input$select_comodity_for_market_analysis == "" )
               return(NULL)
            paste0("Map of ", gsub("s","", tolower(rv_intelHS$ie)) ," values")
         })
      
      output$H2_title_map_selected_note <-
         renderText({
            if( is.null(input$HSCodeTable_rows_selected) | input$select_comodity_for_market_analysis == "" )
               return(NULL)
            paste0( "The size of bubble area and color both represent the value of ", tolower(rv_intelHS$ie) ) 
         })
      
      ## Insert ui here
      insertUI(
         selector = '#ci_intel_by_hs_toadd_intl',
         ui =   div( id = 'ci_intel_by_hs_toadd_markets_map',
                     fluidRow(h2( HTML(paste0(textOutput("H2_title_map_selected"))) ) ,
                              p( HTML(paste0(textOutput("H2_title_map_selected_note"))) ),
                              highchartOutput('MapMarket') )
         )
      )
      ## end Try UI insert --------##
      
      
      
      ## 4.10 Plot fro Top markets for selected commodity line chart ----------------
      output$SelectedMarketLine <- renderHighchart({
         if( is.null(input$HSCodeTable_rows_selected)| input$select_comodity_for_market_analysis == "" )
            return(NULL)
         highchart() %>%
            hc_exporting(enabled = TRUE, formAttributes = list(target = "_blank")) %>%
            hc_add_series( data =  rv_intelHS$tmp_dtf_market_line %>%
                              filter( Country %in% as.character( rv_intelHS$tmp_top10_country_selected  ) ),
                           mapping = hcaes(  x = Year, y = Value, group = Country),
                           type = 'line',
                           marker = list(symbol = 'circle'), 
                           visible = c( rep(T,5), rep(F,length( as.character( rv_intelHS$tmp_top10_country_selected  ) )-5) )
            ) %>%
            hc_xAxis( categories = c( unique( rv_intelHS$tmp_dtf_market_line$Year) ) ) %>%
            hc_yAxis( title = list(text = "$ million, NZD"),
                      labels = list( format = "${value:,.0f} m")  ) %>%
            hc_plotOptions(line = list(
               dataLabels = list(enabled = F),
               #stacking = "normal",
               enableMouseTracking = T)
            )%>%
            hc_tooltip(table = TRUE,
                       sort = TRUE,
                       pointFormat = paste0( '<br> <span style="color:{point.color}">\u25CF</span>',
                                             " {series.name}: ${point.y:,.0f} m"),
                       headerFormat = '<span style="font-size: 13px">Year {point.key}</span>'
            ) %>%
            hc_legend( layout = 'vertical', align = 'left', verticalAlign = 'top', floating = T, x = 100, y = -15 )
      })
      
      output$SelectedMarketLinePercent <-
         renderHighchart({
            if( is.null(input$HSCodeTable_rows_selected)| input$select_comodity_for_market_analysis == "" )
               return(NULL)
            highchart() %>%
               hc_exporting(enabled = TRUE, formAttributes = list(target = "_blank")) %>%
               hc_add_series( data =  rv_intelHS$tmp_dtf_market_line_percent %>%
                                 filter( Country %in% as.character( rv_intelHS$tmp_top10_country_selected  ) ),
                              mapping = hcaes(  x = Year, y = Value, group = Country),
                              type = 'line',
                              marker = list(symbol = 'circle'), 
                              visible = c( rep(T,5), rep(F,length( as.character( rv_intelHS$tmp_top10_country_selected ) )-5) )
               ) %>%
               hc_xAxis( categories = c( unique( rv_intelHS$tmp_dtf_market_line_percent$Year) ) ) %>%
               hc_yAxis( title = list(text = "Percentage (%)"),
                         labels = list( format = "{value:,.1f} %")  ) %>%
               hc_plotOptions(line = list(
                  dataLabels = list(enabled = F),
                  #stacking = "normal",
                  enableMouseTracking = T)
               )%>%
               hc_tooltip(table = TRUE,
                          sort = TRUE,
                          pointFormat = paste0( '<br> <span style="color:{point.color}">\u25CF</span>',
                                                " {series.name}: {point.y:,.1f} %"),
                          headerFormat = '<span style="font-size: 13px">Year {point.key}</span>'
               ) %>%
               hc_legend( layout = 'vertical', align = 'left', verticalAlign = 'top', floating = T, x = 100, y = -15 )
         })
      
      ## !!!!! try UI insert: top markets value and percent line  ----------- 
      output$H2_title_top_market <-
         renderText({
            if( is.null(input$HSCodeTable_rows_selected) | input$select_comodity_for_market_analysis == "")
               return(NULL)
            paste0( "Top 10 ", gsub("s","",tolower(rv_intelHS$ie)) ," markets trends"  )
         })
      
      output$H2_title_top_market_note <-
         renderText({
            if( is.null(input$HSCodeTable_rows_selected) | input$select_comodity_for_market_analysis == "")
               return(NULL)
            paste0( "Click on the country names in the legend area to show their trends"  )
         })
      
      output$H4_title_top_market_value <-
         renderText({
            if( is.null(input$HSCodeTable_rows_selected) | input$select_comodity_for_market_analysis == "")
               return(NULL)
            paste0( rv_intelHS$ie, " values"  )
         })
      
      output$H4_title_top_market_percent <-
         renderText({
            if( is.null(input$HSCodeTable_rows_selected) | input$select_comodity_for_market_analysis == "")
               return(NULL)
            paste0( "As a percent of total ", tolower(rv_intelHS$ie) ," of the selected" )
         })
      
      ## insert ui here    
      insertUI(
         selector = '#ci_intel_by_hs_toadd_intl',
         ui =   div( id = 'ci_intel_by_hs_toadd_markets_top',
                     fluidRow( h2( HTML(paste0(textOutput("H2_title_top_market"))) ),
                               p( HTML(paste0(textOutput("H2_title_top_market_note"))) ),
                               column(6, 
                                      h4( HTML(paste0(textOutput("H4_title_top_market_value"))) ),
                                      highchartOutput("SelectedMarketLine") 
                               ),
                               column(6,
                                      h4( HTML(paste0(textOutput("H4_title_top_market_percent"))) ),
                                      highchartOutput("SelectedMarketLinePercent")
                               )
                     )
         )
      )
      ## end Try UI insert --------##
      
      
      
      ## 4.11 Table for Growth prospective tab ----------------------
      output$SelectedMarketGrowthTab <- renderDataTable({
         if( is.null(input$HSCodeTable_rows_selected) | input$select_comodity_for_market_analysis == "")
            return(NULL)
         datatable( rv_intelHS$tmp_tab_growth,
                    rownames = F,
                    extensions = 'Buttons',
                    options = list(dom = 'Bltp',# 'Bt', 
                                   buttons = c('copy', 'csv', 'excel', 'pdf', 'print') #, pageLength = -1
                                   ,scrollX = TRUE
                                   ,pageLength = 10
                                   ,lengthMenu = list(c(10,  -1), list('10', 'All')) 
                                   #,fixedColumns = list(leftColumns = 2) 
                                   #,autoWidth = T
                    ) ,
                    colnames=c("Markets", "Value ($m)", "Share of world marekt", 'CAGR1', 'CAGR5', 'CAGR10', 'ABS5', 'ABS10')
         ) %>%
            formatStyle(
               c('CAGR1', 'CAGR5', 'CAGR10'),
               background = styleColorBar( c(0, max(c( rv_intelHS$tmp_tab_growth$CAGR1,
                                                       rv_intelHS$tmp_tab_growth$CAGR5,
                                                       rv_intelHS$tmp_tab_growth$CAGR10))*2, na.rm=T) , 'lightblue'),
               backgroundSize = '100% 90%',
               backgroundRepeat = 'no-repeat',
               backgroundPosition = 'center'
            ) %>%
            formatStyle(c('CAGR1', 'CAGR5', 'CAGR10', 'ABS5', 'ABS10'),
                        color = JS("value < 0 ? 'darkred' : value > 0 ? 'darkgreen' : 'black'")) %>%
            formatPercentage( c('Share', 'CAGR1', 'CAGR5', 'CAGR10'),digit = 1 ) %>%
            formatStyle( columns = c('Name', "Value", "Share" ,'CAGR1', 'CAGR5', 'CAGR10'), `font-size`= '115%' ) %>%
            formatCurrency( columns = c("Value", 'ABS5', 'ABS10'), mark = ' ', digits = 1)
      })
      
      
      ## !!!!! try UI insert: growth tab by markets ----------- 
      output$H2_title_growth_market <-
         renderText({
            if( is.null(input$HSCodeTable_rows_selected) | input$select_comodity_for_market_analysis == "")
               return(NULL)
            paste0( "Top ", gsub("s","",rv_intelHS$ie) ," markets growth prospective" )
         })
      
      output$H2_title_growth_market_note <-
         renderText({
            if( is.null(input$HSCodeTable_rows_selected) | input$select_comodity_for_market_analysis == "")
               return(NULL)
            paste0( "Compound annual growth rate (CAGR) for the past 1, 5, and 10 years. Absolute value change (ABS) for the past 5 and 10 years." )
         })
         
      insertUI(
         selector = '#ci_intel_by_hs_toadd_intl',
         ui =   div( id = 'ci_intel_by_hs_toadd_markets_growth',
                     fluidRow( h2( HTML(paste0(textOutput("H2_title_growth_market"))) ),
                               p( HTML(paste0(textOutput("H2_title_growth_market_note"))) ),
                               dataTableOutput("SelectedMarketGrowthTab")
                     )
         )
      )
      ## end Try UI insert --------##
      
      ## 4.12 UN com Trade data analysis starts here Key facts table ----------
      ## world market size
      print("--------- Building facts value boxes -------------")
      output$Un_comtrade_world_market_size <-
         renderInfoBox({
            if( is.null(input$HSCodeTable_rows_selected) | is.null(rv_intelHS$tmp_global_by_country_raw) | input$rbtn_intel_by_hs == 'Imports' | input$select_comodity_for_market_analysis == "")
               return(NULL)
            infoBox( "World market size",
                     paste0("$", 
                            format(round(rv_intelHS$tmp_global_size_value_now/10^6), big.mark = ","),
                            " m"
                            )
                     , icon = icon('globe', lib = "glyphicon")
               
            )
         })
      
      ## 5 year growth
      output$Un_comtrade_world_market_change <-
         renderInfoBox({
            if( is.null(input$HSCodeTable_rows_selected) | is.null(rv_intelHS$tmp_global_by_country_raw) | input$rbtn_intel_by_hs == 'Imports' | input$select_comodity_for_market_analysis == "")
               return(NULL)
            
            if( is.null(rv_intelHS$tmp_global_size_value_change) )
               infoBox( "CAGR (5 years)",
                        HTML(paste0( "Not available" )), 
                        icon = icon('minus'))
               
            if(rv_intelHS$tmp_global_size_value_change>0 ){
               infoBox( "CAGR (5 years)",
                        HTML(paste0( "<font color='green'> +",
                           round(abs(rv_intelHS$tmp_global_size_value_change)*100,1),
                           "% </font>"
                        )), 
                        icon = icon('arrow-up'), color = 'green')
            }else{
               infoBox( "CAGR (5 years)",
                        HTML(paste0( "<font color='red'> -",
                           round(abs(rv_intelHS$tmp_global_size_value_change)*100,1),
                           "% </font>"
                        )), 
                        icon = icon('arrow-down'), color = 'red')
            }
            
         })
      
      ## 5 yr abs change
      output$Un_comtrade_world_market_change_abs <-
         renderInfoBox({
            if( is.null(input$HSCodeTable_rows_selected) | is.null(rv_intelHS$tmp_global_by_country_raw) | input$rbtn_intel_by_hs == 'Imports'| input$select_comodity_for_market_analysis == "" )
               return(NULL)
            
            if( is.null(rv_intelHS$tmp_global_size_value_change_abs) )
               infoBox( "ABS (5 years)",
                        HTML(paste0( "Not available" )), 
                        icon = icon('minus'))
            
            if(rv_intelHS$tmp_global_size_value_change_abs>0 ){
               infoBox( "ABS (5 years)",
                        HTML(paste0("<font color='green'> +$", 
                               format(round(rv_intelHS$tmp_global_size_value_change_abs/10^6), big.mark = ","),
                               " m </font>"
                        )),
                        icon = icon('arrow-up'), color = 'green')
            }else{
               infoBox( "ABS (5 years)",
                        HTML(paste0("<font color='red'> -$", 
                               format(round(abs(rv_intelHS$tmp_global_size_value_change_abs)/10^6), big.mark = ","),
                               " m </font>"
                        )),
                        icon = icon('arrow-down'), color = 'red')
            }
         })
      
      ## top 3 importer share
      output$Un_comtrade_top3_importers_share <-
         renderInfoBox({
            if( is.null(input$HSCodeTable_rows_selected) | is.null(rv_intelHS$tmp_global_by_country_raw) | input$rbtn_intel_by_hs == 'Imports'| input$select_comodity_for_market_analysis == "" )
               return(NULL)
            infoBox( HTML("Top 3 importers <br> share"),
                     paste0( 
                        round(abs(rv_intelHS$tmp_top3_importers_share)*100,1),
                            "%"
                     ),
                     icon = icon('import', lib = "glyphicon"))
         })
      
      ## top 10 importer share
      output$Un_comtrade_top10_importers_share <-
         renderInfoBox({
            if( is.null(input$HSCodeTable_rows_selected) | is.null(rv_intelHS$tmp_global_by_country_raw) | input$rbtn_intel_by_hs == 'Imports' | input$select_comodity_for_market_analysis == "")
               return(NULL)
            infoBox( HTML("Top 10 importers <br> share"),
                     paste0( 
                        round(abs(rv_intelHS$tmp_top10_importers_share)*100,1),
                        "%"
                     ),
                     icon = icon('import', lib = "glyphicon"))
         })
      
      ##  of top 20 markets -- number of high growth market
      output$Un_comtrade_high_growth_importers <-
         renderInfoBox({
            if( is.null(input$HSCodeTable_rows_selected) | is.null(rv_intelHS$tmp_global_by_country_raw) | input$rbtn_intel_by_hs == 'Imports'| input$select_comodity_for_market_analysis == "" )
               return(NULL)
            infoBox( HTML("Top 20 importers <br> with CAGR>10%"),
                     paste0( rv_intelHS$tmp_number_high_growth_importers) ,
                     icon = icon('import', lib = "glyphicon"))
         })
      
      
      ## top 3 exporter share
      output$Un_comtrade_top3_exporters_share <-
         renderInfoBox({
            if( is.null(input$HSCodeTable_rows_selected) | is.null(rv_intelHS$tmp_global_by_country_raw) | input$rbtn_intel_by_hs == 'Imports' | input$select_comodity_for_market_analysis == "")
               return(NULL)
            infoBox( HTML("Top 3 exporters <br> share"),
                     paste0( 
                        round(abs(rv_intelHS$tmp_top3_exporters_share)*100,1),
                        "%"
                     ),
                     icon = icon('export', lib = "glyphicon"))
         })
      
      ## top 10 exporter share
      output$Un_comtrade_top10_exporters_share <-
         renderInfoBox({
            if( is.null(input$HSCodeTable_rows_selected) | is.null(rv_intelHS$tmp_global_by_country_raw) | input$rbtn_intel_by_hs == 'Imports' | input$select_comodity_for_market_analysis == "")
               return(NULL)
            infoBox( HTML("Top 10 exporters <br> share"),
                     paste0( 
                        round(abs(rv_intelHS$tmp_top10_exporters_share)*100,1),
                        "%"
                     ),
                     icon = icon('export', lib = "glyphicon"))
         })
      
      ## new zealand share
      output$Un_comtrade_nz_share <-
         renderInfoBox({
            if( is.null(input$HSCodeTable_rows_selected) | is.null(rv_intelHS$tmp_global_by_country_raw) | input$rbtn_intel_by_hs == 'Imports'| input$select_comodity_for_market_analysis == "" )
               return(NULL)
            if( rv_intelHS$tmp_nz_share < 0.001 ){
               infoBox( HTML("New Zealand <br> share"),
                        paste0( "Less than 0.1%" ),
                        icon = icon('export', lib = "glyphicon"))
            }else{
               infoBox( HTML("New Zealand <br> share"),
                        paste0( 
                           round(abs(rv_intelHS$tmp_nz_share)*100,1),
                           "%"
                        ),
                        icon = icon('export', lib = "glyphicon"))
            }
            
         })
      
      
      ##!!!!! try UI insert: value box for global market facts ----------- 
      output$H1_title_global_facts <-
         renderText({
            if( is.null(input$HSCodeTable_rows_selected) | is.null(rv_intelHS$tmp_global_by_country_raw) | input$rbtn_intel_by_hs == 'Imports' | input$select_comodity_for_market_analysis == "" )
               return(NULL)
            paste0( "Global market analysis (", tmp_un_comtrade_max_year ,")" )
         })
      
      output$H1_title_global_facts_note <-
         renderText({
            if( is.null(input$HSCodeTable_rows_selected) | is.null(rv_intelHS$tmp_global_by_country_raw) | input$rbtn_intel_by_hs == 'Imports' | input$select_comodity_for_market_analysis == "" )
               return(NULL)
            paste0( "All values undner the global market analysis are reported in current US dollar" )
         })
      
      output$H3_title_global_facts_summary <-
         renderText({
            if( is.null(input$HSCodeTable_rows_selected) | is.null(rv_intelHS$tmp_global_by_country_raw) | input$rbtn_intel_by_hs == 'Imports' | input$select_comodity_for_market_analysis == "" )
               return(NULL)
            paste0( "Key facts and summary" )
         })
      
      ### insert global market key facts and summary value boxe
      insertUI(
         selector = '#ci_intel_by_hs_toadd_intl',
         ui =   div( id = 'ci_intel_by_hs_toadd_global_facts',
                     fluidRow( 
                        h1( HTML(paste0(textOutput("H1_title_global_facts"))) ),
                        p( HTML(paste0(textOutput("H1_title_global_facts_note"))) ),
                        h3( HTML(paste0(textOutput("H3_title_global_facts_summary"))) ),
                        infoBoxOutput("Un_comtrade_world_market_size") ,
                        infoBoxOutput("Un_comtrade_world_market_change" ) ,
                        infoBoxOutput("Un_comtrade_world_market_change_abs" ) 
                     ),
                     fluidRow(
                        infoBoxOutput("Un_comtrade_top3_importers_share" ) ,
                        infoBoxOutput("Un_comtrade_top10_importers_share" ) ,
                        infoBoxOutput("Un_comtrade_high_growth_importers" ) 
                     ),
                     fluidRow(
                        infoBoxOutput("Un_comtrade_top3_exporters_share" ) ,
                        infoBoxOutput("Un_comtrade_top10_exporters_share" ) ,
                        infoBoxOutput("Un_comtrade_nz_share" ) 
                     )
         )
      )
      
      
      ## 4.13 Quick glance at both importers and exporters map --------
      print("--------- Building importer and exporter map -------------")
      output$UN_comtrade_importer_Map <- 
         renderHighchart({
            if( is.null(input$HSCodeTable_rows_selected) | is.null(rv_intelHS$tmp_global_by_country_raw) | input$rbtn_intel_by_hs == 'Imports' | input$select_comodity_for_market_analysis == "")
               return(NULL)
            hcmap( data = rv_intelHS$tmp_un_comtrade_importer_map ,
                   value = 'Value',
                   joinBy = c('iso-a2','ISO2'), 
                   name= paste0( "Import value"),
                   borderWidth = 1,
                   borderColor = "#fafafa",
                   nullColor = "lightgrey",
                   tooltip = list( table = TRUE,
                                   sort = TRUE,
                                   headerFormat = '<span style="font-size:13px">{series.name}</span><br/>',
                                   pointFormat = '{point.name}: <b>${point.value:,.1f} m</b>' )
            ) %>%
               hc_add_series(data =  rv_intelHS$tmp_un_comtrade_importer_map ,
                             type = "mapbubble",
                             color  = hex_to_rgba("#DF1995", 0.9),
                             minSize = 0,
                             name= paste0( "Import value"),
                             maxSize = 30,
                             tooltip = list(table = TRUE,
                                            sort = TRUE,
                                            headerFormat = '<span style="font-size:13px">{series.name}</span><br/>',
                                            pointFormat = '{point.name}: <b>${point.z:,.1f} m</b>')
               ) %>%
               hc_exporting(enabled = TRUE, formAttributes = list(target = "_blank")) %>%
               hc_legend( enabled=FALSE ) %>% 
               hc_mapNavigation(enabled = TRUE) 
         })
      
      ## exporter map
      output$UN_comtrade_exporter_Map <- 
         renderHighchart({
            if( is.null(input$HSCodeTable_rows_selected) | is.null(rv_intelHS$tmp_global_by_country_raw) | input$rbtn_intel_by_hs == 'Imports'| input$select_comodity_for_market_analysis == "" )
               return(NULL)
            hcmap( data = rv_intelHS$tmp_un_comtrade_exporter_map ,
                   value = 'Value',
                   joinBy = c('iso-a2','ISO2'), 
                   name= paste0( "Export value"),
                   borderWidth = 1,
                   borderColor = "#fafafa",
                   nullColor = "lightgrey",
                   tooltip = list( table = TRUE,
                                   sort = TRUE,
                                   headerFormat = '<span style="font-size:13px">{series.name}</span><br/>',
                                   pointFormat = '{point.name}: <b>${point.value:,.1f} m</b>' )
            ) %>%
               hc_add_series(data =  rv_intelHS$tmp_un_comtrade_exporter_map ,
                             type = "mapbubble",
                             color  = hex_to_rgba("#97D700", 0.9),
                             minSize = 0,
                             name= paste0( "Export value"),
                             maxSize = 30,
                             tooltip = list(table = TRUE,
                                            sort = TRUE,
                                            headerFormat = '<span style="font-size:13px">{series.name}</span><br/>',
                                            pointFormat = '{point.name}: <b>${point.z:,.1f} m</b>')
               ) %>%
               hc_exporting(enabled = TRUE, formAttributes = list(target = "_blank")) %>%
               hc_legend( enabled=FALSE ) %>% 
               hc_mapNavigation(enabled = TRUE) 
         })
      
      ## !!!!! try UI insert: map of importer / exporters ----------- 
      output$H3_title_un_comtrade_map <-
         renderText({
            if( is.null(input$HSCodeTable_rows_selected) | is.null(rv_intelHS$tmp_global_by_country_raw) | input$rbtn_intel_by_hs == 'Imports' | input$select_comodity_for_market_analysis == "")
               return(NULL)
            paste0("Global importers and exporters at a glance")
         })
      
      output$H3_title_un_comtrade_map_note <-
         renderText({
            if( is.null(input$HSCodeTable_rows_selected) | is.null(rv_intelHS$tmp_global_by_country_raw) | input$rbtn_intel_by_hs == 'Imports' | input$select_comodity_for_market_analysis == "")
               return(NULL)
            paste0( "The size of bubble area and color both represent the value of imports or exports" ) 
         })
      
      output$H4_title_un_comtrade_importer_map <-
         renderText({
            if( is.null(input$HSCodeTable_rows_selected) | is.null(rv_intelHS$tmp_global_by_country_raw) | input$rbtn_intel_by_hs == 'Imports' | input$select_comodity_for_market_analysis == "")
               return(NULL)
            paste0("Global IMPORT markets")
         })
      
      output$H4_title_un_comtrade_exporter_map <-
         renderText({
            if( is.null(input$HSCodeTable_rows_selected) | is.null(rv_intelHS$tmp_global_by_country_raw) | input$rbtn_intel_by_hs == 'Imports'| input$select_comodity_for_market_analysis == "" )
               return(NULL)
            paste0("Global EXPORT markets")
         })
      
      ## Insert ui here
      insertUI(
         selector = '#ci_intel_by_hs_toadd_intl',
         ui =   div( id = 'ci_intel_by_hs_toadd_un_comtrade_map',
                     fluidRow(h3( HTML(paste0(textOutput("H3_title_un_comtrade_map"))) ) ,
                              p( HTML(paste0(textOutput("H3_title_un_comtrade_map_note"))) ),
                              column(6, div(id = "ci_intel_by_hs_un_comtrade_map_import", h4( HTML(paste0(textOutput("H4_title_un_comtrade_importer_map"))) ), highchartOutput('UN_comtrade_importer_Map') ) ),
                              column(6, div(id = "ci_intel_by_hs_un_comtrade_map_export", h4( HTML(paste0(textOutput("H4_title_un_comtrade_exporter_map"))) ), highchartOutput('UN_comtrade_exporter_Map') ) )
                              )
         )
      )
      ## end Try UI insert --------##
      
      
      
      ## 4.13.1 Sankey plot for a commodity ---------------
      print("--------- Building Sankey data -------------")

      observe({
         if( !is.null(input$rbtn_intel_by_hs) &&
             input$rbtn_intel_by_hs == 'Exports' &&
             !is.null(rv_intelHS$tmp_hs) ){
            ## check if able to get sankey data
            rv_intelHS$Fail_sankey_data <-
               try(
                  rv_intelHS$sankey_plot_data <-
                     get_data_sankey_uncomtrade( cc = as.character(rv_intelHS$tmp_hs), max_year = tmp_un_comtrade_max_year, eu_internal = "No" )
               )

            if( class(rv_intelHS$Fail_sankey_data) == 'try-error' )
               print("--------- FAIL: building Sankey data !!! -------------")
         }
      })
      
      print("--------- Building Sankey plots -------------")
      output$Sankey_trade_intelHS <-
         renderSankeyNetwork({
            if(  is.null(rv_intelHS$tmp_global_by_country_raw) |
                 length( as.character(rv_intelHS$tmp_hs) )>1 |
                 class(rv_intelHS$Fail_sankey_data) == 'try-error'|
                 input$rbtn_intel_by_hs == 'Imports'){
               return(NULL)
            }else{
               print("--------- Plotting Sankey plots -------------")
               sankey_uncomtrade( cc = as.character(rv_intelHS$tmp_hs), max_year = tmp_un_comtrade_max_year,eu_internal = as.character(input$btn_eu_internal_intelHS)  )
            }
         })
      
      ## !!!!! try UI insert: Sankey plot ----------- 
      output$H3_title_sankey_intelHS <-
         renderText({
            if( input$rbtn_intel_by_hs == 'Imports' ){
               return(NULL)
            }else{
               if( class(rv_intelHS$Fail_sankey_data) == 'try-error' & 
                   input$select_comodity_for_market_analysis != "" ){
                  paste0("Unable to perform global trade flow analyasis due to data query limits. Please wait for a hour.")
               }
               
               if( class(rv_intelHS$Fail_sankey_data) == 'try-error' & 
                   input$select_comodity_for_market_analysis == "" ){
                  return(NULL)
               }
               
               if( class(rv_intelHS$Fail_sankey_data) != 'try-error' &
                   input$select_comodity_for_market_analysis != "" ){
                  paste0( "Global trade flow analysis" )
               }
            }
            
            # if( input$rbtn_intel_by_hs == 'EXports' ){
            #    if( class(rv_intelHS$Fail_sankey_data) == 'try-error' ){
            #       paste0("Unable to perform global trade flow analyasis due to data query limits. Please wait for a hour.")
            #    }else{
            #       #if( is.null(rv_intelHS$tmp_global_by_country_raw) |
            #        #    length( as.character(rv_intelHS$tmp_hs) )>1 )
            #         # {return(NULL)}else{
            #          paste0( "Global trade flow analysis" )
            #          #}
            #       
            #    }
            #    
            #    # if( class(rv_intelHS$Fail_sankey_data) != 'try-error' &
            #    #     is.null(rv_intelHS$tmp_global_by_country_raw) &
            #    #     length ( as.character(rv_intelHS$tmp_hs) )>1 & 
            #    #     input$select_comodity_for_market_analysis == "" ) {
            #    #    return(NULL)
            #    # }
            #    # 
            #    # if( class(rv_intelHS$Fail_sankey_data) != 'try-error' & 
            #    #     !is.null(rv_intelHS$tmp_global_by_country_raw) &
            #    #     length ( as.character(rv_intelHS$tmp_hs) ) == 1 &
            #    #     input$select_comodity_for_market_analysis != ""
            #    #     ){
            #    #    paste0( "Global trade flow analysis" )
            #    #}
            #    
            #    # if( class(rv_intelHS$Fail_sankey_data) != 'try-error' &
            #    #     (is.null(rv_intelHS$tmp_global_by_country_raw) |
            #    #      length( as.character(rv_intelHS$tmp_hs) )>1 )
            #    #     )
            #    #    return(NULL)
            #    # paste0( "Global trade flow analysis" )
            # }
         })

      output$H3_title_sankey_note_intelHS <-
         renderUI({
            if( is.null(rv_intelHS$tmp_global_by_country_raw) |
                length( as.character(rv_intelHS$tmp_hs) )>1 |
                class(rv_intelHS$Fail_sankey_data) == 'try-error' |
                input$rbtn_intel_by_hs == 'Imports' )
               return(NULL)
            tags$p("This sankey plot shows trade flows of the selected commodity from expoters to importers. The displayed markets coverage is equal to or greater than 90% of global exports. The displayed trade flows are equal to or greater than 0.5% of global exports. Different colors are used to distinguish",
                   tags$span( "EXPORTERS", style = "color: #97D700; font-weight: bold" ),
                   ", ",
                   tags$span( "IMPORTERS", style = "color: #CD5B45; font-weight: bold"),
                   ", and ",
                   tags$span( "BOTH", style = "color: #FBE122; font-weight: bold"), "." )

         })

      ## button to choose show/hide EU internal trade
      output$Btn_EU_Internal_intelHS <-
         renderUI({
            if( is.null(rv_intelHS$tmp_global_by_country_raw) |
                length(  as.character(rv_intelHS$tmp_hs) )>1 |
                class(rv_intelHS$Fail_sankey_data) == 'try-error'|
                input$rbtn_intel_by_hs == 'Imports'  )
               return(NULL)
            radioButtons("btn_eu_internal_intelHS",
                         p("Display EU internal trade: " ),
                         choiceNames = list(icon("check"), icon("times")),
                         choiceValues = list( "Yes" , "No"),
                         #c( "Yes" = "Yes", "No" = "No"),
                         inline=T,
                         selected="No")
         })

      output$Btn_EU_Internal_note_intelHS <-
         renderUI({
            if( is.null(rv_intelHS$tmp_global_by_country_raw) |
                length( as.character(rv_intelHS$tmp_hs) )>1 |
                class(rv_intelHS$Fail_sankey_data) == 'try-error'|
                input$rbtn_intel_by_hs == 'Imports' )
               return(NULL)
            tags$p( "You may choose to show or hide EU internal trade in the sankey plot by using the buttons below." )
         })
      
      ## Insert ui here
      insertUI(
         selector = '#ci_intel_by_hs_toadd_intl',
         ui =   div( id = 'ci_intel_by_hs_toadd_un_comtrade_sankey',
                     fluidRow(h3( HTML(paste0(textOutput("H3_title_sankey_intelHS"))) ) ,
                              #p( HTML(paste0(textOutput("H2_title_sankey_note"))) ),
                              uiOutput("H3_title_sankey_note_intelHS"),
                              uiOutput("Btn_EU_Internal_note_intelHS"),
                              uiOutput("Btn_EU_Internal_intelHS"),
                              sankeyNetworkOutput( "Sankey_trade_intelHS" )
                     )
         )
      )
      ## end Try UI insert --------##
      
      ## 4.14 Generating summary tables for both importers and exporters -------
      # container of the table -- importers 
      print("--------- Building importer and exporter tables -------------")
      sketch_uncomtrade_im<-  htmltools::withTags(table(
         class = 'display',
         thead(
            tr(
               th(rowspan = 2, 'Market'),
               th(rowspan = 2, 'Import share'),
               th(colspan = 3, 'Import value'),
               th(colspan = 2, 'Import price')
            ),
            tr( #th('Country'),
               lapply(rep(c('Value ($m)', 'CAGR5', 'ABS5'), 1), th, align = 'center'),
               lapply(rep(c('$/kg (unit)', 'CAGR5' ), 1), th, align = 'center')
            )
         )
      ))
      
      ## table for importers
      output$UN_com_trade_importer_summary <-
         renderDataTable({
            if( is.null(input$HSCodeTable_rows_selected) | is.null(rv_intelHS$tmp_global_by_country_raw) | input$rbtn_intel_by_hs == 'Imports' | input$select_comodity_for_market_analysis == "")
               return(NULL)
            datatable( rv_intelHS$tmp_un_comtrade_import_summary_tab,
                      container = sketch_uncomtrade_im,
                      rownames = FALSE,
                      extensions = 'Buttons',
                      options = list(dom = 'Bltp', 
                                     scrollX = TRUE,
                                     buttons = c('copy', 'csv', 'excel', 'pdf', 'print'),
                                     pageLength = 10,
                                     lengthMenu = list(c(10, 30 , -1), list('10','30' ,'All')),
                                     columnDefs = list(list(className = 'dt-center', targets = 0:(ncol(rv_intelHS$tmp_un_comtrade_import_summary_tab)-1) ) )
                      )
            ) %>%
               formatPercentage( c('Share', 'Value_per_change', 'Price_per_change' ) , digit = 1 ) %>%
               formatCurrency( columns = c('Trade.Value..US..','Value_abs_change'), digits = 0 ) %>%
               formatCurrency( columns = c('Price'), digits = 2 ) %>%
               formatStyle(
                  c('Value_per_change' ),
                  background = styleColorBar( c(0,max(rv_intelHS$tmp_un_comtrade_import_summary_tab[1:min(20,nrow(rv_intelHS$tmp_un_comtrade_import_summary_tab)),c('Value_per_change' )],na.rm=T)*2) ,
                                              'lightblue'),
                  backgroundSize = '100% 90%',
                  backgroundRepeat = 'no-repeat',
                  backgroundPosition = 'center',
                  color = JS("value < 0 ? 'darkred' : value > 0 ? 'darkgreen' : 'black'")
               ) %>%
               formatStyle(
                  c('Price_per_change' ),
                  background = styleColorBar( c(0,max(rv_intelHS$tmp_un_comtrade_import_summary_tab[1:min(20,nrow(rv_intelHS$tmp_un_comtrade_import_summary_tab)),c('Price_per_change' )],na.rm=T)*2) ,
                                              'lightblue'),
                  backgroundSize = '100% 90%',
                  backgroundRepeat = 'no-repeat',
                  backgroundPosition = 'center',
                  color = JS("value < 0 ? 'darkred' : value > 0 ? 'darkgreen' : 'black'")
               ) %>%
               formatStyle(
                  c('Value_abs_change' ),
                  backgroundSize = '100% 90%',
                  backgroundRepeat = 'no-repeat',
                  backgroundPosition = 'center',
                  color = JS("value < 0 ? 'darkred' : value > 0 ? 'darkgreen' : 'black'")
               ) %>%
               formatStyle( 1:ncol(rv_intelHS$tmp_un_comtrade_import_summary_tab), 'vertical-align'='center', 'text-align' = 'center' )
         })
      
      ### build export table
      # container of the table -- importers 
      sketch_uncomtrade_ex <-  htmltools::withTags(table(
         class = 'display',
         thead(
            tr(
               th(rowspan = 2, 'Market'),
               th(rowspan = 2, 'Export share'),
               th(colspan = 3, 'Export value'),
               th(colspan = 2, 'Export price')
            ),
            tr( #th('Country'),
               lapply(rep(c('Value ($m)', 'CAGR5', 'ABS5'), 1), th, align = 'center'),
               lapply(rep(c('$/kg (unit)', 'CAGR5' ), 1), th, align = 'center')
            )
         )
      ))
      
      ## table for importers
      output$UN_com_trade_exporter_summary <-
         renderDataTable({
            if( is.null(input$HSCodeTable_rows_selected) | is.null(rv_intelHS$tmp_global_by_country_raw) | input$rbtn_intel_by_hs == 'Imports' | input$select_comodity_for_market_analysis == "")
               return(NULL)
            datatable( rv_intelHS$tmp_un_comtrade_export_summary_tab,
                       container = sketch_uncomtrade_ex,
                       rownames = FALSE,
                       extensions = 'Buttons',
                       options = list(dom = 'Bltp', 
                                      scrollX = TRUE,
                                      buttons = c('copy', 'csv', 'excel', 'pdf', 'print'),
                                      pageLength = 10,
                                      lengthMenu = list(c(10, 30, -1), list('10', '30' ,'All')),
                                      columnDefs = list(list(className = 'dt-center', targets = 0:(ncol(rv_intelHS$tmp_un_comtrade_export_summary_tab)-1) ) )
                       )
            ) %>%
               formatPercentage( c('Share', 'Value_per_change', 'Price_per_change' ) , digit = 1 ) %>%
               formatCurrency( columns = c('Trade.Value..US..','Value_abs_change'), digits = 0 ) %>%
               formatCurrency( columns = c('Price'), digits = 2 ) %>%
               formatStyle(
                  c('Value_per_change' ),
                  background = styleColorBar( c(0,max(rv_intelHS$tmp_un_comtrade_export_summary_tab[1:min(20,nrow(rv_intelHS$tmp_un_comtrade_export_summary_tab)),c('Value_per_change' )],na.rm=T)*2) ,
                                              'lightblue'),
                  backgroundSize = '100% 90%',
                  backgroundRepeat = 'no-repeat',
                  backgroundPosition = 'center',
                  color = JS("value < 0 ? 'darkred' : value > 0 ? 'darkgreen' : 'black'")
               ) %>%
               formatStyle(
                  c('Price_per_change' ),
                  background = styleColorBar( c(0,max(rv_intelHS$tmp_un_comtrade_export_summary_tab[1:min(20,nrow(rv_intelHS$tmp_un_comtrade_export_summary_tab)),c('Price_per_change' )],na.rm=T)*2) ,
                                              'lightblue'),
                  backgroundSize = '100% 90%',
                  backgroundRepeat = 'no-repeat',
                  backgroundPosition = 'center',
                  color = JS("value < 0 ? 'darkred' : value > 0 ? 'darkgreen' : 'black'")
               ) %>%
               formatStyle(
                  c('Value_abs_change' ),
                  backgroundSize = '100% 90%',
                  backgroundRepeat = 'no-repeat',
                  backgroundPosition = 'center',
                  color = JS("value < 0 ? 'darkred' : value > 0 ? 'darkgreen' : 'black'")
               ) %>%
               formatStyle( 1:ncol(rv_intelHS$tmp_un_comtrade_export_summary_tab), 'vertical-align'='center', 'text-align' = 'center' )
         })
      
      ## Insert ui here: summary tables  ----------------
      output$H3_title_un_comtrade_summary_tab <-
         renderText({
            if( is.null(input$HSCodeTable_rows_selected) | is.null(rv_intelHS$tmp_global_by_country_raw) | input$rbtn_intel_by_hs == 'Imports' | input$select_comodity_for_market_analysis == "")
               return(NULL)
            paste0("Summary tables for importers and exporters")
         })
      
      
      output$H4_title_un_comtrade_importer_sum_tab <-
         renderText({
            if( is.null(input$HSCodeTable_rows_selected) | is.null(rv_intelHS$tmp_global_by_country_raw) | input$rbtn_intel_by_hs == 'Imports' | input$select_comodity_for_market_analysis == "")
               return(NULL)
            paste0("Global IMPORT markets")
         })
      
      output$H4_title_un_comtrade_exporter_sum_tab <-
         renderText({
            if( is.null(input$HSCodeTable_rows_selected) | is.null(rv_intelHS$tmp_global_by_country_raw) | input$rbtn_intel_by_hs == 'Imports' | input$select_comodity_for_market_analysis == "")
               return(NULL)
            paste0("Global EXPORT markets")
         })
      
      insertUI(
         selector = '#ci_intel_by_hs_toadd_intl',
         ui =   div( id = 'ci_intel_by_hs_toadd_un_comtrade_summary_tab',
                     fluidRow(h3( HTML(paste0(textOutput("H3_title_un_comtrade_summary_tab"))) ) ,
                              #p( HTML(paste0(textOutput("H3_title_un_comtrade_map_note"))) ),
                              column(6, div(id = "ci_intel_by_hs_un_comtrade_import_summary_tab", h4( HTML(paste0(textOutput("H4_title_un_comtrade_importer_sum_tab"))) ), dataTableOutput('UN_com_trade_importer_summary') ) ),
                              column(6, div(id = "ci_intel_by_hs_un_comtrade_export_summary_tab", h4( HTML(paste0(textOutput("H4_title_un_comtrade_exporter_sum_tab"))) ), dataTableOutput('UN_com_trade_exporter_summary') ) )
                     )
         )
      )
      ## end Try UI insert --------##
      
      
      
      ## 4.14.1 Get the leftover quota and reset time ---------
      output$Un_comtrade_msg_intelHS <-
         renderUI({
            if(  is.null(input$HSCodeTable_rows_selected) | 
                 input$select_comodity_for_market_analysis == "" | 
                 input$rbtn_intel_by_hs == 'Imports'){
               return(NULL)
            }else{
               tags$div(
                  tags$hr(),
                  tags$p(paste0( "Note: ",ct_get_remaining_hourly_queries(), 
                                 " number of queries are left for the global analysis section from the UN Comtrade. The reset time will be at ", 
                                 ct_get_reset_time() ,
                                 ", while the current time is ", format(Sys.time()) , "."
                             ))
               )
            }
         })
      
      insertUI( selector = '#ci_intel_by_hs_toadd_intl',
                ui = div( id = 'ci_intel_by_hs_toadd_un_comtrade_msg',
                          fluidRow( uiOutput("Un_comtrade_msg_intelHS") ) )
              )
      
      ## 4.15 Hide generating report message ----------
      observe({
         try(
            #if( !is.null( rv_intelHS$tmp_dtf_market_map ) ){
            if( !is.null(rv_intelHS$tmp_tab) | 
                !is.null(input$select_comodity_for_market_analysis) |
                !is.null(input$HSCodeTable_rows_selected) ){
               shinyjs::hide( id = "ci_intel_hs_loading_message" )
            }
         )
      })
      
      observe({
         try(
            if( !is.null( rv_intelHS$tmp_dtf_market_map ) ){
               #if( !is.null(rv_intelHS$tmp_tab) ){
               shinyjs::hide( id = "ci_intel_hs_loading_message_intl" )
            }
         )
      })
      
      
      ## IV. Appendix .......  Reset buttions -------------------------
      ## 1. reset btn for Commodity intelligence Exports --------------------
      observeEvent( input$btn_reset_ci_ex,
                    {
                       ### try remove UI -- when pre-defined HS
                       removeUI( selector = '#body_ex_line_value_percent')
                       removeUI( selector = '#body_ex_growth_tab')
                       removeUI( selector = '#body_ci_markets_ex_selector')
                       removeUI( selector = '#body_ci_markets_ex_map')
                       removeUI( selector = '#body_ci_markets_ex_top')
                       removeUI( selector = '#body_ci_markets_ex_growth')
                       removeUI( selector = '#body_appendix_hs_ex')
                       removeUI( selector = '#body_ci_markets_ex_fail_msg')
                       removeUI( selector = '#body_ci_markets_ex_global_facts')
                       removeUI( selector = '#body_ci_markets_ex_un_comtrade_map')
                       removeUI( selector = '#body_ci_markets_ex_un_comtrade_sankey')
                       removeUI( selector = '#body_ci_markets_ex_un_comtrade_summary_tab')
                       removeUI( selector = '#body_ci_markets_ex_un_comtrade_msg')
                       shinyjs::hide( id = "body_ci_market_loading_message" )
                       
                       ### remove UI -- when self_defined HS
                       removeUI( selector = '#body_ex_line_value_percent_self_defined')
                       removeUI( selector = '#body_ex_growth_tab_self_defined')
                       removeUI( selector = '#body_ci_markets_ex_selector_self_defined')
                       removeUI( selector = '#body_selected_ex_line_value_percent_self_defined')
                       removeUI( selector = '#body_ci_markets_ex_map_self_defined')
                       removeUI( selector = '#body_ci_markets_ex_top_self_defined')
                       removeUI( selector = '#body_ci_markets_ex_growth_self_defined')
                       removeUI( selector = '#body_appendix_hs_ex_self_defined')
                       removeUI( selector = '#body_ci_markets_ex_fail_msg_self_define')
                       removeUI( selector = '#body_ci_markets_ex_global_facts_self_define')
                       removeUI( selector = '#body_ci_markets_ex_un_comtrade_map_self_define')
                       removeUI( selector = '#body_ci_markets_ex_un_comtrade_sankey_self_define')
                       removeUI( selector = '#body_ci_markets_ex_un_comtrade_summary_tab_self_define')
                       removeUI( selector = '#body_ci_markets_ex_un_comtrade_msg_self_define')
                       shinyjs::hide( id = "body_ci_market_loading_message_self_define" )
                       
                       ### clear all outputs
                       output$HS_ex <- renderDataTable(NULL)
                       output$HS_pre_ex <- renderDataTable(NULL)
                       output$CIExportValueLine <- renderHighchart(highchart())
                       output$CIExportPercentLine <- renderHighchart(highchart())
                       output$MapEXMarket <- renderHighchart(highchart())
                       output$GrowthTabSelectedEx <- renderDataTable(NULL)
                       output$SelectedExMarketLine <- renderHighchart(highchart())
                       output$SelectedExMarketLinePercent <- renderHighchart(highchart())
                       output$SelectedExMarketGrowthTab <- renderDataTable(NULL)
                       
                       ## hide all ids
                       #shinyjs::hide(selector = '#body_ex')
                       #shinyjs::hide(selector = '#body_value_ex')
                       #shinyjs::hide(selector = '#body_percent_ex')
                       #shinyjs::hide(selector = '#body_growth_ex')
                       #shinyjs::hide(selector = '#body_ci_markets_ex')
                       #shinyjs::hide(selector = '#body_appendix_hs_ex')
                       shinyjs::show(id = 'ci_howto_ex')
                       shinyjs::reset('sidebar_ci_exports')
                       ## disable the buttone ---
                       shinyjs::enable("btn_build_commodity_report_ex")
                       shinyjs::enable("select_comodity_ex")
                       shinyjs::enable("file_comodity_ex")
                       shinyjs::enable("rbtn_prebuilt_diy_ex")
                       
                     }
                    )
      
      ## 2. reset btn for Commodity intelligence Imports --------
      observeEvent( input$btn_reset_ci_im,
                    {
                       ### try remove UI -- when pre-defined HS
                       removeUI( selector = '#body_im_line_value_percent')
                       removeUI( selector = '#body_im_growth_tab')
                       removeUI( selector = '#body_ci_markets_im_selector')
                       removeUI( selector = '#body_ci_markets_im_map')
                       removeUI( selector = '#body_ci_markets_im_top')
                       removeUI( selector = '#body_ci_markets_im_growth')
                       removeUI( selector = '#body_appendix_hs_im')
                       
                       ### remove UI -- when self_defined HS
                       removeUI( selector = '#body_im_line_value_percent_self_defined')
                       removeUI( selector = '#body_im_growth_tab_self_defined')
                       removeUI( selector = '#body_ci_markets_im_selector_self_defined')
                       removeUI( selector = '#body_selected_im_line_value_percent_self_defined')
                       removeUI( selector = '#body_ci_markets_im_map_self_defined')
                       removeUI( selector = '#body_ci_markets_im_top_self_defined')
                       removeUI( selector = '#body_ci_markets_im_growth_self_defined')
                       removeUI( selector = '#body_appendix_hs_im_self_defined')
                       
                       ### clear all outputs
                       output$HS_im <- renderDataTable(NULL)
                       output$HS_pre_im <- renderDataTable(NULL)
                       output$CIImportValueLine <- renderHighchart(highchart())
                       output$CIImportPercentLine <- renderHighchart(highchart())
                       output$MapIMMarket <- renderHighchart(highchart())
                       output$GrowthTabSelectedIm <- renderDataTable(NULL)
                       output$SelectedImMarketLine <- renderHighchart(highchart())
                       output$SelectedImMarketLinePercent <- renderHighchart(highchart())
                       output$SelectedImMarketGrowthTab <- renderDataTable(NULL)
                       
                       ## hide all ids
                       # shinyjs::hide(selector = '#body_im')
                       # shinyjs::hide(selector = '#body_value_im')
                       # shinyjs::hide(selector = '#body_percent_im')
                       # shinyjs::hide(selector = '#body_growth_im')
                       # shinyjs::hide(selector = '#body_ci_markets_im')
                       # shinyjs::hide(selector = '#body_appendix_hs_im')
                       shinyjs::show(id = 'ci_howto_im')
                       shinyjs::reset('sidebar_ci_imports') 
                       ## enable the buttone ---
                       shinyjs::enable("btn_build_commodity_report_im")
                       shinyjs::enable("select_comodity_im")
                       shinyjs::enable("file_comodity_im")
                       shinyjs::enable("rbtn_prebuilt_diy_im")
                       
                     }
                    )
      
      ## 3. reset btn for Country intelligence ------------
      observeEvent( input$btn_reset_cr,
                    {
                       ## remove UIs 
                       removeUI( selector = "#country_name_single_or_multiple" )
                       removeUI( selector = "#country_info_table_map" )
                       removeUI( selector = "#country_trade_summary_all_items" )
                       removeUI( selector = "#country_trade_summary_appendix" )
                       
                       ## clear all output
                       output$CountryTable <- renderDataTable(NULL)
                       output$MapSelectedCountry <- renderHighchart(highchart())
                       output$CountryTradeTableTotal <- renderDataTable(NULL)
                       output$CountryTwowayTradeGraphTotal <- renderHighchart(highchart())
                       output$CountryTradeBalanceGraphTotal <- renderHighchart(highchart())
                       output$CountryExportsGraphTotal <- renderHighchart(highchart())
                       output$CountryExportsGraphTotalPercent <- renderHighchart(highchart())
                       output$CountryImportsGraphTotal <- renderHighchart(highchart())
                       output$CountryImportsGraphTotalPercent <- renderHighchart(highchart())
                       output$KeyExCountryTotalTreeMap <- renderHighchart(highchart())
                       output$KeyImCountryTotalTreeMap <- renderHighchart(highchart())
                       output$KeyExCountryTotalLine <- renderHighchart(highchart())
                       output$KeyExCountryTotalLinePercent <- renderHighchart(highchart())
                       output$KeyImCountryTotalLine <- renderHighchart(highchart())
                       output$CountrySummaryAllExports <- renderDataTable(NULL)
                       output$CountrySummaryAllImports <- renderDataTable(NULL)
                       output$CountrySummaryAllTwowayBalance  <- renderDataTable(NULL)

                       
                       output$SelectedMarketSingle <- renderText(NULL)
                       output$SelectedMarketMultiple <-renderText(NULL)
                          
                       ## hide all ids
                       shinyjs::show(id = 'country_howto')
                       #shinyjs::hide(id = 'country_basic_info')
                       #shinyjs::hide(id = 'country_trade_summary')
                       #shinyjs::hide(id = 'country_trade_summary_individual')
                       #shinyjs::hide(id = "country_trade_summary_appendix")
                       #shinyjs::hide(id = "country_single_name")
                       #shinyjs::hide(id = "country_multiple_name")
                       # shinyjs::hide(id = 'country_trade_single')
                       reset('sidebar_cr')
                       ## ensable a button
                       shinyjs::enable("btn_build_country_report")
                       shinyjs::enable("select_country")
                    }
                  )
      # withProgress(message = 'Finishing in about 10s', value = 1, {
      #    # Increment the progress bar, and update the detail text.
      #    incProgress( 1, detail = NULL)
      #    Sys.sleep(3)
      #    
      # })

      ## Financial benchmarking tab (from app.R port) -------------------
      fin_values <- reactiveValues(
         corp_df = NULL,
         df_dart = NULL,
         df_my = NULL,
         df_my_norm = NULL,
         fc_result = NULL,
         corp_real_loaded = FALSE
      )

      fin_validate <- shiny::validate
      fin_need <- shiny::need

      fin_ensure_corp_df <- function(force_reload = FALSE) {
         if (!force_reload && !is.null(fin_values$corp_df) && nrow(fin_values$corp_df) > 0 && fin_values$corp_real_loaded) return()
         corp_path <- fin_find_corp_codes_path()
         if (!is.null(corp_path)) {
            df <- tryCatch(
               fin_get_corp_codes("", corp_path),
               error = function(e) {
                  showNotification("corp_codes.csv 읽기 실패: 데모 리스트로 대체합니다.", type = "error", duration = 6)
                  NULL
               }
            )
            if (!is.null(df)) {
               fin_values$corp_df <- df
               fin_values$corp_real_loaded <- TRUE
               return()
            }
         }
         api_key <- fin_load_api_key()
         if (!is.null(api_key) && nzchar(api_key)) {
            df <- tryCatch(
               fin_get_corp_codes(api_key),
               error = function(e) {
                  showNotification("DART corpCode 조회 실패: 데모 리스트로 대체합니다.", type = "error", duration = 6)
                  NULL
               }
            )
            if (!is.null(df)) {
               fin_values$corp_df <- df
               fin_values$corp_real_loaded <- TRUE
               return()
            }
         } else if (is.null(corp_path)) {
            showNotification("DART_API_KEY가 없어 corp_codes.csv를 불러오지 못했습니다. 데모 리스트로 대체합니다.", type = "warning", duration = 5)
         }
         fin_values$corp_df <- fin_demo_corp_codes()
         fin_values$corp_real_loaded <- FALSE
      }

      # 기본적으로 데모 데이터를 미리 채워서 화면이 비어 보이지 않도록 함
      observeEvent(TRUE, {
         if (is.null(fin_values$df_dart) && is.null(fin_values$df_my_norm)) {
            fin_values$df_dart <- fin_sample_dart_financials(corp_name = "상장사(데모)")
            fin_values$df_my_norm <- fin_sample_my_company()
            fin_values$fc_result <- NULL
         }
      }, once = TRUE)

      fin_find_corp_codes_path <- function() {
         for (p in c("corp_codes.csv", file.path("nz-trade-dash", "corp_codes.csv"))) {
            if (file.exists(p)) return(p)
         }
         NULL
>>>>>>> main
      }
      setProgress(0.6, detail = "이름/종목코드 필터링 중")
      hits <- fin_search_corp_smart(df, query)
      hits <- hits %>%
        arrange(desc(!is.na(.data$stock_code) & nzchar(.data$stock_code)), .data$corp_name)
      if (nrow(hits) == 0) {
        showNotification("검색 결과가 없습니다.", type = "warning", duration = 4)
        return()
      }
      choices <- fin_make_corp_choices(hits)
      updateSelectInput(session, "fin_corp_pick", choices = choices, selected = choices[[1]])
      showNotification(paste0("검색 결과 ", length(choices), "건을 찾았습니다."), type = "message", duration = 3)
      setProgress(1)
    })
  })

  observeEvent(input$fin_fetch_dart, {
    fin_ensure_corp_df()
    selected_code <- if (!is.null(input$fin_corp_pick) && nzchar(input$fin_corp_pick)) input$fin_corp_pick else NA_character_
    selected_code <- trimws(selected_code)
    if (is.na(selected_code)) {
      showNotification("상장사를 먼저 선택하세요.", type = "warning", duration = 4)
      return()
    }
    corp_nm <- "상장사"
    if (!is.null(fin_values$corp_df)) {
      picked <- fin_values$corp_df %>%
        filter(.data$corp_code == selected_code | .data$corp_name == selected_code) %>%
        slice_head(n = 1)
      if (nrow(picked)) {
        corp_nm <- picked$corp_name
        selected_code <- picked$corp_code
      }
<<<<<<< HEAD
    }
    api_key <- fin_load_api_key()
    if (is.null(api_key) || !nzchar(api_key)) {
      showNotification("DART_API_KEY를 설정하세요. 데모 데이터를 사용합니다.", type = "warning", duration = 5)
      fin_values$df_dart <- fin_sample_dart_financials(corp_name = paste0(corp_nm, "(데모)"))
      return()
    }
    years <- seq(2015, as.integer(format(Sys.Date(), "%Y")))
    withProgress(message = paste0("DART 재무제표 불러오는 중: ", corp_nm), value = 0.2, {
      res <- tryCatch(
        fin_fetch_dart_financials(api_key, selected_code, corp_nm, years),
        error = function(e) {
          showNotification(paste("DART 요청 실패:", conditionMessage(e)), type = "error", duration = 8)
          NULL
        }
      )
      df <- if (!is.null(res)) res$data else NULL
      status <- if (!is.null(res)) res$status else NULL
      if (is.null(df) || nrow(df) == 0) {
        fin_values$df_dart <- fin_sample_dart_financials(corp_name = paste0(corp_nm, "(데모)"))
        detail <- if (!is.null(status)) {
          fails <- status %>% filter(!.data$ok)
          msgs <- unique(fails$message)
          paste(
            if (nrow(fails)) paste0("실패 연도: ", paste(fails$year, collapse = ", ")) else "",
            if (length(msgs) && nzchar(msgs[1])) paste0("메시지: ", paste(msgs, collapse = " / ")) else ""
          )
        } else ""
        msg <- paste("DART 불러오기 실패: 데모 데이터로 대체합니다.", detail)
        showNotification(msg, type = "warning", duration = 6)
      } else {
        fin_values$df_dart <- df
        dropped <- NULL
        drop_msgs <- NULL
        if (!is.null(status)) {
          fails <- status %>% filter(!.data$ok)
          if (nrow(fails)) dropped <- paste(fails$year, collapse = ", ")
          msgs <- unique(fails$message)
          if (length(msgs) && nzchar(msgs[1])) drop_msgs <- paste(msgs, collapse = " / ")
        }
        if (is.null(dropped)) {
          showNotification("DART 데이터가 업데이트되었습니다.", type = "message", duration = 4)
        } else {
          msg <- paste0("DART 업데이트: ", dropped, " 연도는 누락됨")
          if (!is.null(drop_msgs)) msg <- paste(msg, " - ", drop_msgs)
          showNotification(msg, type = "warning", duration = 8)
        }
      }
      setProgress(1)
    })
  })

  observeEvent(input$fin_load_demo, {
    fin_values$df_dart <- fin_sample_dart_financials(corp_name = "상장사(데모)")
    fin_values$df_my_norm <- fin_sample_my_company()
    showNotification("데모 데이터가 로드되었습니다.", type = "message", duration = 4)
  })

  observeEvent(input$fin_upload, {
    req(input$fin_upload$datapath)
    df <- fin_read_upload_df(input$fin_upload$datapath)
    cols <- names(df)
    year_col <- fin_guess_col(cols, c("yeondo", "year", "년도"))
    sales_col <- fin_guess_col(cols, c("maechul", "sale", "sales", "revenue", "매출"))
    inv_col <- fin_guess_col(cols, c("jaego", "inv", "inventory", "재고"))
    output$fin_mapping_ui <- renderUI({
      tagList(
        selectInput("fin_col_year", "연도 컬럼", choices = cols, selected = year_col),
        selectInput("fin_col_sales", "매출액 컬럼", choices = cols, selected = sales_col),
        selectInput("fin_col_inventory", "재고자산 컬럼", choices = cols, selected = inv_col),
        selectInput("fin_col_net_income", "당기순이익 컬럼(선택)", choices = c("", cols), selected = ""),
        selectInput("fin_col_assets", "자산총계 컬럼(선택)", choices = c("", cols), selected = ""),
        selectInput("fin_col_cogs", "매출원가 컬럼(선택)", choices = c("", cols), selected = "")
      )
    })
    fin_values$df_my <- df
  })

  observeEvent(list(
    input$fin_col_year, input$fin_col_sales, input$fin_col_inventory,
    input$fin_col_net_income, input$fin_col_assets, input$fin_col_cogs
  ), {
    req(fin_values$df_my)
    df <- fin_values$df_my
    get_num <- function(col) as.numeric(gsub(",", "", df[[col]]))
    res <- tibble(
      year = df[[input$fin_col_year]],
      sales = get_num(input$fin_col_sales),
      inventory = get_num(input$fin_col_inventory),
      net_income = if (nzchar(input$fin_col_net_income)) get_num(input$fin_col_net_income) else NA_real_,
      total_assets = if (nzchar(input$fin_col_assets)) get_num(input$fin_col_assets) else NA_real_,
      cogs = if (nzchar(input$fin_col_cogs)) get_num(input$fin_col_cogs) else NA_real_,
      source = "My Company"
    ) %>%
      mutate(year = as.integer(.data$year))
    fin_values$df_my_norm <- res
  }, ignoreNULL = FALSE)

  fin_combined_df <- reactive({
    rows <- list(fin_values$df_dart, fin_values$df_my_norm)
    rows <- lapply(
      rows,
      function(x) if (is.null(x)) tibble(year = integer(), sales = numeric(), inventory = numeric(), net_income = numeric(), total_assets = numeric(), cogs = numeric(), source = character()) else x
    )
    bind_rows(rows) %>%
      filter(!is.na(.data$year)) %>%
      group_by(.data$source, .data$year) %>%
      summarize(
        sales = sum(.data$sales, na.rm = TRUE),
        inventory = sum(.data$inventory, na.rm = TRUE),
        net_income = sum(.data$net_income, na.rm = TRUE),
        total_assets = sum(.data$total_assets, na.rm = TRUE),
        cogs = sum(.data$cogs, na.rm = TRUE),
        .groups = "drop"
      ) %>%
      mutate(
        inventory_turnover = if_else(!is.na(.data$cogs) & !is.na(.data$inventory) & .data$inventory != 0, .data$cogs / .data$inventory, NA_real_),
        roa = if_else(!is.na(.data$net_income) & !is.na(.data$total_assets) & .data$total_assets != 0, .data$net_income / .data$total_assets, NA_real_)
      )
  })
=======

      observe({
         if (is.null(fin_values$corp_df) || !fin_values$corp_real_loaded) {
            fin_ensure_corp_df(force_reload = TRUE)
         }
      })

      observeEvent(fin_values$corp_df, {
         df <- fin_values$corp_df
         if (is.null(df) || nrow(df) == 0) return()
         choices <- fin_make_corp_choices(head(df, 15))
         updateSelectInput(session, "fin_corp_pick", choices = choices, selected = choices[[1]])
      })

      observeEvent(input$fin_corp_search, {
         fin_ensure_corp_df(force_reload = !fin_values$corp_real_loaded)
         query <- if (is.null(input$fin_corp_query)) "" else trimws(input$fin_corp_query)
         if (!nzchar(query)) {
            showNotification("검색어를 입력하세요.", type = "warning", duration = 3)
            return()
         }
         if (is.null(fin_values$corp_df) || nrow(fin_values$corp_df) == 0) {
            showNotification("기업 리스트가 준비되지 않았습니다. 잠시 후 다시 시도하거나 데모를 사용하세요.", type = "error", duration = 4)
            return()
         }
         withProgress(message = "검색 중...", value = 0.1, {
            if (!fin_values$corp_real_loaded) {
               setProgress(0.25, detail = "기업 리스트를 새로 불러오는 중")
               fin_ensure_corp_df(force_reload = TRUE)
            }
            df <- fin_values$corp_df
            if (is.null(df) || nrow(df) == 0) {
               showNotification("기업 리스트가 없습니다. 데모/키 설정을 확인하세요.", type = "error", duration = 5)
               return()
            }
            setProgress(0.6, detail = "이름/종목코드 필터링 중")
            hits <- fin_search_corp_smart(df, query, limit = 200)
            hits <- hits %>%
               arrange(desc(!is.na(.data$stock_code) & nzchar(.data$stock_code)), .data$corp_name)
            if (nrow(hits) == 0) {
               showNotification("검색 결과가 없습니다.", type = "warning", duration = 4)
               return()
            }
            choices <- fin_make_corp_choices(hits)
            updateSelectInput(session, "fin_corp_pick", choices = choices, selected = choices[[1]])
            showNotification(paste0("검색 결과 ", length(choices), "건을 찾았습니다."), type = "message", duration = 3)
            setProgress(1)
         })
      })

   observeEvent(input$fin_fetch_dart, {
      fin_ensure_corp_df()
      selected_code <- if (!is.null(input$fin_corp_pick) && nzchar(input$fin_corp_pick)) input$fin_corp_pick else NA_character_
      selected_code <- trimws(selected_code)
      if (is.na(selected_code)) {
         showNotification("상장사를 먼저 선택하세요.", type = "warning", duration = 4)
         return()
      }
         corp_nm <- "상장사"
         if (!is.null(fin_values$corp_df)) {
            picked <- fin_values$corp_df %>%
               filter(.data$corp_code == selected_code | .data$corp_name == selected_code) %>%
               slice_head(n = 1)
            if (nrow(picked)) {
               corp_nm <- picked$corp_name
               selected_code <- picked$corp_code
            }
         }
         api_key <- fin_load_api_key()
         if (is.null(api_key) || !nzchar(api_key)) {
            showNotification("DART_API_KEY를 설정하세요. 데모 데이터를 사용합니다.", type = "warning", duration = 5)
            fin_values$df_dart <- fin_sample_dart_financials(corp_name = paste0(corp_nm, "(데모)"))
            fin_values$fc_result <- NULL
            return()
         }
         if (nrow(picked) && (is.na(picked$stock_code) || !nzchar(picked$stock_code))) {
            showNotification("비상장/종목코드가 없는 기업입니다. DART 데이터가 없을 수 있습니다.", type = "warning", duration = 6)
         }
         years <- seq(2015, as.integer(format(Sys.Date(), "%Y")))
      withProgress(message = paste0("DART 재무제표 불러오는 중: ", corp_nm), value = 0.2, {
            res <- tryCatch(
               fin_fetch_dart_financials(api_key, selected_code, corp_nm, years),
               error = function(e) {
                  showNotification(paste("DART 요청 실패:", conditionMessage(e)), type = "error", duration = 8)
                  NULL
               }
            )
            df <- if (!is.null(res)) res$data else NULL
            status <- if (!is.null(res)) res$status else NULL
            if (is.null(df) || nrow(df) == 0) {
               fin_values$df_dart <- fin_sample_dart_financials(corp_name = paste0(corp_nm, "(데모)"))
               fin_values$fc_result <- NULL
               detail <- if (!is.null(status)) {
                  fails <- status %>% filter(!.data$ok)
                  msgs <- unique(fails$message)
                  paste(
                     if (nrow(fails)) paste0("실패 연도: ", paste(fails$year, collapse = ", ")) else "",
                     if (length(msgs) && nzchar(msgs[1])) paste0("메시지: ", paste(msgs, collapse = " / ")) else ""
                  )
               } else ""
               msg <- paste("DART 불러오기 실패: 데모 데이터로 대체합니다.", detail)
               showNotification(msg, type = "warning", duration = 6)
            } else {
               fin_values$df_dart <- df
               fin_values$fc_result <- NULL
               dropped <- NULL
               drop_msgs <- NULL
               if (!is.null(status)) {
                  fails <- status %>% filter(!.data$ok)
                  if (nrow(fails)) dropped <- paste(fails$year, collapse = ", ")
                  msgs <- unique(fails$message)
                  if (length(msgs) && nzchar(msgs[1])) drop_msgs <- paste(msgs, collapse = " / ")
               }
               if (is.null(dropped)) {
                  showNotification("DART 데이터가 업데이트되었습니다.", type = "message", duration = 4)
               } else {
                  msg <- paste0("DART 업데이트: ", dropped, " 연도는 누락됨")
                  if (!is.null(drop_msgs)) msg <- paste(msg, " - ", drop_msgs)
                  showNotification(msg, type = "warning", duration = 8)
               }
            }
            setProgress(1)
         })
      })

      observeEvent(input$fin_load_demo, {
         fin_values$df_dart <- fin_sample_dart_financials(corp_name = "상장사(데모)")
         fin_values$df_my_norm <- fin_sample_my_company()
         fin_values$fc_result <- NULL
         showNotification("데모 데이터가 로드되었습니다.", type = "message", duration = 4)
      })

      observeEvent(input$fin_upload, {
         req(input$fin_upload$datapath)
         df <- fin_read_upload_df(input$fin_upload$datapath)
         cols <- names(df)
         year_col <- fin_guess_col(cols, c("yeondo", "year", "년도"))
         sales_col <- fin_guess_col(cols, c("maechul", "sale", "sales", "revenue", "매출"))
         inv_col <- fin_guess_col(cols, c("jaego", "inv", "inventory", "재고"))
         net_col <- fin_guess_col(cols, c("dang_gisun_iig", "net_income", "income", "순이익"))
         asset_col <- fin_guess_col(cols, c("total_assets", "assets", "자산", "maechul_wonga_cogs"))
         cogs_col <- fin_guess_col(cols, c("cogs", "wonga", "매출원가", "maechul_wonga_cogs"))
         output$fin_mapping_ui <- renderUI({
            tagList(
               selectInput("fin_col_year", "연도 컬럼", choices = cols, selected = year_col),
               selectInput("fin_col_sales", "매출액 컬럼", choices = cols, selected = sales_col),
               selectInput("fin_col_inventory", "재고자산 컬럼", choices = cols, selected = inv_col),
               selectInput("fin_col_net_income", "당기순이익 컬럼(선택)", choices = c("", cols), selected = net_col),
               selectInput("fin_col_assets", "자산총계 컬럼(선택)", choices = c("", cols), selected = asset_col),
               selectInput("fin_col_cogs", "매출원가 컬럼(선택)", choices = c("", cols), selected = cogs_col)
            )
         })
         fin_values$df_my <- df
         fin_values$fc_result <- NULL
      })

      observeEvent(list(input$fin_col_year, input$fin_col_sales, input$fin_col_inventory,
                        input$fin_col_net_income, input$fin_col_assets, input$fin_col_cogs), {
         req(fin_values$df_my)
         df <- fin_values$df_my
         get_num <- function(col) as.numeric(gsub(",", "", df[[col]]))
         res <- tibble(
            year = df[[input$fin_col_year]],
            sales = get_num(input$fin_col_sales),
            inventory = get_num(input$fin_col_inventory),
            net_income = if (nzchar(input$fin_col_net_income)) get_num(input$fin_col_net_income) else NA_real_,
            total_assets = if (nzchar(input$fin_col_assets)) get_num(input$fin_col_assets) else NA_real_,
            cogs = if (nzchar(input$fin_col_cogs)) get_num(input$fin_col_cogs) else NA_real_,
            source = "My Company"
         ) %>%
            mutate(year = as.integer(.data$year))
         fin_values$df_my_norm <- res
         fin_values$fc_result <- NULL
      }, ignoreNULL = FALSE)

      fin_combined_df <- reactive({
         rows <- list(fin_values$df_dart, fin_values$df_my_norm)
         rows <- lapply(rows, function(x) if (is.null(x)) tibble(year = integer(), sales = numeric(), inventory = numeric(), net_income = numeric(), total_assets = numeric(), cogs = numeric(), source = character()) else x)
        bind_rows(rows) %>%
           filter(!is.na(.data$year)) %>%
           group_by(.data$source, .data$year) %>%
           summarize(
              sales = sum(.data$sales, na.rm = TRUE),
              inventory = sum(.data$inventory, na.rm = TRUE),
              net_income = sum(.data$net_income, na.rm = TRUE),
              total_assets = sum(.data$total_assets, na.rm = TRUE),
              cogs = sum(.data$cogs, na.rm = TRUE),
              .groups = "drop"
           ) %>%
           mutate(
               inventory_turnover = dplyr::case_when(
                  is.finite(.data$cogs) & is.finite(.data$inventory) & .data$inventory != 0 ~ .data$cogs / .data$inventory,
                  is.finite(.data$sales) & is.finite(.data$inventory) & .data$inventory != 0 ~ .data$sales / .data$inventory,
                  TRUE ~ NA_real_
               ),
               roa = if_else(!is.na(.data$net_income) & !is.na(.data$total_assets) & .data$total_assets != 0, .data$net_income / .data$total_assets, NA_real_)
           )
     })

      observeEvent(fin_combined_df(), {
         df_all <- fin_combined_df()
         if (is.null(df_all) || nrow(df_all) < 3) return()
         chosen_source <- fin_pick_source(df_all)
         if (is.null(chosen_source)) return()
         df <- df_all %>% filter(.data$source == chosen_source)
         if (nrow(df) < 3) return()
         if (is.null(fin_values$fc_result)) {
            horizon <- if (!is.null(input$fin_forecast_y)) {
               max(1L, min(5L, as.integer(input$fin_forecast_y)))
            } else {
               3L
            }
            fin_values$fc_result <- fin_safe_prophet(df, horizon, source_name = chosen_source)
         }
      })

      fin_forecast_result <- reactive({
         res <- fin_values$fc_result
         fin_validate(fin_need(!is.null(res) && !is.null(res$forecast) && nrow(res$forecast) > 0,
                               "예측 결과가 없습니다. '예측 실행'을 눌러주세요."))
         res
      })

      pred_focus_source <- reactive({
         res <- fin_forecast_result()
         if (!is.null(res$source)) res$source else "선택된 소스"
      })
>>>>>>> main

  ## KPI value boxes (공통) -----------------------------------
  output$fin_kpi_sales <- renderValueBox({
    df <- fin_combined_df()
    fin_validate(fin_need(nrow(df) > 0, "데이터를 불러오세요"))
    latest_year <- max(df$year, na.rm = TRUE)
    latest <- df %>% filter(.data$year == latest_year)
    sales <- sum(latest$sales, na.rm = TRUE)
    formatted <- scales::label_number(scale_cut = scales::cut_short_scale())(sales)
    valueBox(
      value = HTML(sprintf(
        "<span class='fin-kpi-title'>최근 연도 매출</span><span class='fin-kpi-value'>%s</span>",
        formatted
      )),
      subtitle = NULL,
      color = "blue"
    )
  })

  output$fin_kpi_it <- renderValueBox({
    df <- fin_combined_df()
    fin_validate(fin_need(nrow(df) > 0, "데이터를 불러오세요"))
    latest_year <- max(df$year, na.rm = TRUE)
    latest <- df %>% filter(.data$year == latest_year)
    it <- mean(latest$inventory_turnover, na.rm = TRUE)
    it_txt <- if (is.na(it)) "자료 부족" else sprintf("%.1f배", it)
    valueBox(
      value = HTML(sprintf(
        "<span class='fin-kpi-title'>재고자산회전율</span><span class='fin-kpi-value'>%s</span>",
        it_txt
      )),
      subtitle = NULL,
      color = "green"
    )
  })

  output$fin_kpi_roa <- renderValueBox({
    df <- fin_combined_df()
    fin_validate(fin_need(nrow(df) > 0, "데이터를 불러오세요"))
    latest_year <- max(df$year, na.rm = TRUE)
    latest <- df %>% filter(.data$year == latest_year)
    roa <- mean(latest$roa, na.rm = TRUE)
    roa_txt <- if (is.na(roa)) "자료 부족" else scales::percent(roa, accuracy = 0.1)
    valueBox(
      value = HTML(sprintf(
        "<span class='fin-kpi-title'>ROA</span><span class='fin-kpi-value'>%s</span>",
        roa_txt
      )),
      subtitle = NULL,
      color = "yellow"
    )
  })

  output$fin_summary <- renderText({
    df <- fin_combined_df()
    fin_validate(fin_need(nrow(df) > 0, "데이터를 불러오세요"))
    latest_year <- max(df$year, na.rm = TRUE)
    latest <- df %>% filter(.data$year == latest_year)

    sales <- sum(latest$sales, na.rm = TRUE)
    it    <- mean(latest$inventory_turnover, na.rm = TRUE)
    roa   <- mean(latest$roa, na.rm = TRUE)

    sales_txt <- scales::label_number(scale_cut = scales::cut_short_scale())(sales)
    it_txt    <- if (is.na(it)) "자료 부족" else paste0(round(it, 1), "배")
    roa_txt   <- if (is.na(roa)) "자료 부족" else scales::percent(roa, accuracy = 0.1)

    it_comment <- if (is.na(it)) {
      ""
    } else if (it < 2) {
      "재고 회전이 다소 느린 편입니다. 재고 수준 점검이 필요해 보입니다."
    } else if (it < 5) {
      "재고 회전이 보통 수준입니다."
    } else {
      "재고 회전이 빠른 편으로, 재고 효율성이 높은 편입니다."
    }

    roa_comment <- if (is.na(roa)) {
      ""
    } else if (roa < 0.03) {
      "ROA가 낮은 편이라 자산 대비 수익성이 아쉬운 수준입니다."
    } else if (roa < 0.08) {
      "ROA가 무난한 수준입니다."
    } else {
      "ROA가 높은 편이라 자산 활용이 효율적인 편입니다."
    }

    paste0(
      latest_year, "년 기준 매출은 약 ", sales_txt,
      "이고, 재고자산회전율은 ", it_txt,
      ", ROA는 ", roa_txt, " 수준입니다. ",
      it_comment, " ", roa_comment
    )
  })

<<<<<<< HEAD
  output$fin_ts_plot <- renderPlotly({
    df <- fin_combined_df()
    fin_validate(fin_need(nrow(df) > 0, "데이터를 불러오세요"))
    plot_ly(df, x = ~year, y = ~sales, color = ~source, type = "scatter", mode = "lines+markers") %>%
      layout(
        xaxis = list(title = "연도", dtick = 1),
        yaxis = list(title = "매출액", tickformat = "~s")
      )
  })
=======
      output$fin_fc_table <- renderTable({
         res <- fin_forecast_result()
         res$forecast
      })

      output$fin_fc_plot <- renderPlotly({
         res <- fin_forecast_result()
         df_all <- fin_combined_df()
         fin_validate(fin_need(nrow(df_all) > 0, "데이터를 불러오세요"))
         src <- if (!is.null(res$source)) res$source else fin_pick_source(df_all)
         actual <- df_all %>%
            filter(.data$source == src) %>%
            arrange(.data$year) %>%
            transmute(year, value = sales)
         fin_validate(fin_need(nrow(actual) > 0, "예측을 위한 실측값이 없습니다."))
         fc <- res$forecast
         last_year <- max(actual$year, na.rm = TRUE)
         y_min <- min(c(actual$value, fc$yhat_lower), na.rm = TRUE)
         y_max <- max(c(actual$value, fc$yhat_upper), na.rm = TRUE)
         plot_ly() %>%
            add_trace(
               data = actual, x = ~year, y = ~value,
               type = "scatter", mode = "lines+markers",
               name = paste0(src, " 실제")
            ) %>%
            add_trace(
               data = fc, x = ~year, y = ~yhat,
               type = "scatter", mode = "lines+markers",
               name = paste0(src, " 예측")
            ) %>%
            add_ribbons(
               data = fc, x = ~year, ymin = ~yhat_lower, ymax = ~yhat_upper,
               name = "예측 구간",
               fillcolor = "rgba(68, 114, 196, 0.2)",
               line = list(color = "transparent")
            ) %>%
            layout(
               yaxis = list(title = "매출액"),
               xaxis = list(title = "연도"),
               shapes = list(list(
                  type = "line", x0 = last_year, x1 = last_year,
                  y0 = y_min, y1 = y_max,
                  xref = "x", yref = "y",
                  line = list(dash = "dash", color = "gray")
               )),
               annotations = list(list(
                  x = last_year, y = y_max,
                  text = "예측 시작",
                  showarrow = TRUE, arrowhead = 2, ax = 20, ay = -40,
                  bgcolor = "white"
               ))
            )
      })

      observeEvent(input$fin_do_forecast, {
         df_all <- fin_combined_df()
         fin_validate(fin_need(nrow(df_all) > 2, "예측을 위해 최소 3개 연도가 필요합니다."))
         chosen_source <- fin_pick_source(df_all)
         df <- df_all %>% filter(.data$source == chosen_source)
         fin_validate(fin_need(nrow(df) > 2, paste0("예측을 위해 ", chosen_source, " 데이터가 최소 3개 연도 필요합니다.")))
         last_year <- max(df$year, na.rm = TRUE)
         horizon <- min(as.integer(input$fin_forecast_y), max(0, 2030L - last_year))
         if (is.na(horizon) || horizon < 1) {
            showNotification("최근 연도가 2030 이상이라 예측이 없습니다.", type = "warning", duration = 5)
            return()
         }
         fin_values$fc_result <- fin_safe_prophet(df, horizon, source_name = chosen_source)
         showNotification("예측이 업데이트되었습니다.", type = "message", duration = 4)
      })

      analysis_df <- reactive({
         df <- fin_combined_df()
         fin_validate(fin_need(nrow(df) > 0, "데이터가 비어 있어요. 오른쪽 '데모 데이터 로드'나 업로드를 이용해 바로 채워보세요."))
         df
      })

      analysis_focus_source <- reactive({
         df <- analysis_df()
         if (any(df$source == "My Company")) "My Company" else fin_pick_source(df)
      })

      analysis_quality_checks <- reactive({
         df <- fin_combined_df()
         msgs <- c()
         if (nrow(df) < 3) msgs <- c(msgs, "연도별 데이터가 3개 미만이라 추세 읽기가 어려워요. 최근 3년 이상을 넣어주세요.")
         num_missing <- sum(!is.finite(df$sales)) + sum(!is.finite(df$inventory))
         if (num_missing > 0) msgs <- c(msgs, "매출/재고에 빈칸이 있어요. 업로드 파일을 다시 확인해주세요.")
         zero_years <- df %>% filter(.data$sales <= 0 | .data$inventory <= 0) %>% pull(.data$year) %>% unique()
         if (length(zero_years)) msgs <- c(msgs, paste0("매출 또는 재고가 0 이하인 연도: ", paste(sort(zero_years), collapse = ", ")))
         msgs
      })

      output$analysis_quality <- renderUI({
         msgs <- analysis_quality_checks()
         if (length(msgs) == 0) return(HTML("<p><strong>데이터 품질:</strong> 주요 결함 없음</p>"))
         HTML(paste0("<p><strong>데이터 품질 경고:</strong></p><ul>", paste(sprintf("<li>%s</li>", msgs), collapse = ""), "</ul>"))
      })

      output$analysis_top3 <- renderUI({
         df <- analysis_df()
         focus <- analysis_focus_source()
         fin_validate(fin_need(nrow(df) > 0, "데이터를 불러오면 요약이 여기에 표시됩니다."))
         latest_year <- max(df$year, na.rm = TRUE)
         latest <- df %>% filter(.data$year == latest_year)

         by_source <- latest %>%
            group_by(.data$source) %>%
            summarize(sales = sum(.data$sales, na.rm = TRUE), .groups = "drop") %>%
            arrange(desc(.data$sales))
         leader_msg <- NULL
         if (nrow(by_source) > 0) {
            leader <- by_source %>% slice_head(n = 1)
            sales_txt <- scales::label_number(scale_cut = scales::cut_short_scale())(leader$sales)
            leader_msg <- paste0(latest_year, "년 매출이 가장 큰 곳은 ", leader$source, " (약 ", sales_txt, ") 입니다.")
         }

         eff_df <- df %>%
            group_by(.data$source) %>%
            summarize(
               it = mean(.data$inventory_turnover, na.rm = TRUE),
               roa = mean(.data$roa, na.rm = TRUE),
               .groups = "drop"
            )
         eff_msg <- NULL
         if (nrow(eff_df)) {
            best_it <- eff_df %>% filter(!is.na(.data$it)) %>% arrange(desc(.data$it)) %>% slice_head(n = 1)
            best_roa <- eff_df %>% filter(!is.na(.data$roa)) %>% arrange(desc(.data$roa)) %>% slice_head(n = 1)
            parts <- c()
            if (nrow(best_it)) parts <- c(parts, paste0("재고회전율이 가장 빠른 곳: ", best_it$source, " (", round(best_it$it, 2), "배)"))
            if (nrow(best_roa)) parts <- c(parts, paste0("ROA가 높은 곳: ", best_roa$source, " (", scales::percent(best_roa$roa, accuracy = 0.1), ")"))
            if (length(parts)) eff_msg <- paste(parts, collapse = " / ")
         }

         me_vs_peers <- NULL
         others <- df %>% filter(.data$source != focus)
         me <- df %>% filter(.data$source == focus)
         if (nrow(others) && nrow(me)) {
            latest_year_me <- max(me$year, na.rm = TRUE)
            me_latest <- me %>% filter(.data$year == latest_year_me)
            others_latest <- others %>% filter(.data$year == latest_year_me)
            if (nrow(others_latest)) {
               me_it <- mean(me_latest$inventory_turnover, na.rm = TRUE)
               peer_it <- mean(others_latest$inventory_turnover, na.rm = TRUE)
               delta <- me_it - peer_it
               if (is.finite(delta)) {
                  dir <- if (delta > 0) "더 빠릅니다" else "더 느립니다"
                  me_vs_peers <- paste0("우리 재고회전율이 경쟁사 평균보다 ", abs(round(delta, 2)), "배 ", dir, ".")
               }
            }
         }

         tips <- purrr::compact(list(leader_msg, eff_msg, me_vs_peers))
         if (length(tips) == 0) return(tags$p("데이터를 불러오면 핵심 요약이 표시됩니다."))
         tags$ul(class = "friendly-list", lapply(tips, tags$li))
      })

      output$analysis_actions_friendly <- renderUI({
         df <- analysis_df()
         focus <- analysis_focus_source()
         target <- df %>% filter(.data$source == focus)
         fin_validate(fin_need(nrow(target) > 0, "비교할 기업을 선택하면 제안이 표시됩니다."))
         latest_year <- max(target$year, na.rm = TRUE)
         latest <- target %>% filter(.data$year == latest_year)
         prev <- target %>% filter(.data$year == max(target$year[target$year < latest_year], na.rm = TRUE))
         actions <- c()
         inv_turn <- mean(latest$inventory_turnover, na.rm = TRUE)
         roa <- mean(latest$roa, na.rm = TRUE)
         inv_ratio <- if (sum(latest$sales, na.rm = TRUE) != 0) mean(latest$inventory / latest$sales, na.rm = TRUE) else NA_real_
         sales_yoy <- if (nrow(prev) && sum(prev$sales, na.rm = TRUE) != 0) {
            (sum(latest$sales, na.rm = TRUE) - sum(prev$sales, na.rm = TRUE)) / sum(prev$sales, na.rm = TRUE)
         } else NA_real_
         if (!is.na(inv_turn) && inv_turn < 2) actions <- c(actions, "회전이 느린 재고를 할인/묶음판매로 정리하세요.")
         if (!is.na(inv_ratio) && inv_ratio > 0.35) actions <- c(actions, "재고가 매출 대비 높아요. 발주 속도와 안전재고를 점검하세요.")
         if (!is.na(roa) && roa < 0.03) actions <- c(actions, "자산 대비 이익이 낮습니다. 비용 구조나 비효율 지점을 점검하세요.")
         if (!is.na(sales_yoy) && sales_yoy < -0.05) actions <- c(actions, "매출이 줄고 있어요. 잘 팔리는 품목 위주로 진열/마케팅을 전환하세요.")
         if (length(actions) == 0) actions <- c("큰 위험 신호는 없습니다. 현재 전략을 유지하되 인기 품목 재고만 주기적으로 확인하세요.")
         tags$ul(class = "friendly-list", lapply(actions, tags$li))
      })

      output$analysis_kpi_row <- renderUI({
         df <- analysis_df()
         focus <- analysis_focus_source()
         fin_validate(fin_need(!is.null(focus), "비교할 소스를 선택하세요."))
         target <- df %>% filter(.data$source == focus)
         fin_validate(fin_need(nrow(target) > 0, "선택된 소스 데이터가 없습니다."))
         latest_year <- max(target$year, na.rm = TRUE)
         prev_year <- max(target$year[target$year < latest_year], na.rm = TRUE)
         latest <- target %>% filter(.data$year == latest_year)
         prev <- target %>% filter(.data$year == prev_year)
         safe_mean <- function(x) if (length(x) == 0) NA_real_ else mean(x, na.rm = TRUE)
         sales_latest <- sum(latest$sales, na.rm = TRUE)
         sales_prev <- if (nrow(prev)) sum(prev$sales, na.rm = TRUE) else NA_real_
         yoy <- if (!is.na(sales_prev) && sales_prev != 0) (sales_latest - sales_prev) / sales_prev else NA_real_
         inv_turn <- safe_mean(latest$inventory_turnover)
         roa <- safe_mean(latest$roa)
         inv_ratio <- if (sales_latest != 0) safe_mean(latest$inventory / latest$sales) else NA_real_
         fmt <- scales::label_comma()
         mk_box <- function(title, value, subtitle = "", color = "blue", formatter = fmt, unit = NULL) {
            value_txt <- if (is.na(value)) "-" else formatter(value)
            sub_txt <- paste0(subtitle, if (!is.null(unit)) paste0(" (", unit, ")") else "")
            valueBox(value = value_txt, subtitle = sub_txt, color = color)
         }
         fluidRow(
            mk_box(paste0(focus, " 매출(", latest_year, ")"), sales_latest / 1e8,
                   subtitle = if (is.na(yoy)) "전년 대비 -" else paste0("전년 대비 ", scales::percent(yoy, accuracy = 0.1)),
                   unit = "억 원", formatter = function(x) scales::comma(x, accuracy = 0.1)),
            mk_box("재고회전율", inv_turn, subtitle = "높을수록 효율", color = "green",
                   formatter = function(x) if (is.na(x)) "-" else round(x, 2),
                   unit = "회전(배)"),
            mk_box("ROA", roa, subtitle = if (is.na(roa)) "" else scales::percent(roa, accuracy = 0.1),
                   color = "yellow", formatter = function(x) if (is.na(x)) "-" else scales::percent(x, accuracy = 0.1),
                   unit = "수익성(%)"),
            mk_box("재고비율(재고/매출)", inv_ratio, subtitle = "낮을수록 건전", color = "red",
                   formatter = function(x) if (is.na(x)) "-" else scales::percent(x, accuracy = 0.1),
                   unit = "비율(%)")
         )
      })

      output$analysis_plot_1 <- renderPlotly({
         df <- analysis_df() %>%
            mutate(sales_clean = if_else(is.finite(.data$sales) & .data$sales > 0, .data$sales, NA_real_))
         plot_ly(
            df,
            x = ~year, y = ~sales_clean / 1e8, color = ~source,
            type = "scatter", mode = "lines+markers",
            connectgaps = TRUE,
            hovertemplate = paste(
               "연도: %{x}<br>",
               "매출: %{y:.1f} 억 원<br>",
               "소스: %{color}<extra></extra>"
            )
         ) %>%
            layout(
               yaxis = list(title = "매출액 (억 원)", tickformat = ",.0f"),
               xaxis = list(title = "연도")
            )
      })

      output$analysis_plot_2 <- renderPlotly({
         df <- analysis_df() %>%
            group_by(.data$source) %>%
            summarize(
               inventory_turnover = mean(.data$inventory_turnover, na.rm = TRUE),
               roa = mean(.data$roa, na.rm = TRUE),
               .groups = "drop"
            )
         fin_validate(fin_need(nrow(df) > 0, "지표를 계산할 수 없습니다."))
         p_turn <- plot_ly(
            df,
            x = ~source, y = ~inventory_turnover,
            type = "bar", name = "재고회전율(배)",
            hovertemplate = "소스: %{x}<br>회전율: %{y:.2f} 배<extra></extra>"
         ) %>%
            layout(yaxis = list(title = "재고회전율(배)"), xaxis = list(title = ""))
         p_roa <- plot_ly(
            df,
            x = ~source, y = ~roa * 100,
            type = "bar", name = "ROA(%)",
            hovertemplate = "소스: %{x}<br>ROA: %{y:.2f}%<extra></extra>"
         ) %>%
            layout(yaxis = list(title = "ROA(%)"), xaxis = list(title = ""))
         subplot(p_turn, p_roa, nrows = 1, shareX = TRUE, titleX = FALSE, margin = 0.05) %>%
            layout(showlegend = FALSE)
      })

      output$analysis_plot_3 <- renderPlotly({
         df <- analysis_df() %>%
            mutate(
               profit_margin = if_else(.data$sales != 0, .data$net_income / .data$sales, NA_real_),
               inventory_ratio = if_else(.data$sales != 0, .data$inventory / .data$sales, NA_real_)
            )
         fin_validate(fin_need(sum(!is.na(df$inventory_ratio) & !is.na(df$profit_margin)) > 0, "비교 지표가 부족합니다. 업로드/선택 데이터를 확인하세요."))
         x_limit <- if (all(is.na(df$inventory_ratio))) 1 else max(df$inventory_ratio, na.rm = TRUE)
         plot_ly(df, x = ~inventory_ratio, y = ~profit_margin, color = ~source, type = "scatter", mode = "markers",
                 text = ~paste0(source, " / ", year),
                 hovertemplate = paste(
                    "소스: %{color}<br>",
                    "연도: %{text}<br>",
                    "재고비율: %{x:.2%}<br>",
                    "이익률: %{y:.2%}<extra></extra>"
                 )) %>%
            layout(
               xaxis = list(title = "재고 비율(%)", tickformat = ".0%"),
               yaxis = list(title = "이익률(%)", tickformat = ".0%"),
               shapes = list(list(type = "line", x0 = 0, x1 = x_limit,
                                  y0 = 0, y1 = 0, xref = "x", yref = "y",
                                  line = list(color = "gray", dash = "dot")))
            )
      })

      output$analysis_desc_1 <- renderText({
         df <- analysis_df()
         latest_year <- max(df$year, na.rm = TRUE)
         latest <- df %>% filter(.data$year == latest_year)
         by_source <- latest %>%
            group_by(.data$source) %>%
            summarize(sales = sum(.data$sales, na.rm = TRUE), .groups = "drop") %>%
            arrange(desc(.data$sales))
         fin_validate(fin_need(nrow(by_source) > 0, "매출 비교를 위한 데이터가 없습니다."))
         leader <- by_source %>% slice_head(n = 1)
         sales_txt <- scales::label_number(scale_cut = scales::cut_short_scale())(leader$sales)
         paste0(latest_year, "년 매출 규모는 ", leader$source, "이(가) 약 ", sales_txt, "로 가장 큽니다.")
      })

      output$analysis_desc_2 <- renderText({
         df <- analysis_df() %>%
            group_by(.data$source) %>%
            summarize(
               it = mean(.data$inventory_turnover, na.rm = TRUE),
               roa = mean(.data$roa, na.rm = TRUE),
               .groups = "drop"
            )
         best_it <- df %>% filter(!is.na(.data$it)) %>% arrange(desc(.data$it)) %>% slice_head(n = 1)
         best_roa <- df %>% filter(!is.na(.data$roa)) %>% arrange(desc(.data$roa)) %>% slice_head(n = 1)
         parts <- c()
         if (nrow(best_it)) parts <- c(parts, paste0("재고회전율은 ", best_it$source, "가 ", round(best_it$it, 2), "배로 가장 효율적입니다."))
         if (nrow(best_roa)) parts <- c(parts, paste0("ROA는 ", best_roa$source, "가 ", scales::percent(best_roa$roa, accuracy = 0.1), "로 가장 높습니다."))
         if (length(parts) == 0) return("효율성 비교를 위한 지표가 부족합니다.")
         paste(parts, collapse = " ")
      })

      output$analysis_desc_3 <- renderText({
         df <- analysis_df() %>%
            mutate(
               profit_margin = if_else(.data$sales != 0, .data$net_income / .data$sales, NA_real_),
               inventory_ratio = if_else(.data$sales != 0, .data$inventory / .data$sales, NA_real_)
            ) %>%
            group_by(.data$source) %>%
            summarize(
               margin = mean(.data$profit_margin, na.rm = TRUE),
               inv_ratio = mean(.data$inventory_ratio, na.rm = TRUE),
               .groups = "drop"
            )
         leader <- df %>% filter(!is.na(.data$margin)) %>% arrange(desc(.data$margin)) %>% slice_head(n = 1)
         heavy_inv <- df %>% filter(!is.na(.data$inv_ratio)) %>% arrange(desc(.data$inv_ratio)) %>% slice_head(n = 1)
         msg <- character()
         if (nrow(leader)) msg <- c(msg, paste0("이익률은 ", leader$source, "가 상대적으로 우수합니다."))
         if (nrow(heavy_inv)) msg <- c(msg, paste0("재고 비중은 ", heavy_inv$source, "가 높아 관리가 필요합니다."))
         if (length(msg) == 0) return("이익률/재고 구조를 비교할 데이터가 부족합니다.")
         paste(msg, collapse = " ")
      })

      output$analysis_alerts <- renderUI({
         df <- analysis_df()
         focus <- analysis_focus_source()
         fin_validate(fin_need(!is.null(focus), "소스를 선택하세요."))
         target <- df %>% filter(.data$source == focus)
         fin_validate(fin_need(nrow(target) > 0, "소스 데이터가 없습니다."))
         latest_year <- max(target$year, na.rm = TRUE)
         latest <- target %>% filter(.data$year == latest_year)
         prev <- target %>% filter(.data$year == max(target$year[target$year < latest_year], na.rm = TRUE))
         alerts <- c()
         inv_turn <- mean(latest$inventory_turnover, na.rm = TRUE)
         roa <- mean(latest$roa, na.rm = TRUE)
         inv_ratio <- if (sum(latest$sales, na.rm = TRUE) != 0) mean(latest$inventory / latest$sales, na.rm = TRUE) else NA_real_
         sales_yoy <- if (nrow(prev) && sum(prev$sales, na.rm = TRUE) != 0) {
            (sum(latest$sales, na.rm = TRUE) - sum(prev$sales, na.rm = TRUE)) / sum(prev$sales, na.rm = TRUE)
         } else NA_real_
         if (!is.na(inv_turn) && inv_turn < 2) alerts <- c(alerts, "재고회전율이 낮아 재고 과잉 위험이 있습니다.")
         if (!is.na(roa) && roa < 0.03) alerts <- c(alerts, "ROA가 낮아 자산 활용 효율이 떨어집니다.")
         if (!is.na(inv_ratio) && inv_ratio > 0.35) alerts <- c(alerts, "재고비율이 높습니다. 재고 축소를 검토하세요.")
         if (!is.na(sales_yoy) && sales_yoy < -0.05) alerts <- c(alerts, "매출이 전년 대비 감소 중입니다.")
         if (length(alerts) == 0) return(HTML("<p><strong>위험 신호 없음.</strong></p>"))
         HTML(paste0("<p><strong>위험 신호:</strong></p><ul>", paste(sprintf("<li>%s</li>", alerts), collapse = ""), "</ul>"))
      })

      output$analysis_delta_note <- renderText({
         df <- analysis_df()
         focus <- analysis_focus_source()
         others <- df %>% filter(.data$source != focus)
         me <- df %>% filter(.data$source == focus)
         if (nrow(others) == 0 || nrow(me) == 0) return("비교 대상이 없습니다.")
         latest_year <- max(me$year, na.rm = TRUE)
         me_latest <- me %>% filter(.data$year == latest_year)
         others_latest <- others %>% filter(.data$year == latest_year)
         if (nrow(others_latest) == 0) return("동일 연도 비교 대상 없음.")
         me_it <- mean(me_latest$inventory_turnover, na.rm = TRUE)
         peer_it <- mean(others_latest$inventory_turnover, na.rm = TRUE)
         delta <- me_it - peer_it
         if (is.na(delta)) return("재고회전율 비교를 위한 데이터가 부족합니다.")
         dir <- if (delta > 0) "높습니다" else "낮습니다"
         paste0("재고회전율이 경쟁사 평균보다 ", abs(round(delta, 2)), "배 ", dir, ".")
      })

      output$analysis_insight_main <- renderValueBox({
         df <- analysis_df()
         focus <- analysis_focus_source()
         fin_validate(fin_need(!is.null(focus), "소스를 선택하세요."))
         latest_year <- max(df$year, na.rm = TRUE)
         me <- df %>% filter(.data$source == focus, .data$year == latest_year)
         sales <- sum(me$sales, na.rm = TRUE) / 1e8
         valueBox(
            value = paste0(round(sales, 1), " 억"),
            subtitle = paste0(latest_year, "년 ", focus, " 매출 (억 원)"),
            color = "blue",
            icon = NULL
         )
      })

      output$analysis_warning <- renderValueBox({
         df <- analysis_df()
         focus <- analysis_focus_source()
         fin_validate(fin_need(!is.null(focus), "소스를 선택하세요."))
         latest_year <- max(df$year, na.rm = TRUE)
         me <- df %>% filter(.data$source == focus, .data$year == latest_year)
         inv_turn <- mean(me$inventory_turnover, na.rm = TRUE)
         roa <- mean(me$roa, na.rm = TRUE)
         warn <- if (!is.na(inv_turn) && inv_turn < 2) "재고 회전 느림" else if (!is.na(roa) && roa < 0.03) "ROA 낮음" else "특별 위험 없음"
         valueBox(
            value = warn,
            subtitle = "주의 신호",
            color = if (warn == "특별 위험 없음") "green" else "yellow",
            icon = NULL
         )
      })

      output$analysis_action <- renderValueBox({
         df <- analysis_df()
         focus <- analysis_focus_source()
         fin_validate(fin_need(!is.null(focus), "소스를 선택하세요."))
         latest_year <- max(df$year, na.rm = TRUE)
         me <- df %>% filter(.data$source == focus, .data$year == latest_year)
         inv_turn <- mean(me$inventory_turnover, na.rm = TRUE)
         action <- if (!is.na(inv_turn) && inv_turn < 2) {
            "재고 축소/셀다운"
         } else {
            "성장 채널 투자"
         }
         valueBox(
            value = action,
            subtitle = "추천 행동",
            color = "purple",
            icon = NULL
         )
      })

      output$pred_ts_plot <- renderPlotly({
         res <- fin_forecast_result()
         df_all <- fin_combined_df()
         fin_validate(fin_need(nrow(df_all) > 0, "데이터를 불러오세요"))
         horizon <- if (!is.null(input$fin_forecast_y)) as.integer(input$fin_forecast_y) else res$horizon
         if (is.na(horizon) || horizon < 1) horizon <- res$horizon
         sources <- unique(df_all$source)
         actual_all <- df_all %>%
            mutate(
               sales_krw = if_else(is.finite(.data$sales) & .data$sales > 0, .data$sales / 1e8, NA_real_)
            ) %>%
            arrange(.data$year)
         forecasts <- list()
         for (s in sources) {
            df_src <- df_all %>%
               filter(.data$source == s) %>%
               mutate(sales = if_else(is.finite(.data$sales) & .data$sales > 0, .data$sales, NA_real_)) %>%
               filter(!is.na(.data$sales))
            if (nrow(df_src) < 3) next
            fc_try <- try(fin_safe_prophet(df_src, horizon, source_name = s), silent = TRUE)
            if (inherits(fc_try, "try-error") || is.null(fc_try$forecast)) next
            forecasts[[s]] <- fc_try$forecast %>% mutate(source = s, yhat = yhat / 1e8, yhat_lower = yhat_lower / 1e8, yhat_upper = yhat_upper / 1e8)
         }
         p <- plot_ly()
         for (s in sources) {
            act_src <- actual_all %>% filter(.data$source == s)
            if (nrow(act_src) == 0) next
            p <- add_trace(
               p, data = act_src, x = ~year, y = ~sales_krw, type = "scatter", mode = "lines+markers",
               name = paste0(s, " 실제"),
               connectgaps = TRUE,
               hovertemplate = "연도: %{x}<br>실제: %{y:.1f} 억 원<extra></extra>"
            )
         }
         for (s in names(forecasts)) {
            fc_src <- forecasts[[s]]
            p <- add_trace(
               p, data = fc_src, x = ~year, y = ~yhat, type = "scatter", mode = "lines+markers",
               name = paste0(s, " 예측"),
               hovertemplate = "연도: %{x}<br>예측: %{y:.1f} 억 원<extra></extra>"
            )
            p <- add_ribbons(
               p, data = fc_src, x = ~year, ymin = ~yhat_lower, ymax = ~yhat_upper,
               name = paste0(s, " 예측 구간"),
               fillcolor = "rgba(68, 114, 196, 0.2)", line = list(color = "transparent"),
               hovertemplate = "연도: %{x}<br>하한: %{ymin:.1f} 억 원<br>상한: %{ymax:.1f} 억 원<extra></extra>"
            )
            # 연결선: 마지막 실제 값과 첫 예측 값을 이어 시각적으로 끊김을 없앰
            act_src <- actual_all %>% filter(.data$source == s, is.finite(.data$sales_krw), .data$sales_krw > 0)
            if (nrow(act_src) && nrow(fc_src)) {
               last_act <- act_src %>% slice_tail(n = 1)
               first_fc <- fc_src %>% slice_head(n = 1)
               p <- add_segments(
                  p,
                  x = last_act$year, xend = first_fc$year,
                  y = last_act$sales_krw, yend = first_fc$yhat,
                  line = list(color = "rgba(0,0,0,0.3)", dash = "dot"),
                  hoverinfo = "skip",
                  showlegend = FALSE
               )
            }
         }
         p %>%
            layout(
               yaxis = list(title = "매출액 (억 원)", tickformat = ",.0f"),
               xaxis = list(title = "연도")
            )
      })

      output$pred_comp_plot <- renderPlotly({
         res <- fin_forecast_result()
         fc_full <- res$full
         fin_validate(fin_need(nrow(fc_full) > 0, "예측 결과가 없습니다."))
         plot_ly(
            fc_full, x = ~year, y = ~trend / 1e8, type = "scatter", mode = "lines",
            name = "추세", hovertemplate = "연도: %{x}<br>추세: %{y:.1f} 억 원<extra></extra>"
         ) %>%
            add_trace(
               y = ~(yhat / 1e8), name = "예측값",
               mode = "lines", line = list(dash = "dot", color = "#1f78b4"),
               hovertemplate = "연도: %{x}<br>예측값: %{y:.1f} 억 원<extra></extra>"
            ) %>%
            layout(
               xaxis = list(title = "연도"),
               yaxis = list(title = "추세 / 예측 (억 원)")
            )
      })

      output$pred_comp_note <- renderText({
         src <- pred_focus_source()
         paste0("트렌드/컴포넌트는 '", src, "' 예측 기준입니다. (My Company 데이터가 있으면 우선 사용)")
      })

      output$pred_fc_error_plot <- renderPlotly({
         res <- fin_forecast_result()
         fitted <- res$fitted
         fin_validate(fin_need(nrow(fitted) > 0, "잔차를 계산할 데이터가 부족합니다. 예측을 다시 실행하세요."))
         x_min <- min(fitted$year, na.rm = TRUE)
         x_max <- max(fitted$year, na.rm = TRUE)
         abs_max <- max(abs(fitted$resid), na.rm = TRUE)
         if (is.na(abs_max) || abs_max == 0) abs_max <- 1
         if (abs_max >= 1e9) {
            divisor <- 1e8; unit_label <- "억 원"; fmt_hover <- ":,.1f"
         } else if (abs_max >= 1e7) {
            divisor <- 1e6; unit_label <- "백만 원"; fmt_hover <- ":,.1f"
         } else if (abs_max >= 1e5) {
            divisor <- 1e4; unit_label <- "만 원"; fmt_hover <- ":,.0f"
         } else {
            divisor <- 1; unit_label <- "원"; fmt_hover <- ":,.0f"
         }
         plot_ly(
            fitted,
            x = ~year, y = ~resid / divisor,
            type = "bar", name = "잔차(실제-예측)",
            hovertemplate = paste0("연도: %{x}<br>잔차: %{y", fmt_hover, "} ", unit_label, "<extra></extra>")
         ) %>%
            layout(
               xaxis = list(title = "연도"),
               yaxis = list(
                  title = paste0("잔차 (", unit_label, ")"),
                  tickformat = if (unit_label == "원") ",.0f" else ",.1f"
               ),
               shapes = list(list(type = "line", x0 = x_min, x1 = x_max, y0 = 0, y1 = 0, xref = "x", yref = "y",
                                  line = list(color = "gray", dash = "dot")))
            )
      })

      output$pred_resid_note <- renderText({
         src <- pred_focus_source()
         paste0("잔차/불확실성 역시 '", src, "' 예측을 기반으로 합니다. 다른 소스 잔차는 별도 계산되지 않습니다.")
      })

      output$pred_quality <- renderUI({
         res <- fin_forecast_result()
         hist_years <- range(res$history$year, na.rm = TRUE)
         msgs <- c()
         if (length(res$history$year) < 3) msgs <- c(msgs, "학습 연도 3개 미만: 예측 불확실성이 큼")
         if (any(!is.finite(res$history$sales))) msgs <- c(msgs, "학습 데이터에 결측/비정상 값 포함")
         core <- paste0("학습 구간: ", hist_years[1], " ~ ", hist_years[2], ", 예측: ", res$horizon, "년")
         if (length(msgs) == 0) return(HTML(paste0("<p><strong>예측 데이터:</strong> ", core, "</p>")))
         HTML(paste0("<p><strong>예측 데이터 경고:</strong> ", core, "</p><ul>", paste(sprintf("<li>%s</li>", msgs), collapse = ""), "</ul>"))
      })

      output$pred_top3 <- renderUI({
         res <- fin_forecast_result()
         fc <- res$forecast %>% arrange(.data$year)
         fin_validate(fin_need(nrow(fc) > 0, "예측이 준비되면 요약을 보여드릴게요."))
         latest <- fc %>% slice_tail(n = 1)
         hist_last <- res$history %>% arrange(.data$year) %>% slice_tail(n = 1)
         change <- if (!is.null(hist_last$sales) && !is.na(hist_last$sales) && hist_last$sales != 0) {
            (latest$yhat - hist_last$sales) / hist_last$sales
         } else NA_real_
         change_txt <- if (is.na(change)) "직전 연도 비교 불가" else paste0("직전 대비 ", scales::percent(change, accuracy = 0.1))
         main_msg <- paste0(latest$year, "년 예상 매출: 약 ",
                            scales::label_number(scale_cut = scales::cut_short_scale())(latest$yhat),
                            " (", change_txt, ")")

         direction <- if (nrow(fc) >= 2) {
            diff <- fc$yhat[nrow(fc)] - fc$yhat[1]
            if (is.na(diff) || abs(diff) < 1e-8) "보합세" else if (diff > 0) "증가세" else "감소세"
         } else {
            "보합세"
         }
         direction_msg <- paste0("전체 추세는 ", direction, "입니다.")

         band_ratio <- median((fc$yhat_upper - fc$yhat_lower) / fc$yhat, na.rm = TRUE)
         band_msg <- if (!is.na(band_ratio)) {
            if (band_ratio > 0.4) {
               paste0("예측 폭이 넓어요(폭 약 ", scales::percent(band_ratio, accuracy = 1), "). 보수적 발주를 추천.")
            } else {
               paste0("예측 폭이 보통입니다(폭 약 ", scales::percent(band_ratio, accuracy = 1), ").")
            }
         } else NULL

         yr_range <- range(res$history$year, na.rm = TRUE)
         learn_msg <- if (all(is.finite(yr_range))) paste0("학습 기간: ", yr_range[1], "년 ~ ", yr_range[2], "년, 예측 ", res$horizon, "년") else NULL

         tips <- purrr::compact(list(main_msg, direction_msg, band_msg, learn_msg))
         if (length(tips) == 0) return(tags$p("예측이 준비되면 핵심 요약이 나타납니다."))
         tags$ul(class = "friendly-list", lapply(tips[seq_len(min(3, length(tips)))], tags$li))
      })

      output$pred_action_simple <- renderUI({
         res <- fin_forecast_result()
         fc <- res$forecast %>% arrange(.data$year)
         hist_last <- res$history %>% arrange(.data$year) %>% slice_tail(n = 1)
         fin_validate(fin_need(nrow(fc) > 0, "예측을 실행하면 추천 행동이 표시됩니다."))
         growth <- if (!is.null(hist_last$sales) && !is.na(hist_last$sales) && hist_last$sales != 0) {
            (fc$yhat[1] - hist_last$sales) / hist_last$sales
         } else NA_real_
         band_ratio <- median((fc$yhat_upper - fc$yhat_lower) / fc$yhat, na.rm = TRUE)
         headline <- if (!is.na(growth) && growth < -0.05) {
            "매출이 줄 가능성이 있어요."
         } else if (!is.na(growth) && growth > 0.1) {
            "매출이 늘 가능성이 커요."
         } else {
            "큰 변동은 없을 것으로 보입니다."
         }
         steps <- c()
         if (!is.na(growth) && growth < -0.05) {
            steps <- c(steps, "발주량과 고정비를 한시적으로 낮추고, 판매 촉진/온라인 채널을 활용하세요.")
         } else if (!is.na(growth) && growth > 0.1) {
            steps <- c(steps, "핵심 상품을 선발주하고 리드타임을 체크하세요.")
         } else {
            steps <- c(steps, "안전재고를 점검하며 주력 품목 위주로 발주하세요.")
         }
         if (!is.na(band_ratio) && band_ratio > 0.4) {
            steps <- c(steps, "예측 폭이 넓어 변동성이 큽니다. 소량·자주 발주나 주간 모니터링을 권장합니다.")
         }
         tags$div(
            tags$p(tags$strong(headline)),
            tags$ul(class = "friendly-list", lapply(steps, tags$li))
         )
      })

      output$pred_insight_main <- renderValueBox({
         res <- fin_forecast_result()
         fc <- res$forecast %>% arrange(.data$year)
         fin_validate(fin_need(nrow(fc) > 0, "예측 결과가 없습니다."))
         latest <- fc %>% slice_tail(n = 1)
         valueBox(
            value = paste0(round(latest$yhat / 1e8, 1), " 억"),
            subtitle = paste0(latest$year, "년 예상 매출"),
            color = "blue",
            icon = NULL
         )
      })

      output$pred_warning <- renderValueBox({
         res <- fin_forecast_result()
         fc <- res$forecast %>% arrange(.data$year)
         hist_last <- res$history %>% arrange(.data$year) %>% slice_tail(n = 1)
         growth <- if (!is.null(hist_last$sales) && !is.na(hist_last$sales) && hist_last$sales != 0) {
            (fc$yhat[1] - hist_last$sales) / hist_last$sales
         } else NA_real_
         band_ratio <- median((fc$yhat_upper - fc$yhat_lower) / fc$yhat, na.rm = TRUE)
         warn <- if (!is.na(growth) && growth < -0.05) {
            "매출 감소 우려"
         } else if (!is.na(band_ratio) && band_ratio > 0.4) {
            "예측 폭 넓음"
         } else {
            "안심 수준"
         }
         valueBox(
            value = warn,
            subtitle = "예측 신호",
            color = if (warn == "안심 수준") "green" else "yellow",
            icon = NULL
         )
      })

      output$pred_action_box <- renderValueBox({
         res <- fin_forecast_result()
         fc <- res$forecast %>% arrange(.data$year)
         hist_last <- res$history %>% arrange(.data$year) %>% slice_tail(n = 1)
         growth <- if (!is.null(hist_last$sales) && !is.na(hist_last$sales) && hist_last$sales != 0) {
            (fc$yhat[1] - hist_last$sales) / hist_last$sales
         } else NA_real_
         action <- if (!is.na(growth) && growth < -0.05) {
            "재고·비용 축소"
         } else if (!is.na(growth) && growth > 0.1) {
            "재고 선제 확보"
         } else {
            "보합: 재고 점검"
         }
         valueBox(
            value = action,
            subtitle = "추천 행동",
            color = "purple",
            icon = NULL
         )
      })

      output$pred_accuracy <- renderTable({
         res <- fin_forecast_result()
         fitted <- res$fitted
         fin_validate(fin_need(nrow(fitted) > 0, "정확도 계산을 위한 학습 데이터가 부족합니다."))
         mae <- mean(abs(fitted$resid), na.rm = TRUE)
         mape <- mean(abs(fitted$resid / fitted$actual), na.rm = TRUE)
         last_resid <- fitted %>% arrange(desc(.data$year)) %>% slice_head(n = 1) %>% pull(.data$resid)
         tibble(
            Metric = c("MAE", "MAPE", "최근 연도 잔차"),
            Value = c(mae / 1e8, mape, last_resid / 1e8)
         ) %>%
            mutate(Value = dplyr::case_when(
               Metric == "MAPE" ~ scales::percent(as.numeric(Value), accuracy = 0.1),
               TRUE ~ paste0(scales::comma(as.numeric(Value), accuracy = 0.1), " 억 원")
            ))
      })

      output$pred_summary <- renderText({
         res <- fin_forecast_result()
         fc <- res$forecast %>% arrange(.data$year)
         fin_validate(fin_need(nrow(fc) > 0, "예측 결과가 없습니다."))
         latest <- fc %>% slice_tail(n = 1)
         hist_last <- res$history %>% arrange(.data$year) %>% slice_tail(n = 1)
         change <- if (!is.null(hist_last$sales) && !is.na(hist_last$sales) && hist_last$sales != 0) {
            (latest$yhat - hist_last$sales) / hist_last$sales
         } else {
            NA_real_
         }
         change_txt <- if (is.na(change)) "변화율 계산 불가" else scales::percent(change, accuracy = 0.1)
         paste0(res$source, "의 ", latest$year, "년 예상 매출은 ",
                scales::label_number(scale_cut = scales::cut_short_scale())(latest$yhat),
                " (직전 연도 대비 ", change_txt, ") 수준입니다.")
      })

      output$pred_detail_1 <- renderText({
         res <- fin_forecast_result()
         yr_range <- range(res$history$year, na.rm = TRUE)
         fin_validate(fin_need(all(is.finite(yr_range)), "학습 데이터가 부족합니다."))
         paste0("학습 데이터: ", yr_range[1], "년 ~ ", yr_range[2], "년, 예측 기간: ", res$horizon, "년")
      })

      output$pred_detail_2 <- renderText({
         res <- fin_forecast_result()
         fc <- res$forecast %>% arrange(.data$year)
         fin_validate(fin_need(nrow(fc) > 0, "예측 결과가 없습니다."))
         direction <- if (nrow(fc) >= 2) {
            diff <- fc$yhat[nrow(fc)] - fc$yhat[1]
            if (is.na(diff) || abs(diff) < 1e-8) "보합세" else if (diff > 0) "증가세" else "감소세"
         } else {
            "보합세"
         }
         paste0("예측 결과는 ", direction, "로 나타납니다. 예측 구간과 불확실성을 고려해 재고 및 자금 계획을 점검하세요.")
      })

      output$pred_risk <- renderUI({
         res <- fin_forecast_result()
         fc <- res$forecast %>% arrange(.data$year)
         hist_last <- res$history %>% arrange(.data$year) %>% slice_tail(n = 1)
         growth <- if (!is.null(hist_last$sales) && !is.na(hist_last$sales) && hist_last$sales != 0) {
            (fc$yhat[1] - hist_last$sales) / hist_last$sales
         } else NA_real_
         band_ratio <- median((fc$yhat_upper - fc$yhat_lower) / fc$yhat, na.rm = TRUE)
         msgs <- c()
         if (!is.na(growth) && growth < -0.05) msgs <- c(msgs, "단기 매출 감소 위험이 있습니다.")
         if (!is.na(band_ratio) && band_ratio > 0.4) msgs <- c(msgs, "예측 구간이 넓어 불확실성이 높습니다.")
         if (length(msgs) == 0) return(HTML("<p><strong>리스크:</strong> 중대한 위험 신호 없음.</p>"))
         HTML(paste0("<p><strong>리스크:</strong></p><ul>", paste(sprintf("<li>%s</li>", msgs), collapse = ""), "</ul>"))
      })

      output$pred_action <- renderUI({
         res <- fin_forecast_result()
         fc <- res$forecast %>% arrange(.data$year)
         hist_last <- res$history %>% arrange(.data$year) %>% slice_tail(n = 1)
         growth <- if (!is.null(hist_last$sales) && !is.na(hist_last$sales) && hist_last$sales != 0) {
            (fc$yhat[1] - hist_last$sales) / hist_last$sales
         } else NA_real_
         if (!is.na(growth) && growth < -0.05) {
            return(HTML("<p><strong>액션:</strong> 비용/재고 축소, 프로모션·채널 전환으로 단기 수요를 방어하세요.</p>"))
         }
         if (!is.na(growth) && growth > 0.1) {
            return(HTML("<p><strong>액션:</strong> 매출 증가 예상. 리드타임 고려해 핵심 상품 재고를 선제 확보하세요.</p>"))
         }
         HTML("<p><strong>액션:</strong> 보합세 예상. 안전재고를 재점검하고 변동성이 큰 품목을 모니터링하세요.</p>")
      })
>>>>>>> main

  output$fin_quad_plot <- renderPlotly({
    df <- fin_combined_df()
    fin_validate(fin_need(nrow(df) > 0, "데이터를 불러오세요"))
    x_mean <- mean(df$inventory_turnover, na.rm = TRUE)
    y_mean <- mean(df$roa, na.rm = TRUE)
    plot_ly(df, x = ~inventory_turnover, y = ~roa, color = ~source, type = "scatter", mode = "markers",
            text = ~paste0(source, " (", year, ")")) %>%
      layout(
        shapes = list(
          list(type = "line", x0 = x_mean, x1 = x_mean, y0 = 0, y1 = 1, xref = "x", yref = "paper",
               line = list(dash = "dash", color = "gray")),
          list(type = "line", x0 = 0, x1 = 1, y0 = y_mean, y1 = y_mean, xref = "paper", yref = "y",
               line = list(dash = "dash", color = "gray"))
        ),
        xaxis = list(title = "재고자산회전율"),
        yaxis = list(title = "ROA", tickformat = ".0%")
      )
  })

  observeEvent(input$fin_do_forecast, {
    df_all <- fin_combined_df()
    fin_validate(fin_need(nrow(df_all) > 2, "예측을 위해 최소 3개 연도가 필요합니다."))
    chosen_source <- NULL
    non_my <- df_all %>% filter(.data$source != "My Company")
    if (nrow(non_my) > 0) chosen_source <- non_my$source[[1]]
    if (is.null(chosen_source) && any(df_all$source == "My Company")) chosen_source <- "My Company"
    if (is.null(chosen_source)) chosen_source <- df_all$source[[1]]
    df <- df_all %>% filter(.data$source == chosen_source)
    fin_validate(fin_need(nrow(df) > 2, paste0("예측을 위해 ", chosen_source, " 데이터가 최소 3개 연도 필요합니다.")))
    last_year <- max(df$year, na.rm = TRUE)
    horizon <- min(as.integer(input$fin_forecast_y), max(0, 2030L - last_year))
    if (is.na(horizon) || horizon < 1) {
      showNotification("최근 연도가 2030 이상이라 예측이 없습니다.", type = "warning", duration = 5)
      return()
    }
    fc_full <- fin_safe_prophet(df, horizon, return_full = TRUE)
    fc <- fc_full$preds
    output$fin_fc_table <- renderTable(fc)
    output$fin_fc_plot <- renderPlotly({
      combined <- bind_rows(
        df %>% transmute(year, value = sales),
        fc %>% transmute(year, value = yhat)
      ) %>% arrange(year)
      last_year <- max(df$year, na.rm = TRUE)
      plot_ly(
        data = combined,
        x = ~year, y = ~value,
        type = "scatter", mode = "lines+markers",
        name = "매출(실제+예측)"
      ) %>%
        layout(
          yaxis = list(title = "매출액"),
          shapes = list(list(
            type = "line", x0 = last_year, x1 = last_year,
            y0 = min(combined$value, na.rm = TRUE),
            y1 = max(combined$value, na.rm = TRUE),
            xref = "x", yref = "y",
            line = list(dash = "dash", color = "gray")
          )),
          annotations = list(list(
            x = last_year, y = max(combined$value, na.rm = TRUE),
            text = "예측 시작",
            showarrow = TRUE, arrowhead = 2, ax = 20, ay = -40,
            bgcolor = "white"
          ))
        )
    })
    pred_values$base <- df
    pred_values$model <- fc_full$model
    pred_values$forecast <- fc_full$forecast
    pred_values$preds <- fc
    pred_values$horizon <- horizon
  })

  ## Detail tab outputs ---------------------------------------------------

  output$detail_plot_1 <- renderPlotly({
    df <- fin_combined_df()
    fin_validate(fin_need(nrow(df) > 0, "데이터를 불러오세요"))

    my_df <- df %>% filter(.data$source == "My Company")
    # 내 기업 데이터가 없으면 가장 첫 번째 소스를 대신 사용
    if (nrow(my_df) == 0) {
      src <- df$source[[1]]
      my_df <- df %>% filter(.data$source == src)
    }

    p <- plot_ly()
    if (nrow(my_df) > 0) {
      p <- p %>%
        add_lines(
          data = my_df,
          x = ~year, y = ~sales,
          name = "내 기업 매출",
          line = list(color = "#1f77b4")
        ) %>%
        add_lines(
          data = my_df,
          x = ~year, y = ~inventory,
          name = "내 기업 재고",
          line = list(color = "#ff7f0e", dash = "dash")
        )
    }
    p %>%
      layout(
        xaxis = list(title = "연도", dtick = 1),
        yaxis = list(title = "매출 (단위: 억원)", tickformat = "~s"),
        yaxis2 = list(
          title = "재고/매출 비율 (%)",
          overlaying = "y",
          side = "right",
          tickformat = ".0%"
        ),
        margin = list(t = 20)
      )
  })

  output$detail_plot_2 <- renderPlotly({
    df <- fin_combined_df()
    fin_validate(fin_need(nrow(df) > 1, "패턴을 보려면 최소 2개 연도가 필요합니다."))

    my_df <- df %>%
      filter(.data$source == "My Company") %>%
      arrange(.data$year) %>%
      mutate(
        sales_growth = (sales / dplyr::lag(sales)) - 1,
        inv_ratio = if_else(sales > 0, inventory / sales, NA_real_)
      )
    fin_validate(fin_need(nrow(my_df) > 1, "내 기업 데이터가 부족합니다. 파일을 업로드하세요."))

    bar_colors <- ifelse(my_df$sales_growth < 0, "#d62728", "#1f77b4")

    plot_ly(my_df, x = ~year) %>%
      add_bars(y = ~sales_growth, name = "매출 성장률",
               marker = list(color = bar_colors)) %>%
      add_lines(
        y = ~inv_ratio,
        name = "재고/매출 비율",
        yaxis = "y2",
        line = list(color = "orange")
      ) %>%
      layout(
        xaxis = list(title = "연도", dtick = 1),
        yaxis = list(
          title = "매출 성장률",
          tickformat = ".0%",
          rangemode = "tozero"
        ),
        yaxis2 = list(
          title = "재고/매출 비율",
          overlaying = "y",
          side = "right",
          tickformat = ".0%"
        ),
        legend = list(x = 0.01, y = 0.99),
        margin = list(t = 20)
      )
  })

  output$detail_plot_3 <- renderPlotly({
    df <- fin_combined_df()
    fin_validate(fin_need(nrow(df) > 0, "데이터를 불러오세요"))
    my_df <- df %>% filter(.data$source == "My Company")
    if (nrow(my_df) == 0) {
      my_df <- df %>% filter(.data$source != "My Company")
    }
    fin_validate(fin_need(nrow(my_df) > 0, "내 기업 또는 비교 기업 데이터가 필요합니다."))

    latest_year <- max(my_df$year, na.rm = TRUE)
    latest <- my_df %>% filter(.data$year == latest_year) %>% slice_head(n = 1)

    parts <- tibble::tibble(
      항목 = c("매출", "매출원가", "재고", "총자산", "당기순이익"),
      값 = c(
        latest$sales,
        latest$cogs,
        latest$inventory,
        latest$total_assets,
        latest$net_income
      ) %>% as.numeric()
    )

    colors <- c(
      "매출" = "#1f77b4",
      "매출원가" = "#4f9bd4",
      "재고" = "#9ecae1",
      "총자산" = "#2ca02c",
      "당기순이익" = "#9467bd"
    )

    plot_ly(
      parts,
      x = ~항목,
      y = ~값,
      type = "bar",
      marker = list(color = unname(colors[parts$항목])),
      text = ~scales::label_number(scale_cut = scales::cut_short_scale())(값),
      textposition = "auto"
    ) %>%
      layout(
        yaxis = list(title = "금액", tickformat = "~s"),
        margin = list(t = 20)
      )
  })

  # 추가 그래프: 기본적으로 예측 결과를 재사용 (없으면 빈 플롯)
  output$detail_plot_4 <- renderPlotly({
    shiny::validate(shiny::need(!is.null(pred_values$base) && !is.null(pred_values$preds), "왼쪽 사이드바에서 '예측 실행'을 눌러 주세요."))
    df <- pred_values$base
    fc <- pred_values$preds
    combined <- bind_rows(
      df %>% transmute(year, value = sales),
      fc %>% transmute(year, value = yhat)
    ) %>% arrange(year)

    plot_ly(
      data = combined,
      x = ~year, y = ~value,
      type = "scatter", mode = "lines+markers",
      name = "매출(실제+예측)"
      ) %>%
      layout(
        yaxis = list(title = "매출액", tickformat = "~s"),
        margin = list(t = 20)
      )
  })

  output$detail_desc_1 <- renderUI({
    df <- fin_combined_df()
    fin_validate(fin_need(nrow(df) > 0, "데이터를 불러오세요"))
    my_df <- df %>% filter(.data$source == "My Company") %>% arrange(.data$year)
    if (nrow(my_df) == 0) {
      return(HTML("내 기업 데이터가 없어 재무 구조를 설명할 수 없습니다. 파일을 업로드해 주세요."))
    }

    first_year <- min(my_df$year)
    last_year <- max(my_df$year)
    first_sales <- my_df$sales[my_df$year == first_year]
    last_sales <- my_df$sales[my_df$year == last_year]
    first_inv <- my_df$inventory[my_df$year == first_year]
    last_inv <- my_df$inventory[my_df$year == last_year]
    sales_last_txt <- scales::label_number(scale_cut = scales::cut_short_scale())(last_sales)
    inv_last_txt <- scales::label_number(scale_cut = scales::cut_short_scale())(last_inv)

    sales_trend <- if (last_sales > first_sales * 1.1) {
      "매출은 전반적으로 올라가는 방향입니다. 해마다 조금씩이라도 더 많은 손님이 들어오고 있다는 뜻이므로, 잘 팔리는 카테고리에 재고와 마케팅을 더 집중해도 좋습니다."
    } else if (last_sales < first_sales * 0.9) {
      "매출은 전반적으로 줄어드는 방향입니다. 예전만큼 팔리지 않는 구간이 생기고 있으니, 무엇이 고객을 멀어지게 하는지(가격·상품·채널)를 점검해 보세요."
    } else {
      "매출은 큰 폭의 변화 없이 비슷한 수준입니다. 큰 위기는 없지만, 성장 기회도 제한되어 있으니 새로운 상품/채널 실험이 필요할 수 있습니다."
    }

    inv_trend <- if (last_inv > first_inv * 1.1) {
      "재고도 함께 늘어나는 흐름이라, 팔리지 않고 창고에 쌓이는 물량이 없는지 확인이 필요합니다."
    } else if (last_inv < first_inv * 0.9) {
      "재고는 오히려 줄어드는 쪽이라, 불필요하게 쌓여 있던 재고를 어느 정도 정리해 온 것으로 보입니다."
    } else {
      "재고 규모 역시 크게 늘지도 줄지도 않고, 비슷한 수준을 유지하고 있습니다."
    }

    avg_it <- mean(my_df$inventory_turnover, na.rm = TRUE)
    it_txt <- if (is.na(avg_it)) {
      "재고 회전율(연간 재고 교체 횟수)을 계산할 수 없어 정확한 속도를 확인하기 어렵습니다."
    } else if (avg_it < 2) {
      "재고가 1년에 두 번도 채 돌지 않아 느리게 팔리는 상품(슬로우무버)이 꽤 있을 수 있습니다. 오래 쌓여 있는 아이템은 세일/단종 대상을 따로 관리하는 것이 좋습니다."
    } else if (avg_it < 5) {
      "재고는 1년에 2~5번 정도의 속도로 빠져 나가며 업종 평균 수준을 보입니다. 잘 나가는 상품과 그렇지 않은 상품을 구분해 집중도를 조절하면 효율이 더 좋아집니다."
    } else {
      "재고는 1년에 5번 이상 새로 채워질 만큼 잘 팔리므로, 재고가 창고에 오래 묶여 있는 리스크는 크지 않습니다. 대신 품절 위험과 공급 일정 관리에 주의하세요."
    }

    inv_ratio <- ifelse(is.na(last_sales) || last_sales == 0, NA_real_, last_inv / last_sales)
    ratio_txt <- if (is.na(inv_ratio)) {
      ""
    } else {
      paste0(" (재고/매출 비율 약 ", scales::percent(inv_ratio, accuracy = 0.1), ")")
    }

    HTML(paste0(
      "<b>매출/재고 구조</b><br/>",
      sprintf("%d년부터 %d년까지의 흐름을 보면, ", first_year, last_year),
      "우리 매출이 어떻게 달라졌고 그에 맞춰 재고가 잘 따라가고 있는지를 한눈에 확인할 수 있습니다. ",
      sales_trend, "<br/>",
      inv_trend, " 재고가 매출보다 더 가파르게 늘고 있다면 창고 공간과 자금이 묶이기 쉬우니 조기 경보로 삼으세요.<br/>",
      it_txt, "<br/><br/>",
      sprintf(
        "최근 연도(%d년) 기준으로는 <b>매출 약 %s</b>, <b>재고 약 %s</b>%s 수준입니다. ",
        last_year, sales_last_txt, inv_last_txt, ratio_txt
      ),
      "이 값을 ‘목표 재고 한도’와 비교해 초과분을 정리하면 현금 흐름이 훨씬 가벼워집니다."
    ))
  })

  output$detail_desc_2 <- renderUI({
    df <- fin_combined_df()
    fin_validate(fin_need(nrow(df) > 1, "패턴을 설명하려면 최소 2개 연도가 필요합니다."))
    my_df <- df %>%
      filter(.data$source == "My Company") %>%
      arrange(.data$year) %>%
      mutate(
        sales_growth = (sales / dplyr::lag(sales)) - 1
      )
    if (nrow(my_df) < 2 || all(is.na(my_df$sales_growth))) {
      return(HTML("성장 패턴을 계산할 수 있는 데이터가 충분하지 않습니다."))
    }

    avg_growth <- mean(my_df$sales_growth, na.rm = TRUE)
    vol <- sd(my_df$sales_growth, na.rm = TRUE)
    avg_growth_txt <- if (is.na(avg_growth)) {
      ""
    } else if (avg_growth > 0.1) {
      paste0("연평균으로 따지면 매년 ", scales::percent(avg_growth, accuracy = 0.1), "씩 커졌습니다. 성수기를 확대하거나 신규 채널을 과감히 테스트해 볼 만합니다. ")
    } else if (avg_growth > 0.02) {
      paste0("연평균 성장률은 약 ", scales::percent(avg_growth, accuracy = 0.1), "입니다. 느리지만 꾸준히 오른다면 잘 팔리는 상품군에 더 집중하면 좋습니다. ")
    } else {
      paste0("연평균 성장률은 약 ", scales::percent(avg_growth, accuracy = 0.1), " 수준으로, 크게 늘지도 줄지도 않는 상태입니다. 브랜딩·상품 믹스를 새로 볼 필요가 있습니다. ")
    }

    vol_txt <- if (vol > 0.15) {
      "연도별 매출 폭이 크게 출렁입니다. 특정 시즌이나 이벤트에 매출이 크게 쏠리고 있을 가능성이 높으니, 성수기 대비를 철저히 하고 비수기에는 재고를 낮게 가져가야 합니다."
    } else if (vol > 0.05) {
      "연도별 매출 차이가 어느 정도 있습니다. 어느 시즌/채널이 성과를 끌어올리는지 파악해 집중하면 효과적입니다."
    } else {
      "연도별 매출 차이가 크지 않아 안정적으로 유지되고 있습니다. 이럴 때는 상품 구성과 채널 효율을 더 세밀하게 관리하면 효과가 납니다."
    }

    tip_txt <- if (avg_growth > 0.05) {
      "성장 구간에서는 “언제 가장 잘 팔리는지(예: 봄 신상품, 연말 이벤트)”를 미리 파악해 그 시기에 재고·인력을 집중하세요."
    } else if (avg_growth < 0) {
      "감소 구간에서는 성수기가 약해졌는지, 특정 채널에서만 하락하는지 나눠 보는 것이 중요합니다. 잘 되는 영역은 살리고, 부진한 영역은 정리하는 방향이 좋습니다."
    } else {
      "예측하기 어려운 구간에서는 시즌보다 상품 믹스와 채널 효율이 더 중요합니다. 어떤 상품이 수익에 기여하는지 따로 분석해 보세요."
    }

    HTML(paste0(
      "<b>시즌·패턴</b><br/>",
      "매년 얼마나 성장했는지, 성과가 어느 시점에 몰리는지를 나눠서 보는 것이 핵심입니다. ",
      avg_growth_txt, "<br/>",
      vol_txt, "<br/>",
      "또한 월별·분기별 판매 캘린더를 다시 작성해 ‘언제 물량을 쌓고, 언제 줄일지’ 미리 정의해 두면 예측 오차가 줄어듭니다. ",
      tip_txt
    ))
  })

  output$detail_desc_3 <- renderUI({
    df <- fin_combined_df()
    fin_validate(fin_need(nrow(df) > 0, "데이터를 불러오세요"))
    my_df <- df %>% filter(.data$source == "My Company")
    if (nrow(my_df) == 0) {
      return(HTML("내 기업 데이터가 없어 리스크 포인트를 구체적으로 제시하기 어렵습니다."))
    }

    latest_year <- max(my_df$year, na.rm = TRUE)
    latest <- my_df %>% filter(.data$year == latest_year)

    it <- mean(latest$inventory_turnover, na.rm = TRUE)
    roa <- mean(latest$roa, na.rm = TRUE)
    roa_txt <- if (is.na(roa)) "" else paste0("(자산 대비 이익 약 ", scales::percent(roa, accuracy = 0.1), ")")

    it_risk <- if (is.na(it)) {
      "재고가 얼마나 빨리 팔려 나가는지(재고 회전 속도)를 계산하기 어려워, 재고 리스크를 정확히 판단하기가 어렵습니다."
    } else if (it < 2) {
      "재고가 1년에 두 번도 채 돌지 않는 속도로, 팔리지 않고 쌓여 있는 상품이 꽤 있을 수 있습니다."
    } else if (it < 5) {
      "재고는 1년에 2~5번 정도의 속도로 빠져 나가며, 업종 평균 수준의 속도를 보입니다."
    } else {
      "재고가 1년에 5번 이상 새로 채워질 정도로 잘 팔리고 있어, 재고가 창고에 오래 머무르는 리스크는 크지 않은 편입니다."
    }

    roa_risk <- if (is.na(roa)) {
      "회사에 묶여 있는 자산(가게·시설·재고 등)을 이용해 어느 정도 이익을 내고 있는지 계산하기 어려운 상태입니다."
    } else if (roa < 0.03) {
      "자산 대비 이익이 3% 미만으로, 매출 규모에 비해 남는 돈이 많지 않은 편입니다. 가게 임대료·인건비 같은 고정비 구조를 점검할 필요가 있습니다."
    } else if (roa < 0.08) {
      "자산 대비 이익이 3~8% 수준으로, 아주 높지는 않지만 일반적인 수준의 수익성을 보입니다."
    } else {
      "자산 대비 이익이 8% 이상으로, 현재 자산을 비교적 잘 활용해 돈을 벌고 있는 편입니다."
    }

    HTML(paste0(
      "<b>리스크와 주의 신호</b><br/>",
      latest_year, "년 데이터를 기준으로 현재 체력을 요약하면 다음과 같습니다.<br/>",
      "· ", it_risk, "<br/>",
      "· ", roa_risk, " ", roa_txt, "<br/>",
      "재고 속도와 자산 활용도가 동시에 좋지 않으면 현금 흐름이 빠르게 나빠질 수 있으니, 재고 회전 목표와 월별 손익표를 함께 모니터링하세요."
    ))
  })

  output$detail_action <- renderUI({
    df <- fin_combined_df()
    fin_validate(fin_need(nrow(df) > 0, "데이터를 불러오세요"))
    my_df <- df %>% filter(.data$source == "My Company")
    if (nrow(my_df) == 0) {
      return(HTML("내 기업 데이터가 없어 구체적인 액션 플랜을 제시하기 어렵습니다. 데이터 업로드 후 다시 확인해 주세요."))
    }

    latest_year <- max(my_df$year, na.rm = TRUE)
    latest <- my_df %>% filter(.data$year == latest_year)

    it <- mean(latest$inventory_turnover, na.rm = TRUE)
    roa <- mean(latest$roa, na.rm = TRUE)

    # 상황에 따른 구체적인 TODO 리스트
    items <- if (is.na(it) || is.na(roa)) {
      c(
        "POS·ERP 등에서 최근 5년 데이터를 다시 추출해 누락된 해가 없는지 확인하고, 가능한 한 동일한 회계 기준으로 맞춥니다.",
        "핵심 상품 20%를 골라 재고 수량·입출고 날짜를 체크리스트로 정리하면, ‘느리게 움직이는 상품’이 바로 드러납니다."
      )
    } else if (it < 2 || roa < 0.03) {
      c(
        "최근 3년간 거의 팔리지 않은 SKU를 리스트업해 ‘정리 대상’ 컬럼을 만들고, 할인·묶음 판매·단종 계획을 각 항목에 적어 둡니다.",
        "점포·온라인·마켓플레이스별 손익을 간단한 표로 만들어 적자가 나는 채널은 임대료/수수료 재협상을 시도합니다.",
        "임대료·인건비·물류비 같은 고정비를 매출 대비 비중으로 정리하고, 상위 항목부터 비용 절감 아이디어를 실행합니다."
      )
    } else if (it < 5 || roa < 0.08) {
      c(
        "재고가 느리게 도는 카테고리에 대해 ‘어떤 옵션이 안 팔리는지(색상/사이즈/가격대)’를 세분화해 원인을 찾아냅니다.",
        "마진이 낮은 상품은 공급가 재협상, 번들 구성, 구독형 패키지 등으로 판가·회전율을 동시에 높일 방법을 고민합니다.",
        "잘 팔리는 상품에는 재고와 광고 예산을 더 배분하고, 그렇지 않은 군은 SKU 수를 줄여 팀의 관리 범위를 줄입니다."
      )
    } else {
      c(
        "재고가 빠르게 돌고 수익성도 준수하므로, 성장성이 높은 카테고리나 신규 채널(온라인, 라이브커머스 등)에 테스트 예산을 배정합니다.",
        "동시에 매출이 주춤한 영역에는 미리 재고를 줄이고, 소량 테스트 발주로 전환해 손실을 제한합니다.",
        "분기마다 ‘매출-재고-이익’ 세 지표를 동일한 대시보드에서 확인하는 루틴을 만들어, 이상 징후를 빠르게 잡아냅니다."
      )
    }

    HTML(paste0(
      "<b>다음 액션</b><br/><ul><li>",
      paste(items, collapse = "</li><li>"),
      "</li></ul>"
    ))
  })

  ## Prediction tab outputs (Prophet 기반) --------------------------------

  output$pred_ts_plot <- renderPlotly({
    shiny::validate(shiny::need(!is.null(pred_values$base) && !is.null(pred_values$forecast), "왼쪽 사이드바에서 '예측 실행'을 눌러 주세요."))
    df <- pred_values$base
    fc <- pred_values$forecast
    fc_years <- as.integer(format(fc$ds, "%Y"))
    last_year <- max(df$year, na.rm = TRUE)
    fut_fc <- fc[fc_years > last_year, ]

    p <- plot_ly() %>%
      add_lines(
        data = df,
        x = ~year, y = ~sales,
        name = "실제 매출",
        line = list(color = "#1f77b4")
      )

    if (nrow(fut_fc) > 0) {
      p <- p %>%
        add_lines(
          data = fut_fc,
          x = ~as.integer(format(ds, "%Y")), y = ~yhat,
          name = "예측 매출",
          line = list(color = "#ff7f0e", dash = "dash")
        ) %>%
        add_ribbons(
          data = fut_fc,
          x = ~as.integer(format(ds, "%Y")),
          ymin = ~yhat_lower,
          ymax = ~yhat_upper,
          name = "예측 구간",
          fillcolor = "rgba(255,127,14,0.2)",
          line = list(color = "transparent")
        )
    }

    p %>% layout(
      xaxis = list(title = "연도", dtick = 1),
      yaxis = list(title = "매출액", tickformat = "~s"),
      margin = list(t = 20)
    )
  })

  output$pred_comp_plot <- renderPlotly({
    shiny::validate(shiny::need(!is.null(pred_values$forecast), "왼쪽 사이드바에서 '예측 실행'을 눌러 주세요."))
    fc <- pred_values$forecast
    trend_df <- tibble::tibble(
      date = fc$ds,
      trend = fc$trend
    )
    yearly_df <- tibble::tibble(
      day = as.integer(format(fc$ds, "%j")),
      label = format(fc$ds, "%m-%d"),
      yearly = fc$yearly
    ) %>%
      dplyr::group_by(.data$day) %>%
      dplyr::summarise(
        yearly = mean(.data$yearly, na.rm = TRUE),
        label = dplyr::first(.data$label),
        .groups = "drop"
      ) %>%
      dplyr::arrange(.data$day)

    trend_plot <- plot_ly(trend_df, x = ~date, y = ~trend, type = "scatter", mode = "lines", name = "추세") %>%
      layout(
        xaxis = list(title = "연도"),
        yaxis = list(title = "추세 (억 원)", tickformat = "~s"),
        margin = list(t = 10)
      )

    tickvals <- c(15, 75, 135, 195, 255, 315)
    ticktext <- c("1월", "3월", "5월", "7월", "9월", "11월")
    yearly_plot <- plot_ly(yearly_df, x = ~day, y = ~yearly, type = "scatter", mode = "lines", name = "연간 패턴", line = list(color = "#ff7f0e")) %>%
      layout(
        xaxis = list(title = "연중 흐름", tickvals = tickvals, ticktext = ticktext),
        yaxis = list(title = "계절 효과 (억 원)", tickformat = "~s"),
        margin = list(t = 10)
      )

    subplot(trend_plot, yearly_plot, nrows = 2, shareX = FALSE, titleX = FALSE)
  })

  output$pred_fc_error_plot <- renderPlotly({
    shiny::validate(shiny::need(!is.null(pred_values$base) && !is.null(pred_values$forecast), "왼쪽 사이드바에서 '예측 실행'을 눌러 주세요."))
    df <- pred_values$base
    fc <- pred_values$forecast
    fc_years <- as.integer(format(fc$ds, "%Y"))
    hist_fc <- fc[fc_years %in% df$year, ]

    comp <- tibble::tibble(
      year = df$year,
      actual = df$sales,
      pred = hist_fc$yhat[match(df$year, as.integer(format(hist_fc$ds, "%Y")))]
    ) %>%
      dplyr::mutate(error = pred - actual)

    plot_ly(comp, x = ~year) %>%
      add_bars(y = ~error, name = "예측 오차", marker = list(color = "#d62728")) %>%
      add_lines(y = ~0, name = "오차 0 기준", line = list(color = "gray", dash = "dash")) %>%
      layout(
        xaxis = list(title = "연도", dtick = 1),
        yaxis = list(title = "예측 - 실제", tickformat = "~s"),
        margin = list(t = 20)
      )
  })

  output$pred_error_box <- renderPlotly({
    shiny::validate(shiny::need(!is.null(pred_values$base) && !is.null(pred_values$forecast), "왼쪽 사이드바에서 '예측 실행'을 눌러 주세요."))
    df <- pred_values$base
    fc <- pred_values$forecast
    fc_years <- as.integer(format(fc$ds, "%Y"))
    hist_fc <- fc[fc_years %in% df$year, ]

    comp <- tibble::tibble(
      year = df$year,
      actual = df$sales,
      pred = hist_fc$yhat[match(df$year, as.integer(format(hist_fc$ds, "%Y")))]
    ) %>%
      dplyr::mutate(error = pred - actual)

    plot_ly(comp, y = ~error, type = "box", name = "오차 분포", boxpoints = "all", jitter = 0.3) %>%
      layout(
        yaxis = list(title = "예측 - 실제", tickformat = "~s"),
        margin = list(t = 20)
      )
  })

  output$pred_cum_plot <- renderPlotly({
    shiny::validate(shiny::need(!is.null(pred_values$base) && !is.null(pred_values$preds), "왼쪽 사이드바에서 '예측 실행'을 눌러 주세요."))
    df <- pred_values$base %>% arrange(.data$year) %>% mutate(value = cumsum(.data$sales), type = "실제 누적")
    fut_fc <- pred_values$preds %>% arrange(.data$year) %>% mutate(value = cumsum(.data$yhat), type = "예측 누적")
    combined <- bind_rows(df, fut_fc)
    plot_ly(combined, x = ~year, y = ~value, color = ~type, type = "scatter", mode = "lines+markers") %>%
      layout(
        xaxis = list(title = "연도", dtick = 1),
        yaxis = list(title = "누적 매출", tickformat = "~s"),
        margin = list(t = 20)
      )
  })

  output$pred_summary <- renderText({
    shiny::validate(shiny::need(!is.null(pred_values$base) && !is.null(pred_values$preds), "왼쪽 사이드바에서 '예측 실행'을 눌러 주세요."))
    df <- pred_values$base
    fut_fc <- pred_values$preds
    if (nrow(fut_fc) == 0) {
      return("추가 예측 구간이 충분하지 않아 요약을 제공하기 어렵습니다.")
    }
    last_hist <- dplyr::slice_tail(df, n = 1)
    last_year <- max(df$year, na.rm = TRUE)
    last_sales <- last_hist$sales
    next_fc <- fut_fc[1, ]
    change_rate <- as.numeric(next_fc$yhat / last_sales - 1)
    dir_txt <- if (change_rate > 0.05) {
      "상승 구간일 가능성이 크니 인기 상품의 물량과 마케팅을 조금 더 공격적으로 가져가도 됩니다."
    } else if (change_rate < -0.05) {
      "감소 위험 신호가 있으므로 고정비·재고를 미리 줄여 방어적인 태세를 갖추는 것이 좋습니다."
    } else {
      "큰 폭의 변동이 예상되지는 않아 기존 운영 리듬을 유지하되, 세부 지표를 통해 미세 조정하는 쪽이 안전합니다."
    }
    paste0(
      last_year + 1, "년 예상 매출은 약 ",
      scales::label_number(scale_cut = scales::cut_short_scale())(next_fc$yhat),
      " 수준으로, 전년 대비 ",
      scales::percent(change_rate, accuracy = 0.1),
      " 변화가 전망됩니다. ",
      dir_txt,
      " 모든 수치는 실제 판매가 반영될 때마다 달라질 수 있으니, 분기마다 실적을 업데이트해 추세가 꺾이는 시점을 빠르게 포착하세요."
    )
  })

  output$pred_detail_1 <- renderText({
    shiny::validate(shiny::need(!is.null(pred_values$base) && !is.null(pred_values$horizon), "왼쪽 사이드바에서 '예측 실행'을 눌러 주세요."))
    df <- pred_values$base
    years <- range(df$year, na.rm = TRUE)
    horizon <- pred_values$horizon
    paste0(
      years[1], "년부터 ", years[2], "년까지 총 ", nrow(df), "개의 연도별 매출 값을 Prophet 모델에 학습시켰습니다. ",
      "Prophet은 추세(오르는/내려가는 속도)와 계절 패턴(연중 어떤 달에 잘 팔리는지)을 따로 계산해 줍니다. ",
      "이번 모델은 연간 패턴만 사용했고 주간·일간 노이즈는 꺼 두어, 장기 흐름을 보는 데 집중했습니다. ",
      "그 결과를 바탕으로 앞으로 ", horizon, "년치 예상 매출 밴드를 제시하니, ‘많이 팔릴 때 대비용 재고’, ‘주의해야 할 구간’을 가늠하는 참고 자료로 활용하세요."
    )
  })

  output$pred_detail_2 <- renderText({
    shiny::validate(shiny::need(!is.null(pred_values$base) && !is.null(pred_values$preds), "왼쪽 사이드바에서 '예측 실행'을 눌러 주세요."))
    df <- pred_values$base
    fut_fc <- pred_values$preds
    horizon <- pred_values$horizon
    if (nrow(fut_fc) == 0) {
      return("예측 결과를 바탕으로 한 인사이트를 제공하기에 데이터가 충분하지 않습니다.")
    }
    hist_last <- dplyr::slice_tail(df, n = 1)
    last_sales <- hist_last$sales
    fut_mean <- mean(fut_fc$yhat, na.rm = TRUE)
    growth <- fut_mean / last_sales - 1
    insight <- if (growth > 0.1) {
      "긍정적인 성장 흐름이 이어질 가능성이 높습니다. 인기 상품군을 미리 확정하고, 물류 인력 및 광고 예산을 피크 시즌에 맞춰 확대해 두세요."
    } else if (growth > 0) {
      "완만한 성장세라서 ‘어디에 힘을 실을지’ 정하는 게 중요합니다. 상위 매출 20% SKU를 중심으로 재고를 채우고, 나머지는 수요가 확인될 때까지 소량만 보유하세요."
    } else {
      "정체 혹은 하락 신호가 잡혀 비용 효율이 관건입니다. 고정비 구조를 재점검하고, 기존 고객 재구매 캠페인과 묶음 할인 등 빠른 현금 회전을 우선시하세요."
    }
    trend_phrase <- if (growth > 0.05) {
      "늘어날 가능성이 높다"
    } else if (growth > -0.02) {
      "큰 변화 없이 유지될 가능성이 높다"
    } else {
      "줄어들 가능성이 있다"
    }
    paste0(
      "예측 곡선이 보여 주는 메시지는 “향후 ", horizon, "년 동안 매출이 ",
      trend_phrase,
      "”는 것입니다. ",
      insight,
      " 또한 예측 오차 그래프를 함께 보면서, 어느 연도에서 모델이 유독 빗나갔는지 체크하면 ‘불확실한 시기’에 대비하는 힌트를 얻을 수 있습니다."
    )
  })

  output$pred_error_hist <- renderPlotly({
    shiny::validate(shiny::need(!is.null(pred_values$base) && !is.null(pred_values$forecast), "왼쪽 사이드바에서 '예측 실행'을 눌러 주세요."))
    df <- pred_values$base
    fc <- pred_values$forecast
    fc_years <- as.integer(format(fc$ds, "%Y"))
    hist_fc <- fc[fc_years %in% df$year, ]
    comp <- tibble::tibble(
      year = df$year,
      actual = df$sales,
      pred = hist_fc$yhat[match(df$year, as.integer(format(hist_fc$ds, "%Y")))]
    ) %>%
      dplyr::mutate(error = pred - actual)
    plot_ly(comp, x = ~error, type = "histogram", nbinsx = min(12, nrow(comp))) %>%
      layout(
        xaxis = list(title = "예측 - 실제 (억 원 단위)", tickformat = "~s"),
        yaxis = list(title = "빈도"),
        margin = list(t = 20)
      )
  })
}
