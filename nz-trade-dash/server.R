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

  output$fin_ts_plot <- renderPlotly({
    df <- fin_combined_df()
    fin_validate(fin_need(nrow(df) > 0, "데이터를 불러오세요"))
    plot_ly(df, x = ~year, y = ~sales, color = ~source, type = "scatter", mode = "lines+markers") %>%
      layout(
        xaxis = list(title = "연도", dtick = 1),
        yaxis = list(title = "매출액", tickformat = "~s")
      )
  })

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
