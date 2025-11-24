library(shiny)
library(bslib)
library(dplyr)
library(purrr)
library(readr)
library(readxl)
library(httr)
library(xml2)
library(plotly)
library(prophet)
library(janitor)
library(jsonlite)
library(stringr)
library(scales)
library(utils)
library(zip)

utils::globalVariables(c(
  "corp_name", "stock_code", "year", "sales", "inventory",
  "net_income", "total_assets", "cogs", "ds", "yhat"
))

# ---------------------------
# Theme and layout
# ---------------------------
theme <- bs_theme(
  version = 5,
  bootswatch = "flatly",
  bg = "#f7f9fb",
  fg = "#1f2430",
  primary = "#0b6efd",
  secondary = "#6c757d"
)

# ---------------------------
# Helper functions
# ---------------------------
load_api_key <- function() {
  read_env_key <- function(path) {
    if (!file.exists(path)) return(NULL)
    lines <- readLines(path, warn = FALSE)
    lines <- trimws(lines)
    lines <- lines[nzchar(lines) & !startsWith(lines, "#")]
    if (length(lines) == 0) return(NULL)
    kv <- strsplit(lines, "=", fixed = TRUE)
    kv <- Filter(function(x) length(x) == 2, kv)
    if (length(kv) == 0) return(NULL)
    env_list <- setNames(trimws(vapply(kv, `[`, character(1), 2)), trimws(vapply(kv, `[`, character(1), 1)))
    for (nm in c("DART_API_KEY", "DART_KEY", "DART_API")) {
      if (!is.null(env_list[[nm]]) && nzchar(env_list[[nm]])) return(env_list[[nm]])
    }
    NULL
  }

  pick_env <- function() {
    for (nm in c("DART_API_KEY", "DART_KEY", "DART_API")) {
      val <- Sys.getenv(nm)
      if (nzchar(val)) return(val)
    }
    NULL
  }

  key <- pick_env()
  if (nzchar(key)) return(key)

  if (requireNamespace("dotenv", quietly = TRUE)) {
    try(dotenv::load_dotenv(), silent = TRUE)
    key <- pick_env()
    if (nzchar(key)) return(key)
  }

  # fallback: manual parse .env if dotenv 패키지 미설치/미로딩
  key <- read_env_key(".env")
  if (nzchar(key)) return(key)
  NULL
}

http_get_with_retry <- function(url, params = list(), timeout = 40) {
  resp <- GET(url, query = params, timeout(timeout))
  if (resp$status_code == 429) {
    retry_after <- as.numeric(headers(resp)[["retry-after"]])
    Sys.sleep(ifelse(is.na(retry_after), 2, retry_after + runif(1, 0, 0.5)))
    resp <- GET(url, query = params, timeout(timeout))
  }
  stop_for_status(resp)
  resp
}

safe_num <- function(x) {
  if (is.null(x) || length(x) == 0) return(NA_real_)
  as.numeric(gsub("[^0-9\\.\\-]", "", x))
}

dart_single_acnt <- function(api_key, corp_code, year, reprt_code = "11014") {
  url <- "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"
  resp <- http_get_with_retry(
    url,
    params = list(
      crtfc_key = api_key,
      corp_code = corp_code,
      bsns_year = year,
      reprt_code = reprt_code
    ),
    timeout = 40
  )
  txt <- content(resp, "text", encoding = "UTF-8")
  js <- fromJSON(txt, simplifyVector = TRUE)
  if (is.null(js$status) || js$status != "000") {
    msg <- if (!is.null(js$message)) js$message else "DART 응답 오류"
    stop(msg)
  }
  if (is.null(js$list) || length(js$list) == 0) stop("DART 응답에 계정 목록이 없습니다.")
  as_tibble(js$list)
}

extract_accounts <- function(acct_tbl) {
  if (is.null(acct_tbl) || nrow(acct_tbl) == 0) {
    return(tibble(sales = NA_real_, inventory = NA_real_, net_income = NA_real_, total_assets = NA_real_, cogs = NA_real_))
  }
  pick_first <- function(names) {
    hit <- acct_tbl %>% filter(.data$account_nm %in% names) %>% slice_head(n = 1)
    if (nrow(hit) == 0) return(NA_real_)
    safe_num(hit$thstrm_amount)
  }
  tibble(
    sales = pick_first(c("매출액", "수익(매출액)", "수익")),
    inventory = pick_first(c("재고자산")),
    net_income = pick_first(c("당기순이익", "당기순이익(손실)")),
    total_assets = pick_first(c("자산총계")),
    cogs = pick_first(c("매출원가"))
  )
}

fetch_dart_financials <- function(api_key, corp_code, corp_name, years) {
  res <- map(years, function(y) {
    tryCatch({
      accts <- dart_single_acnt(api_key, corp_code, y)
      vals <- extract_accounts(accts)
      mutate(vals, year = y, source = corp_name)
    }, error = function(e) {
      NULL
    })
  })
  res <- compact(res)
  if (length(res) == 0) stop("DART 재무제표를 가져오지 못했습니다.")
  bind_rows(res) %>%
    select(.data$year, .data$sales, .data$inventory, .data$net_income, .data$total_assets, .data$cogs, .data$source)
}

fetch_corp_codes_cached <- function(api_key) {
  url <- "https://opendart.fss.or.kr/api/corpCode.xml"
  resp <- http_get_with_retry(url, params = list(crtfc_key = api_key))
  ctype <- headers(resp)[["content-type"]]
  if (!ctype %in% c("application/zip", "application/x-zip-compressed")) {
    stop("DART API 응답이 ZIP 파일이 아닙니다. API KEY를 확인하세요.")
  }
  tf <- tempfile(fileext = ".zip")
  writeBin(content(resp, "raw"), tf)
  xml_file <- NULL
  files <- basename(zip::zip_list(tf)$filename)
  pick <- files[str_detect(tolower(files), "corpcode")]
  if (length(pick)) xml_file <- pick[[1]]
  if (is.null(xml_file)) {
    xml_file <- "CORPCODE.xml"
  }
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

find_corp_codes_path <- function() {
  for (p in c("corp_codes.csv", file.path("nz-trade-dash", "corp_codes.csv"))) {
    if (file.exists(p)) return(p)
  }
  NULL
}

get_corp_codes <- function(api_key, path = "corp_codes.csv") {
  if (file.exists(path)) {
    try({
      df <- read_csv(path, show_col_types = FALSE, locale = locale(encoding = "UTF-8"))
      return(df %>% mutate(across(everything(), as.character)))
    }, silent = TRUE)
  }
  if (!nzchar(api_key)) stop("API Key 없음")
  fetch_corp_codes_cached(api_key)
}

demo_corp_codes <- function() {
  tibble(
    corp_code = c("00000001", "00000002", "00000003"),
    corp_name = c("한섬", "신세계인터내셔날", "코오롱인더스트리"),
    stock_code = c("020000", "031430", "120110")
  )
}

norm_name <- function(s) {
  if (is.null(s)) return("")
  s <- stringi::stri_trans_nfkc(as.character(s))
  s <- tolower(gsub("[\\s\\-\\_\\.\\&\\/\\(\\)\\[\\],\\+]+", "", s))
  s <- str_replace_all(s, "주식회사|\\(주\\)|㈜", "")
  s
}

alias_map <- c(
  "f&f" = "에프앤에f",
  "ff" = "에프앤에f",
  "fnf" = "에프앤에프",
  "thehandsome" = "한섬",
  "handsome" = "한섬",
  "shinsegaeinternational" = "신세계인터내셔날",
  "shinsegaeintl" = "신세계인터내셔날",
  "kolonindustries" = "코오롱인더스트리"
)

make_corp_choices <- function(df) {
  if (is.null(df) || nrow(df) == 0) return(setNames(character(), character()))
  labels <- ifelse(
    !is.na(df$stock_code) & nzchar(df$stock_code),
    paste0(df$corp_name, " (", df$stock_code, ")"),
    df$corp_name
  )
  setNames(df$corp_code, labels)
}

search_corp_smart <- function(corp_df, query, limit = 30) {
  if (is.null(query) || query == "") return(head(corp_df, limit))
  q <- trimws(query)
  if (!nzchar(q)) return(head(corp_df, limit))
  q_norm <- norm_name(q)
  if (str_detect(q, "^\\d{6}$")) {
    hit <- corp_df %>% filter(.data$stock_code == q)
    if (nrow(hit)) return(head(hit, limit))
  }
  alias <- alias_map[[q_norm]]
  tokens <- if (!is.null(alias)) {
    norm_name(alias)
  } else {
    tks <- str_split(q, "\\s+")[[1]]
    tks <- tks[nzchar(tks)]
    if (length(tks) == 0) q_norm else map_chr(tks, norm_name)
  }
  norm_series <- corp_df %>% mutate(norm_name = map_chr(.data$corp_name, norm_name))
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

read_upload_df <- function(path) {
  ext <- tools::file_ext(path) %>% tolower()
  if (ext %in% c("xlsx", "xls")) return(read_excel(path) %>% clean_names())
  read_csv(path, show_col_types = FALSE) %>% clean_names()
}

guess_col <- function(cols, patterns) {
  hit <- which(str_detect(cols, regex(paste(patterns, collapse = "|"), ignore_case = TRUE)))[1]
  if (length(hit) && !is.na(hit)) cols[[hit]] else cols[[1]]
}

sample_dart_financials <- function(corp_name = "상장사", years = 2019:2023) {
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

sample_my_company <- function(years = 2019:2023) {
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

safe_prophet <- function(df, horizon) {
  req(nrow(df) > 2)
  m <- prophet(df %>% transmute(ds = as.Date(paste0(.data$year, "-12-31")), y = .data$sales))
  future <- make_future_dataframe(m, periods = horizon, freq = "year")
  predict(m, future) %>%
    transmute(year = as.integer(format(.data$ds, "%Y")), yhat = .data$yhat)
}

calc_metrics <- function(df) {
  df %>%
    mutate(
      inventory_turnover = if_else(
        !is.na(.data$cogs) & !is.na(.data$inventory) & .data$inventory != 0,
        .data$cogs / .data$inventory,
        NA_real_
      ),
      roa = if_else(
        !is.na(.data$net_income) & !is.na(.data$total_assets) & .data$total_assets != 0,
        .data$net_income / .data$total_assets,
        NA_real_
      )
    )
}

empty_fin_df <- tibble(
  year = integer(),
  sales = numeric(),
  inventory = numeric(),
  net_income = numeric(),
  total_assets = numeric(),
  cogs = numeric(),
  source = character()
)

# ---------------------------
# UI
# ---------------------------
ui <- page_sidebar(
  fillable = TRUE,
  header = tags$head(tags$style(HTML("
    .app-hero {
      background: linear-gradient(120deg, #0b6efd 0%, #5ac8fa 100%);
      color: #fff;
      border-radius: 14px;
      padding: 18px 20px;
      box-shadow: 0 10px 30px rgba(11,110,253,0.15);
      margin-bottom: 16px;
    }
    .app-hero h4 { margin: 0 0 6px 0; }
    .app-hero .sub { opacity: 0.85; margin: 0; }
    .card.bg-primary, .card.bg-success, .card.bg-info {
      box-shadow: 0 8px 18px rgba(0,0,0,0.08);
      border: 0;
    }
    .nav-card-tabs .nav-link.active { font-weight: 600; }
    .sidebar { background: #fff; box-shadow: 0 8px 18px rgba(0,0,0,0.06); }
  "))),
  sidebar = sidebar(
    title = "📊 1:1 재무 벤치마킹",
    textInput("corp_query", "상장사 검색", placeholder = "예: 한섬, 020000"),
    actionButton("corp_search", "검색"),
    selectInput("corp_pick", "상장사 선택", choices = c(), selected = NULL),
    actionButton("fetch_dart", "DART 불러오기"),
    actionButton("load_demo", "데모 데이터 로드"),
    hr(),
    fileInput("upload", "내 가게 파일 업로드", accept = c(".xlsx", ".xls", ".csv")),
    uiOutput("mapping_ui"),
    hr(),
    numericInput("forecast_y", "예측 연도 수", value = 3, min = 1, max = 5),
    actionButton("do_forecast", "예측 실행", class = "btn-primary")
  ),
  navset_card_tab(
    nav_panel(
      "요약",
      div(
        class = "app-hero",
        h4("📊 1:1 재무 벤치마킹"),
        p(class = "sub", "상장사 vs 내 가게: 요약 지표와 추이를 한눈에")
      ),
      uiOutput("kpi_row"),
      plotlyOutput("ts_plot")
    ),
    nav_panel("개요", plotlyOutput("ts_plot")),
    nav_panel("4분면", plotlyOutput("quad_plot")),
    nav_panel("예측", plotlyOutput("fc_plot"), tableOutput("fc_table"))
  ),
  theme = theme,
  title = "재무 벤치마킹 대시보드 (Shiny)"
)

# ---------------------------
# Server
# ---------------------------
server <- function(input, output, session) {
  values <- reactiveValues(
    corp_df = NULL,
    df_dart = NULL,
    df_my = NULL,
    df_my_norm = NULL,
    corp_real_loaded = FALSE
  )

  # 초기에는 데모 리스트로 설정
  observe({
    if (is.null(values$corp_df)) {
      corp_path <- find_corp_codes_path()
      if (!is.null(corp_path)) {
        values$corp_df <- tryCatch(
          get_corp_codes("", corp_path),
          error = function(e) {
            showNotification("corp_codes.csv 읽기 실패: 데모 리스트로 대체합니다.", type = "error", duration = 6)
            demo_corp_codes()
          }
        )
        values$corp_real_loaded <- TRUE
      } else {
        values$corp_df <- demo_corp_codes()
      }
    }
  })

  observeEvent(values$corp_df, {
    df <- values$corp_df
    if (is.null(df) || nrow(df) == 0) return()
    choices <- make_corp_choices(head(df, 15))
    updateSelectInput(session, "corp_pick", choices = choices, selected = choices[[1]])
  })

  # Search and pick corp
  observeEvent(input$corp_search, {
    query <- if (is.null(input$corp_query)) "" else trimws(input$corp_query)
    if (!nzchar(query)) {
      showNotification("검색어를 입력하세요.", type = "warning", duration = 3)
      return()
    }

    withProgress(message = "검색 중...", value = 0.1, {
      # 필요 시 실제 corpCode를 한번만 가져오기
      if (!values$corp_real_loaded) {
        corp_path <- find_corp_codes_path()
        if (!is.null(corp_path)) {
          setProgress(0.2, detail = "로컬 corp_codes.csv 읽는 중")
          values$corp_df <- tryCatch(
            get_corp_codes("", corp_path),
            error = function(e) {
              showNotification("corp_codes.csv 읽기 실패: 데모 리스트로 대체합니다.", type = "error", duration = 6)
              demo_corp_codes()
            }
          )
          values$corp_real_loaded <- TRUE
        } else {
          api_key <- load_api_key()
          if (!is.null(api_key)) {
            withProgress(message = "DART 기업 리스트 불러오는 중...", value = 0.3, {
              values$corp_df <- tryCatch(
                get_corp_codes(api_key),
                error = function(e) {
                  showNotification("DART corpCode 조회 실패: 데모 리스트로 대체합니다.", type = "error", duration = 6)
                  demo_corp_codes()
                }
              )
              values$corp_real_loaded <- TRUE
            })
          } else {
            showNotification("DART_API_KEY가 없어 데모 리스트를 사용합니다.", type = "warning", duration = 5)
          }
        }
      }

      df <- values$corp_df
      if (is.null(df) || nrow(df) == 0) {
        showNotification("기업 리스트가 없습니다. 데모/키 설정을 확인하세요.", type = "error", duration = 5)
        return()
      }
      setProgress(0.6, detail = "이름/종목코드 필터링 중")
      hits <- search_corp_smart(df, query)
      hits <- hits %>%
        arrange(desc(!is.na(.data$stock_code) & nzchar(.data$stock_code)), .data$corp_name)
      if (nrow(hits) == 0) {
        showNotification("검색 결과가 없습니다.", type = "warning", duration = 4)
        return()
      }
      choices <- make_corp_choices(hits)
      updateSelectInput(session, "corp_pick", choices = choices, selected = choices[[1]])
      showNotification(paste0("검색 결과 ", length(choices), "건을 찾았습니다."), type = "message", duration = 3)
      setProgress(1)
    })
  })

  # Fetch DART data (placeholder uses sample data)
  observeEvent(input$fetch_dart, {
    selected_code <- if (!is.null(input$corp_pick) && nzchar(input$corp_pick)) input$corp_pick else NA_character_
    if (is.na(selected_code)) {
      showNotification("상장사를 먼저 선택하세요.", type = "warning", duration = 4)
      return()
    }
    corp_nm <- "상장사"
    if (!is.null(values$corp_df)) {
      picked <- values$corp_df %>%
        filter(.data$corp_code == selected_code | .data$corp_name == selected_code) %>%
        slice_head(n = 1)
      if (nrow(picked)) {
        corp_nm <- picked$corp_name
        selected_code <- picked$corp_code
      }
    }

    api_key <- load_api_key()
    if (is.null(api_key) || !nzchar(api_key)) {
      showNotification("DART_API_KEY를 설정하세요. 데모 데이터를 사용합니다.", type = "warning", duration = 5)
      values$df_dart <- sample_dart_financials(corp_name = paste0(corp_nm, "(데모)"))
      return()
    }

    years <- seq(as.integer(format(Sys.Date(), "%Y")) - 4, as.integer(format(Sys.Date(), "%Y")))
    withProgress(message = paste0("DART 재무제표 불러오는 중: ", corp_nm), value = 0.2, {
      df <- tryCatch(
        fetch_dart_financials(api_key, selected_code, corp_nm, years),
        error = function(e) {
          showNotification(paste0("DART 불러오기 실패: ", e$message), type = "error", duration = 6)
          NULL
        }
      )
      if (is.null(df) || nrow(df) == 0) {
        values$df_dart <- sample_dart_financials(corp_name = paste0(corp_nm, "(데모)"))
      } else {
        values$df_dart <- df
        showNotification("DART 데이터가 업데이트되었습니다.", type = "message", duration = 4)
      }
      setProgress(1)
    })
  })

  # Load demo data for quick testing
  observeEvent(input$load_demo, {
    values$df_dart <- sample_dart_financials(corp_name = "상장사(데모)")
    values$df_my_norm <- sample_my_company()
    showNotification("데모 데이터가 로드되었습니다.", type = "message", duration = 4)
  })

  # Upload my company data and build mapping UI
  observeEvent(input$upload, {
    req(input$upload$datapath)
    df <- read_upload_df(input$upload$datapath)
    cols <- names(df)
    year_col <- guess_col(cols, c("year", "년도"))
    sales_col <- guess_col(cols, c("sale", "매출"))
    inv_col <- guess_col(cols, c("inv", "재고"))
    output$mapping_ui <- renderUI({
      tagList(
        selectInput("col_year", "연도 컬럼", choices = cols, selected = year_col),
        selectInput("col_sales", "매출액 컬럼", choices = cols, selected = sales_col),
        selectInput("col_inventory", "재고자산 컬럼", choices = cols, selected = inv_col),
        selectInput("col_net_income", "당기순이익 컬럼(선택)", choices = c("", cols), selected = ""),
        selectInput("col_assets", "자산총계 컬럼(선택)", choices = c("", cols), selected = ""),
        selectInput("col_cogs", "매출원가 컬럼(선택)", choices = c("", cols), selected = "")
      )
    })
    values$df_my <- df
  })

  # Normalize uploaded data based on mapping
  observeEvent(list(input$col_year, input$col_sales, input$col_inventory,
                    input$col_net_income, input$col_assets, input$col_cogs), {
    req(values$df_my)
    df <- values$df_my
    get_num <- function(col) as.numeric(gsub(",", "", df[[col]]))
    res <- tibble(
      year = df[[input$col_year]],
      sales = get_num(input$col_sales),
      inventory = get_num(input$col_inventory),
      net_income = if (nzchar(input$col_net_income)) get_num(input$col_net_income) else NA_real_,
      total_assets = if (nzchar(input$col_assets)) get_num(input$col_assets) else NA_real_,
      cogs = if (nzchar(input$col_cogs)) get_num(input$col_cogs) else NA_real_,
      source = "My Company"
    ) %>%
      mutate(year = as.integer(.data$year))
    values$df_my_norm <- res
  }, ignoreNULL = FALSE)

  # Combine data
  combined_df <- reactive({
    rows <- list(values$df_dart, values$df_my_norm)
    rows <- lapply(rows, function(x) if (is.null(x)) empty_fin_df else x)
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
      calc_metrics()
  })

  # KPI cards (최근 연도 기준)
  output$kpi_row <- renderUI({
    df <- combined_df()
    validate(need(nrow(df) > 0, "데이터를 불러오세요"))
    latest_year <- max(df$year, na.rm = TRUE)
    latest <- df %>% filter(.data$year == latest_year)
    mk_card <- function(title, value, color = "primary") {
      card(
        class = paste("bg-", color),
        card_body(
          h6(title, class = "text-light mb-1"),
          h4(scales::label_number(scale_cut = scales::cut_short_scale())(value), class = "text-light mb-0")
        )
      )
    }
    layout_column_wrap(
      width = 1 / 3,
      mk_card("최근 연도 매출", sum(latest$sales, na.rm = TRUE), "primary"),
      mk_card("재고자산회전율", mean(latest$inventory_turnover, na.rm = TRUE), "success"),
      mk_card("ROA", mean(latest$roa, na.rm = TRUE), "info")
    )
  })

  # Time series plot
  output$ts_plot <- renderPlotly({
    df <- combined_df()
    validate(need(nrow(df) > 0, "데이터를 불러오세요"))
    plot_ly(df, x = ~year, y = ~sales, color = ~source, type = "scatter", mode = "lines+markers") %>%
      layout(yaxis = list(title = "매출액"))
  })

  # Quadrant plot
  output$quad_plot <- renderPlotly({
    df <- combined_df()
    validate(need(nrow(df) > 0, "데이터를 불러오세요"))
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
        yaxis = list(title = "ROA")
      )
  })

  # Forecast
  observeEvent(input$do_forecast, {
    df_all <- combined_df()
    validate(need(nrow(df_all) > 2, "예측을 위해 최소 3개 연도가 필요합니다."))

    # Prophet은 중복된 날짜를 허용하지 않으므로 단일 소스만 선택
    # 1순위: 선택한 상장사, 2순위: My Company, 3순위: 첫 번째 소스
    chosen_source <- NULL
    # 상장사(데모 포함) 우선
    non_my <- df_all %>% filter(.data$source != "My Company")
    if (nrow(non_my) > 0) chosen_source <- non_my$source[[1]]
    # My Company만 있으면 그걸 사용
    if (is.null(chosen_source) && any(df_all$source == "My Company")) chosen_source <- "My Company"
    # 그래도 없으면 첫 source
    if (is.null(chosen_source)) chosen_source <- df_all$source[[1]]

    df <- df_all %>% filter(.data$source == chosen_source)
    validate(need(nrow(df) > 2, paste0("예측을 위해 ", chosen_source, " 데이터가 최소 3개 연도 필요합니다.")))

    fc <- safe_prophet(df, input$forecast_y)
    output$fc_table <- renderTable(fc)
    output$fc_plot <- renderPlotly({
      plot_ly(fc, x = ~year, y = ~yhat, type = "scatter", mode = "lines+markers", name = "예측") %>%
        add_trace(data = df, x = ~year, y = ~sales, type = "scatter", mode = "markers", name = "실제") %>%
        layout(yaxis = list(title = "매출액"))
    })
  })
}

shinyApp(ui, server)
