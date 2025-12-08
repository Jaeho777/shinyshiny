
## load
library(shiny)
library(shinyjs)
library(shinydashboard)
library(shinycssloaders)
library(DT)
library(highcharter)
library(treemap)
library(timevis)
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
utils::globalVariables(c(
   "dtf_shiny_full",
   "dtf_shiny_commodity_service_ex",
   "dtf_shiny_commodity_service_im",
   "dtf_shiny_country_gs",
   "Country",
   "Year",
   "Type_ie",
   "Type_gs",
   "Value",
   "SNZ_commodity",
   "Commodity",
   "ISO2",
   "lat",
   "lon",
   "Note",
   "CAGR1",
   "CAGR5",
   "CAGR10",
   "CAGR20",
   "Name",
   "Share"
))
source('share_load.R')

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
   # httr::RETRY로 429/5xx 재시도, as.request 에러 방지
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
   reprt_codes <- c("11011", "11014", "11012", "11013") # business, annual, half, quarterly
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
}

## 000 user input setup. Please pay close attention and change -----
## un comtrade max year, you can find it on https://comtrade.un.org/data/da
tmp_un_comtrade_max_year <- year(Sys.time()) - 2 # 2 years of lag

## build server.R
server <- 
   function(input, output, session) {
      ## Header navigation: icons controlling sidebar tab
      observeEvent(input$go_fin, {
         updateTabItems(session, "sidebar", "tab_diagnosis")
      })
      ## I. Main dashboard -----------------------------
      i_prog <- 1
      tot_step <- 25
      
      # 1. Value boxes  ---------------------------------------------------------
      ## try add progress bars
      withProgress(message = 'Loading...', value = (i_prog-1)/tot_step, {
         # Increment the progress bar, and update the detail text.
         incProgress( i_prog/tot_step, detail = NULL)
         ##Sys.sleep(0.1)
         
      })
      i_prog <- i_prog + 1
      
      tmp_ex_g <-
         dtf_shiny_full %>%
         filter( Country == 'World',
                 Year == max(Year),
                 Type_ie == 'Exports',
                 Type_gs == 'Goods'
         ) %>%
         group_by( Year ) %>%
         summarise( Value = round(sum(Value/10^6),0) ) %>%
         dplyr::ungroup() %>%
         dplyr::select(Value) %>%
         as.numeric

      ###
      tmp_ex_s <-
         dtf_shiny_full %>%
         filter( Country == 'World',
                 Year == max(Year),
                 Type_ie == 'Exports',
                 Type_gs == 'Services'
         ) %>%
         group_by( Year ) %>%
         summarise( Value = round(sum(Value/10^6),0) ) %>%
         dplyr::ungroup() %>%
         dplyr::select(Value) %>%
         as.numeric

      ###
      tmp_ex_tot <- tmp_ex_g + tmp_ex_s

      ###
      tmp_im_g <-
         dtf_shiny_full %>%
         filter( Country == 'World',
                 Year == max(Year),
                 Type_ie == 'Imports',
                 Type_gs == 'Goods'
         ) %>%
         group_by( Year ) %>%
         summarise( Value = round(sum(Value/10^6),0) ) %>%
         dplyr::ungroup() %>%
         dplyr::select(Value) %>%
         as.numeric

      ###
      tmp_im_s <-
         dtf_shiny_full %>%
         filter( Country == 'World',
                 Year == max(Year),
                 Type_ie == 'Imports',
                 Type_gs == 'Services'
         ) %>%
         group_by( Year ) %>%
         summarise( Value = round(sum(Value/10^6),0) ) %>%
         dplyr::ungroup() %>%
         dplyr::select(Value) %>%
         as.numeric

      ###
      tmp_im_tot <- tmp_im_g + tmp_im_s

      ###
      tmp_balance_g <- tmp_ex_g - tmp_im_g
      tmp_balance_s <- tmp_ex_s - tmp_im_s
      tmp_balance_tot <- tmp_balance_g + tmp_balance_s
      
      ## build GODDS value boxes
      output$ExGBox <- renderValueBox({
         valueBox(
            VB_style( paste0( '$',format(tmp_ex_g,big.mark=','), " m" ),  "font-size: 60%;"  ),
            VB_style( paste0("Goods exports (", round(tmp_ex_g/tmp_ex_tot*100,0) ,"%)")  ), 
            icon = icon('export', lib = 'glyphicon'), #icon("sign-in"),
            color = "green"
         )
      })
      
      ###
      output$ImGBox <- renderValueBox({
         valueBox(
            VB_style( paste0( '$', format(tmp_im_g, big.mark = ','), " m"),  "font-size: 60%;"  ),
            paste0("Goods imports (", round(tmp_im_g/tmp_im_tot*100,0) ,"%)"), 
            icon = icon('import', lib = 'glyphicon'),# icon("sign-out"),
            color = "red"
         )
      })
      
      ###
      output$BlGBox <- renderValueBox({
         valueBox(
            VB_style( paste0( ifelse( tmp_balance_g>0, '+', '-' ), '$', format(abs(tmp_balance_g),big.mark=','), " m"),  "font-size: 60%;"  ),
            "Goods balance", 
            icon = icon("balance-scale"),
            color = ifelse( tmp_balance_g>0, 'green', 'red' )
         )
      })
      
      ## build Services value boxes
      output$ExSBox <- renderValueBox({
         valueBox(
            VB_style( paste0( '$', format(tmp_ex_s,big.mark=','), " m"), "font-size: 60%;"  ),
            paste0("Services exports (", round(tmp_ex_s/tmp_ex_tot*100,0) ,"%)"), 
            icon = icon('export', lib = 'glyphicon'),#icon("sign-in"),
            color = "green"
         )
      })
      
      ###
      output$ImSBox <- renderValueBox({
         valueBox(
            VB_style( paste0( '$',format(tmp_im_s, big.mark = ','), " m"),"font-size: 60%;"  ),
            paste0("Services imports (", round(tmp_im_s/tmp_im_tot*100,0) ,"%)"), 
            icon = icon('import', lib = 'glyphicon'), #icon("sign-out"),
            color = "red"
         )
      })
      
      ###
      output$BlSBox <- renderValueBox({
         valueBox(
            VB_style( paste0( ifelse( tmp_balance_s>0, '+', '-' ),'$',format(abs(tmp_balance_s),big.mark=','), " m"), "font-size: 60%;"  ),
            "Services balance", 
            icon = icon("balance-scale"),
            color = ifelse( tmp_balance_s>0, 'green', 'red' )
         )
      })
      
      ## build Total trade value boxes
      output$ExTotBox <- renderValueBox({
         valueBox(
            VB_style( paste0( '$',format(tmp_ex_tot,big.mark=','), " m"), "font-size: 60%;"  ),
            "Total exports", 
            icon = icon('export', lib = 'glyphicon'), # icon("sign-in"),
            color = "green"
         )
      })
      
      ###
      output$ImTotBox <- renderValueBox({
         valueBox(
            VB_style( paste0( '$',format(tmp_im_tot, big.mark = ','), " m"),"font-size: 60%;"  ),
            "Total imports", 
            icon = icon('import', lib = 'glyphicon'), #icon("sign-out"),
            color = "red"
         )
      })
      
      ###
      output$BlTotBox <- renderValueBox({
         valueBox(
            VB_style( paste0( ifelse( tmp_balance_tot>0, '+', '-' ),'$', format(abs(tmp_balance_tot),big.mark=','), " m"),"font-size: 60%;"  ),
            "Trade balance", 
            icon = icon("balance-scale"),
            color = ifelse( tmp_balance_tot>0, 'green', 'red' )
         )
      })
      
      
      # 2. Total Trade a line chart  -----------------------------------------------------------------
      withProgress(message = 'Loading...', value = (i_prog-1)/tot_step, {
         # Increment the progress bar, and update the detail text.
         incProgress( i_prog/tot_step, detail = NULL)
         ##Sys.sleep(0.1)
         
      })
      i_prog <- i_prog + 1
      
      tmp_dtf <-
         dtf_shiny_full %>%
         filter( Country == 'World',
                 #Type_ie == 'Imports',
                 Year >= (max(Year) - 20) ) %>%
         mutate( Value = round(Value/10^6) )

      output$IEGSLineHc <-renderHighchart({
         highchart() %>%
            hc_exporting(enabled = TRUE, formAttributes = list(target = "_blank")) %>%
            hc_chart(type = 'line') %>%
            hc_series( list(name = 'Goods exports', data =tmp_dtf$Value[tmp_dtf$Type_gs=='Goods'&tmp_dtf$Type_ie=='Exports'], color='green', marker = list(symbol = 'circle') ),
                       list(name = 'Services exports', data =tmp_dtf$Value[tmp_dtf$Type_gs=='Services'&tmp_dtf$Type_ie=='Exports'], color = 'green', dashStyle = 'shortDot', marker = list(symbol = 'triangle') ),
                       list(name = 'Goods imports', data =tmp_dtf$Value[tmp_dtf$Type_gs=='Goods'&tmp_dtf$Type_ie=='Imports'], color = 'red', marker = list(symbol = 'circle') ),
                       list(name = 'Services imports', data =tmp_dtf$Value[tmp_dtf$Type_gs=='Services'&tmp_dtf$Type_ie=='Imports'], color = 'red', dashStyle = 'shortDot', marker = list(symbol = 'triangle')  )
            )%>%
            hc_xAxis( categories = unique(tmp_dtf$Year) ) %>%
            hc_yAxis( title = list(text = "$ million, NZD"),
                      labels = list( format = "${value:,.0f} m")  ) %>%
            hc_plotOptions(column = list(
               dataLabels = list(enabled = F),
               #stacking = "normal",
               enableMouseTracking = T ) 
            )%>%
            hc_tooltip(table = TRUE,
                       sort = TRUE,
                       pointFormat = paste0( '<br> <span style="color:{point.color}">\u25CF</span>',
                                             " {series.name}: ${point.y} m"),
                       headerFormat = '<span style="font-size: 13px">Year {point.key}</span>'
            ) %>%
            hc_legend( layout = 'vertical', align = 'left', verticalAlign = 'top', floating = T, x = 100, y = 000 )
      })
      
      # 2.1 Total Trade balance a line chart  -----------------------------------------------------------------
      withProgress(message = 'Loading...', value = (i_prog-1)/tot_step, {
         # Increment the progress bar, and update the detail text.
         incProgress( i_prog/tot_step, detail = NULL)
         ##Sys.sleep(0.1)
         
      })
      i_prog <- i_prog + 1
      
      tmp_dtf_balance <-
         dtf_shiny_full %>%
         filter( Country == 'World',
                 #Type_ie == 'Imports',
                 Year >= (max(Year) - 20) ) %>%
         group_by( Year, Country, Type_gs ) %>%
         mutate( Value = Value[ Type_ie == 'Exports'] - Value[ Type_ie == 'Imports']  ) %>%
         ungroup %>%
         filter( Type_ie == 'Exports' ) %>%
         mutate(  Type_gs = paste0(Type_gs, ' balance') )
      
      tmp_dtf_balance_tot <-
         tmp_dtf_balance %>%
         group_by( Year, Country, Type_ie ) %>%
         summarise( Value = sum(Value, na.rm=T) ) %>%
         ungroup %>%
         mutate( Type_gs = 'Trade balance' )
      
      tmp_dtf_balance %<>%
         bind_rows( tmp_dtf_balance_tot  ) %>%
         mutate( Value = round(Value/10^6) )
      
      output$GSTotalBalanceLineHc <-renderHighchart({
         highchart() %>%
            hc_exporting(enabled = TRUE, formAttributes = list(target = "_blank")) %>%
            hc_chart(type = 'line') %>%
            hc_series( list(name = 'Trade balance', data =tmp_dtf_balance$Value[tmp_dtf_balance$Type_gs=='Trade balance'], color='brown' , marker = list(enabled = F), lineWidth = 3 ),
                       list(name = 'Goods balance', data =tmp_dtf_balance$Value[tmp_dtf_balance$Type_gs=='Goods balance'], color = 'darkgreen', dashStyle = 'shortDot', marker = list(symbol = 'circle') ),
                       list(name = 'Services balance', data =tmp_dtf_balance$Value[tmp_dtf_balance$Type_gs=='Services balance'], color = 'darkblue', dashStyle = 'shortDot',  marker = list(symbol = 'triangle') )
            )%>%
            hc_xAxis( categories = unique(tmp_dtf_balance$Year) ) %>%
            hc_yAxis( title = list(text = "$ million, NZD"),
                      labels = list( format = "${value:,.0f} m"),
                      plotLines = list(
                         list(#label = list(text = "This is a plotLine"),
                            color = "#ff0000",
                            #dashStyle = 'shortDot',
                            width = 2,
                            value = 0 ) )
                      ) %>%
            hc_plotOptions(column = list(
               dataLabels = list(enabled = F),
               #stacking = "normal",
               enableMouseTracking = T ) 
            )%>%
            hc_tooltip(table = TRUE,
                       sort = TRUE,
                       pointFormat = paste0( '<br> <span style="color:{point.color}">\u25CF</span>',
                                             " {series.name}: ${point.y} m"),
                       headerFormat = '<span style="font-size: 13px">Year {point.key}</span>'
            ) %>%
            hc_legend( layout = 'vertical', align = 'left', verticalAlign = 'top', floating = T, x = 100, y = 000 )
      })


      # 3. Growth prospective ---------------------------------------------------
      withProgress(message = 'Loading...', value = (i_prog-1)/tot_step, {
         # Increment the progress bar, and update the detail text.
         incProgress( i_prog/tot_step, detail = NULL)
         ##Sys.sleep(0.1)
         
      })
      i_prog <- i_prog + 1
      
      tmp_tot <-
         tmp_dtf %>%
         group_by( Year, Type_ie ) %>%
         summarise( Value = sum(Value,na.rm=T) ) %>%
         ungroup( ) %>%
         mutate( Name = paste0('Total', ' ', tolower(Type_ie)) )

      tmp_tab <-
         tmp_dtf %>%
         mutate( Name = paste0( Type_gs,' ', tolower(Type_ie) ) ) %>%
         bind_rows( tmp_tot ) %>%
         group_by( Name) %>%
         mutate( CAGR1 = CAGR( Value[Year == max(Year)]/
                                  Value[Year == (max(Year)-1)], 1)/100,
                 CAGR5 = CAGR( Value[Year == max(Year)]/
                                  Value[Year == (max(Year)-5)], 5)/100,
                 CAGR10 = CAGR( Value[Year == max(Year)]/
                                   Value[Year == (max(Year)-10)], 10)/100,
                 CAGR20 = CAGR( Value[Year == max(Year)]/
                                   Value[Year == (max(Year)-20)], 20)/100
         ) %>%
         ungroup %>%
         filter( Year == max(Year) ) %>%
         dplyr::select( Name, Value, CAGR1, CAGR5, CAGR10, CAGR20 ) %>%
         mutate( Name = factor(Name, levels = c("Total exports",
                                       'Goods exports',
                                       'Services exports',
                                       'Total imports',
                                       'Goods imports',
                                       'Services imports')) ) %>%
         arrange( Name )


      output$GrowthTab <- renderDataTable({
         datatable( tmp_tab,
                    rownames = F,
                    extensions = 'Buttons',
                    options = list(dom = 'Bt', 
                                   buttons = c('copy', 'csv', 'excel', 'pdf', 'print'),
                                   scrollX = TRUE) ,
                    colnames=c(" ", 'Value ($m)', 'CAGR 1', 'CAGR 5', 'CAGR 10', 'CAGR 20')
                   ) %>%
            formatStyle(columns = 'Name',
                        target = 'row',
                        fontWeight = styleEqual(c('Total imports','Total exports'), c('bold','bold')),
                        backgroundColor = styleEqual(c('Total imports','Total exports'), c('lightgrey','lightgrey'))
                       ) %>%
            formatStyle(
               c('CAGR1', 'CAGR5', 'CAGR10', 'CAGR20'),
               background = styleColorBar( c(0,max(c(tmp_tab$CAGR1,tmp_tab$CAGR5, tmp_tab$CAGR10, tmp_tab$CAGR20))*2) , 'lightblue'),
               backgroundSize = '100% 90%',
               backgroundRepeat = 'no-repeat',
               backgroundPosition = 'center'
            ) %>%
            formatPercentage( c('CAGR1', 'CAGR5', 'CAGR10', 'CAGR20'),digit = 1 ) %>%
            formatStyle( columns = c('Name','Value','CAGR1', 'CAGR5', 'CAGR10', 'CAGR20'), `font-size`= '115%' ) %>%
            formatCurrency( columns = c('Value'), mark = " ", digits = 0)
      })


      ## remove the waiting message -- 
      removeUI( selector = '#main_wait_message' )
      
      # 7.10  Show more button --------------------
      observeEvent( input$btn_show_more,
                    {
                       
                       ## disable the buttone ---
                       shinyjs::disable("btn_show_more")
                       ## --- hide message to show more -----
                       shinyjs::hide(id = 'message_to_show_more')
                       ## --- show loading message ---
                       shinyjs::show( id = "load_more_message" )
                       
                       # 4. Treemap key export commodity and services ------------------------------------
                       withProgress(message = 'Loading...', value = (i_prog-1)/tot_step, {
                          # Increment the progress bar, and update the detail text.
                          incProgress( i_prog/tot_step, detail = NULL)
                          ##Sys.sleep(0.1)
                          
                       })
                       i_prog <- i_prog + 1
                       
                       ## commodity concordance is download from http://tariffdata.wto.org/ReportersAndProducts.aspx
                       tmp_tm_ex <- treemap( dtf_shiny_commodity_service_ex %>%
                                                filter(Year==max(Year)) %>%
                                                mutate( Value = Value/10^6) ,
                                             index = c("Type_gs", "SNZ_commodity"),
                                             vSize = "Value",
                                             vColor = "CAGR5",
                                             type = 'value',
                                             #aspRatio = 1.618,
                                             overlap.labels = 1,
                                             fun.aggregate = "weighted.mean",
                                             #palette = "RdYlGn",
                                             draw = FALSE)
                       
                       ## modify Goods and Service CAGR 5 change
                       tmp_tm_ex$tm$vColor[tmp_tm_ex$tm$vSize == tmp_ex_g] <-
                          tmp_tm_ex$tm$vColorValue[tmp_tm_ex$tm$vSize == tmp_ex_g] <- tmp_tab$CAGR5[tmp_tab$Name =='Goods exports']*100
                       
                       tmp_tm_ex$tm$vColor[tmp_tm_ex$tm$vSize == tmp_ex_s] <-
                          tmp_tm_ex$tm$vColorValue[tmp_tm_ex$tm$vSize == tmp_ex_s] <- tmp_tab$CAGR5[tmp_tab$Name =='Services exports']*100
                       
                       ## not highlight confidential data
                       #tmp_tm$tm$vColor[tmp_tm$tm$SNZ_commodity == 'Confidential data'] <-
                       #   tmp_tm$tm$vColorValue[tmp_tm$tm$SNZ_commodity == 'Confidential data'] <-
                       #   tmp_tm$tm$color[tmp_tm$tm$SNZ_commodity == 'Confidential data'] <- NA
                       
                       output$KeyExTM <- renderHighchart({
                          highchart() %>%
                             hc_add_series_treemap2( tmp_tm_ex , #hctreemap
                                                     allowDrillToNode = TRUE,
                                                     layoutAlgorithm = "squarified",
                                                     levelIsConstant = FALSE,
                                                     levels = list(list(level = 1,
                                                                        dataLabels = list(enabled = TRUE,
                                                                                          style = list(fontSize = '20px', color = 'white',
                                                                                                       fontWeight = 'normal'),
                                                                                          backgroundColor = 'lightgrey',
                                                                                          align = 'left', verticalAlign = 'top'),
                                                                        borderColor = "#555",
                                                                        borderWidth = 2 ),
                                                                   list(level = 2,
                                                                        dataLabels = list(enabled = TRUE,
                                                                                          style = list(fontSize = '9px',
                                                                                                       fontWeight = 'normal')
                                                                        )
                                                                   )
                                                     )
                             ) %>%
                             hc_chart(backgroundColor = NULL, plotBorderColor = "#555", plotBorderWidth = 2) %>%
                             hc_title(text = "key commodities and services EXPORTS") %>%
                             hc_subtitle(text = "Coloured by compound annual growth rate (CAGR) for the past 5 years (%)") %>%
                             hc_exporting(enabled = TRUE, formAttributes = list(target = "_blank")) %>%
                             hc_tooltip(pointFormat = "<b>{point.name}</b>:<br>
                                        Export value: ${point.value:,.0f} m <br>
                                        CAGR 5: {point.colorValue:,.1f}%") %>% 
                             hc_colorAxis(minColor = tmp_tm_ex$tm$color[which.min(tmp_tm_ex$tm$vColorValue)],
                                          maxColor = tmp_tm_ex$tm$color[which.max(tmp_tm_ex$tm$vColorValue)] ,
                                          labels = list(format = "{value}%", useHTML = TRUE), reversed = FALSE
                             ) %>%
                             hc_legend(align = "right", layout = "vertical", verticalAlign = "top",
                                       reversed = TRUE , y = 70, symbolHeight = 250, itemMarginTop = 10)
                       })
                       
                       
                       # 4.2 Treemap key import commodity and services ------------------------------------
                       withProgress(message = 'Loading...', value = (i_prog-1)/tot_step, {
                          # Increment the progress bar, and update the detail text.
                          incProgress( i_prog/tot_step, detail = NULL)
                          ##Sys.sleep(0.1)
                          
                       })
                       i_prog <- i_prog + 1
                       
                       ## commodity concordance is download from http://tariffdata.wto.org/ReportersAndProducts.aspx
                       tmp_tm_im <- treemap( dtf_shiny_commodity_service_im %>%
                                                filter(Year==max(Year)) %>%
                                                mutate( Value = Value/10^6 ) ,
                                             index = c("Type_gs", "SNZ_commodity"),
                                             vSize = "Value",
                                             vColor = "CAGR5",
                                             type = 'value',
                                             #aspRatio = 1.618,
                                             overlap.labels = 1,
                                             fun.aggregate = "weighted.mean",
                                             #palette = "RdYlGn",
                                             draw = FALSE)
                       
                       ## modify Goods and Service CAGR 5 change
                       tmp_tm_im$tm$vColor[tmp_tm_im$tm$vSize == tmp_im_g] <-
                          tmp_tm_im$tm$vColorValue[tmp_tm_im$tm$vSize == tmp_im_g] <- tmp_tab$CAGR5[tmp_tab$Name =='Goods imports']*100
                       
                       tmp_tm_im$tm$vColor[tmp_tm_im$tm$vSize == tmp_im_s] <-
                          tmp_tm_im$tm$vColorValue[tmp_tm_im$tm$vSize == tmp_im_s] <- tmp_tab$CAGR5[tmp_tab$Name =='Services imports']*100
                       
                       ## not highlight confidential data
                       #tmp_tm$tm$vColor[tmp_tm$tm$SNZ_commodity == 'Confidential data'] <-
                       #   tmp_tm$tm$vColorValue[tmp_tm$tm$SNZ_commodity == 'Confidential data'] <-
                       #   tmp_tm$tm$color[tmp_tm$tm$SNZ_commodity == 'Confidential data'] <- NA
                       
                       output$KeyImTM <- renderHighchart({
                          highchart() %>%
                             hc_add_series_treemap2( tmp_tm_im , #         hctreemap(
                                                     allowDrillToNode = TRUE,
                                                     layoutAlgorithm = "squarified",
                                                     levelIsConstant = FALSE,
                                                     levels = list(list(level = 1,
                                                                        dataLabels = list(enabled = TRUE,
                                                                                          style = list(fontSize = '20px', color = 'white',
                                                                                                       fontWeight = 'normal'),
                                                                                          backgroundColor = 'lightgrey',
                                                                                          align = 'left', verticalAlign = 'top'),
                                                                        borderColor = "#555",
                                                                        borderWidth = 2 ),
                                                                   list(level = 2,
                                                                        dataLabels = list(enabled = TRUE,
                                                                                          style = list(fontSize = '9px',
                                                                                                       fontWeight = 'normal')
                                                                        )
                                                                   )
                                                     )
                             ) %>%
                             hc_chart(backgroundColor = NULL, plotBorderColor = "#555", plotBorderWidth = 2) %>%
                             hc_title(text = "key commodities and services IMPORTS") %>%
                             hc_subtitle(text = "Coloured by compound annual growth rate (CAGR) for the past 5 years (%)") %>%
                             hc_exporting(enabled = TRUE, formAttributes = list(target = "_blank")) %>%
                             hc_tooltip(pointFormat = "<b>{point.name}</b>:<br>
                                        Import value: ${point.value:,.0f} m <br>
                                        CAGR 5: {point.colorValue:,.1f}%") %>% 
                             hc_colorAxis(minColor = tmp_tm_im$tm$color[which.min(tmp_tm_im$tm$vColorValue)],
                                          maxColor = tmp_tm_im$tm$color[which.max(tmp_tm_im$tm$vColorValue)] ,
                                          labels = list(format = "{value}%", useHTML = TRUE), reversed = FALSE
                             ) %>%
                             hc_legend(align = "right", layout = "vertical", verticalAlign = "top",
                                       reversed = TRUE , y = 70, symbolHeight = 250, itemMarginTop = 10)
                       })
                       
                       
                       # 5.0 Top key commodities and export over time -----------------------------
                       withProgress(message = 'Loading...', value = (i_prog-1)/tot_step, {
                          # Increment the progress bar, and update the detail text.
                          incProgress( i_prog/tot_step, detail = NULL)
                          ##Sys.sleep(0.1)
                          
                       })
                       i_prog <- i_prog + 1
                       
                       tmp_top_g_ex <-
                          dtf_shiny_commodity_service_ex %>%
                          filter( Year == max(Year),
                                  Type_gs == 'Goods',
                                  !SNZ_commodity %in% c('Confidential data', 'Other goods'),
                                  Value >= 10^9) %>% ## 1 bn commodity
                          arrange( -Value ) %>%
                          dplyr::select( SNZ_commodity ) %>%
                          as.matrix() %>%
                          as.character
                       
                       tmp_top_s_ex <-
                          dtf_shiny_commodity_service_ex %>%
                          filter( Year == max(Year),
                                  Type_gs == 'Services',
                                  !SNZ_commodity %in% c('Other business services', 'Other services'),
                                  Value >= (10^9)
                          ) %>%
                          arrange( -Value ) %>%
                          dplyr::select( SNZ_commodity ) %>%
                          as.matrix() %>%
                          as.character
                       
                       ## top 10 commodities and top 5services
                       tmp_top_ex <- c( tmp_top_g_ex, tmp_top_s_ex)
                       
                       tmp_dtf_key_line_ex <- dtf_shiny_commodity_service_ex %>%
                          filter( SNZ_commodity %in% tmp_top_ex,
                                  Year >=2007) %>%
                          mutate( Value = round(Value/10^6),
                                  SNZ_commodity = factor(SNZ_commodity, levels = tmp_top_ex)
                          ) %>%
                          arrange( SNZ_commodity )
                       
                       ### plot
                       output$KeyExLine <- renderHighchart({
                          highchart() %>%
                             hc_exporting(enabled = TRUE, formAttributes = list(target = "_blank")) %>%
                             hc_add_series( data =  tmp_dtf_key_line_ex %>% filter( Type_gs == 'Goods' ) ,
                                            mapping = hcaes(  x = Year, y = Value, group = SNZ_commodity ),
                                            type = 'line',
                                            marker = list(symbol = 'circle') ,
                                            visible = c(T,rep(F,length(tmp_top_g_ex)-1))
                             ) %>%
                             hc_add_series( data =  tmp_dtf_key_line_ex %>% filter( Type_gs == 'Services' ),
                                            mapping = hcaes(  x = Year, y = Value, group = SNZ_commodity ),
                                            type = 'line', dashStyle = 'DashDot', marker = list(symbol = 'circle') ,
                                            visible = c(T,rep(F,length(tmp_top_s_ex)-1))
                             ) %>%
                             hc_xAxis( categories = c( unique( tmp_dtf_key_line_ex$Year) ) ) %>%
                             hc_yAxis( title = list(text = "$ million, NZD"), #"Commodities and services exports over $1 bn"
                                       labels = list( format = "${value:,.0f} m")  ) %>%
                             hc_plotOptions(line = list(
                                dataLabels = list(enabled = F),
                                #stacking = "normal",
                                enableMouseTracking = T)
                             )%>%
                             hc_tooltip(table = TRUE,
                                        sort = TRUE,
                                        pointFormat = paste0( '<br> <span style="color:{point.color}">\u25CF</span>',
                                                              " {series.name}: ${point.y} m"),
                                        headerFormat = '<span style="font-size: 13px">Year {point.key}</span>'
                             ) %>%
                             hc_legend( layout = 'vertical', align = 'left', verticalAlign = 'top', floating = T, x = 100, y = -15 )
                       })
                       
                       # 5.0.1 Top key commodities exports over time -- Percentage -------------------------------
                       withProgress(message = 'Loading...', value = (i_prog-1)/tot_step, {
                          # Increment the progress bar, and update the detail text.
                          incProgress( i_prog/tot_step, detail = NULL)
                          ##Sys.sleep(0.1)
                          
                       })
                       i_prog <- i_prog + 1
                       
                       tmp_tot_ex <-
                          dtf_shiny_full %>%
                          filter( Country == 'World',
                                  Type_ie == 'Exports',
                                  Year >= 2007 )  %>%
                          mutate( Value = round(Value/10^6) ) %>%
                          group_by( Year, Country, Type_ie ) %>%
                          summarize( Value = sum(Value, na.rm=T) ) %>%
                          ungroup %>%
                          mutate( SNZ_commodity = 'Total exports' )
                       
                       tmp_dtf_percent_line_ex <-
                          tmp_dtf_key_line_ex %>%
                          bind_rows( tmp_tot_ex ) %>%
                          group_by( Year, Country, Type_ie ) %>%
                          mutate( Share = Value/Value[SNZ_commodity=='Total exports'],
                                  Value = Share*100 ) %>%
                          ungroup %>%
                          filter( SNZ_commodity != 'Total exports' ) %>%
                          mutate( SNZ_commodity = factor(SNZ_commodity, levels = tmp_top_ex) ) %>%
                          arrange( SNZ_commodity )
                       
                       ### plot
                       tmp_export_percent_hc <- 
                          highchart() %>%
                          hc_exporting(enabled = TRUE, formAttributes = list(target = "_blank")) %>%
                          hc_xAxis( categories = c( unique( tmp_dtf_percent_line_ex$Year) ) ) %>%
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
                       #hc_legend( enabled = FALSE )
                       
                       ### if any services are selected?
                       if( length(tmp_top_g_ex)>=1&length(tmp_top_s_ex)==0 ) {
                          output$KeyExLinePercent <- 
                             renderHighchart({
                                tmp_export_percent_hc %>%
                                   hc_add_series( data =  tmp_dtf_percent_line_ex %>% filter( Type_gs == 'Goods' ) ,
                                                  mapping = hcaes(  x = Year, y = Value, group = SNZ_commodity ),
                                                  type = 'line',
                                                  marker = list(symbol = 'circle') ,
                                                  visible = c(T,rep(F,length(tmp_top_g_ex)-1))
                                   )
                             })
                       }
                       if( length(tmp_top_g_ex)==0 & length(tmp_top_s_ex)>=1 ){
                          output$KeyExLinePercent <- 
                             renderHighchart({
                                tmp_export_percent_hc %>%
                                   hc_add_series( data =  tmp_dtf_percent_line_ex %>% filter( Type_gs == 'Services' ),
                                                  mapping = hcaes(  x = Year, y = Value, group = SNZ_commodity ),
                                                  type = 'line', dashStyle = 'DashDot', marker = list(symbol = 'circle') ,
                                                  visible = c(T,rep(F,length(tmp_top_s_ex)-1))
                                   )
                             })
                       }
                       if( length(tmp_top_g_ex)>=1 & length(tmp_top_s_ex)>=1 ){
                          output$KeyExLinePercent <- 
                             renderHighchart({
                                tmp_export_percent_hc %>%
                                   hc_add_series( data =  tmp_dtf_percent_line_ex %>% filter( Type_gs == 'Goods' ) ,
                                                  mapping = hcaes(  x = Year, y = Value, group = SNZ_commodity ),
                                                  type = 'line',
                                                  marker = list(symbol = 'circle') ,
                                                  visible = c(T,rep(F,length(tmp_top_g_ex)-1))
                                   ) %>%
                                   hc_add_series( data =  tmp_dtf_percent_line_ex %>% filter( Type_gs == 'Services' ),
                                                  mapping = hcaes(  x = Year, y = Value, group = SNZ_commodity ),
                                                  type = 'line', dashStyle = 'DashDot', marker = list(symbol = 'circle') ,
                                                  visible = c(T,rep(F,length(tmp_top_s_ex)-1))
                                   )
                             })
                       }
                       
                       
                       # 5.1 Top key commodities and import over time -----------------------------
                       withProgress(message = 'Loading...', value = (i_prog-1)/tot_step, {
                          # Increment the progress bar, and update the detail text.
                          incProgress( i_prog/tot_step, detail = NULL)
                          ##Sys.sleep(0.1)
                          
                       })
                       i_prog <- i_prog + 1
                       
                       tmp_top_g_im <-
                          dtf_shiny_commodity_service_im %>%
                          filter( Year == max(Year),
                                  Type_gs == 'Goods',
                                  !SNZ_commodity %in% c('Confidential data', 'Other goods'),
                                  Value >= 10^9) %>% ## 1 bn commodity
                          arrange( -Value ) %>%
                          dplyr::select( SNZ_commodity ) %>%
                          as.matrix() %>%
                          as.character
                       
                       tmp_top_s_im <-
                          dtf_shiny_commodity_service_im %>%
                          filter( Year == max(Year),
                                  Type_gs == 'Services',
                                  !SNZ_commodity %in% c('Other business services', 'Other services'),
                                  Value >= (10^9)
                          ) %>%
                          arrange( -Value ) %>%
                          dplyr::select( SNZ_commodity ) %>%
                          as.matrix() %>%
                          as.character
                       
                       ## top 10 commodities and top 5services
                       tmp_top_im <- c( tmp_top_g_im, tmp_top_s_im)
                       
                       tmp_dtf_key_line_im <- dtf_shiny_commodity_service_im %>%
                          filter( SNZ_commodity %in% tmp_top_im,
                                  Year >=2007) %>%
                          mutate( Value = round(Value/10^6),
                                  SNZ_commodity = factor(SNZ_commodity, levels = tmp_top_im)
                          ) %>%
                          arrange( SNZ_commodity )
                       
                       ### plot
                       output$KeyImLine <- renderHighchart({
                          highchart() %>%
                             hc_exporting(enabled = TRUE, formAttributes = list(target = "_blank")) %>%
                             hc_add_series( data =  tmp_dtf_key_line_im %>% filter( Type_gs == 'Goods' ) ,
                                            mapping = hcaes(  x = Year, y = Value, group = SNZ_commodity ),
                                            type = 'line',
                                            marker = list(symbol = 'circle') ,
                                            visible = c(T,rep(F,length(tmp_top_g_im)-1))
                             ) %>%
                             hc_add_series( data =  tmp_dtf_key_line_im %>% filter( Type_gs == 'Services' ),
                                            mapping = hcaes(  x = Year, y = Value, group = SNZ_commodity ),
                                            type = 'line', dashStyle = 'DashDot', marker = list(symbol = 'circle') ,
                                            visible = c(T,rep(F,length(tmp_top_s_im)-1))
                             ) %>%
                             hc_xAxis( categories = c( unique( tmp_dtf_key_line_im$Year) ) ) %>%
                             hc_yAxis( title = list(text = "$ million, NZD"), # "Commodities and services imports over $1 bn"
                                       labels = list( format = "${value:,.0f} m")  ) %>%
                             hc_plotOptions(line = list(
                                dataLabels = list(enabled = F),
                                #stacking = "normal",
                                enableMouseTracking = T)
                             )%>%
                             hc_tooltip(table = TRUE,
                                        sort = TRUE,
                                        pointFormat = paste0( '<br> <span style="color:{point.color}">\u25CF</span>',
                                                              " {series.name}: ${point.y} m"),
                                        headerFormat = '<span style="font-size: 13px">Year {point.key}</span>'
                             ) %>%
                             hc_legend( layout = 'vertical', align = 'left', verticalAlign = 'top', floating = T, x = 100, y = -15 )
                       })
                       
                       # 5.1.1 Top key commodities and import over time Percent -----------------------------
                       withProgress(message = 'Loading...', value = (i_prog-1)/tot_step, {
                          # Increment the progress bar, and update the detail text.
                          incProgress( i_prog/tot_step, detail = NULL)
                          ##Sys.sleep(0.1)
                          
                       })
                       i_prog <- i_prog + 1
                       
                       tmp_tot_im <-
                          dtf_shiny_full %>%
                          filter( Country == 'World',
                                  Type_ie == 'Imports',
                                  Year >= 2007 )  %>%
                          mutate( Value = round(Value/10^6) ) %>%
                          group_by( Year, Country, Type_ie ) %>%
                          summarize( Value = sum(Value, na.rm=T) ) %>%
                          ungroup %>%
                          mutate( SNZ_commodity = 'Total imports' )
                       
                       tmp_dtf_percent_line_im <-
                          tmp_dtf_key_line_im %>%
                          bind_rows( tmp_tot_im ) %>%
                          group_by( Year, Country, Type_ie ) %>%
                          mutate( Share = Value/Value[SNZ_commodity=='Total imports'],
                                  Value = Share*100 ) %>%
                          ungroup %>%
                          filter( SNZ_commodity != 'Total imports' ) %>%
                          mutate( SNZ_commodity = factor(SNZ_commodity, levels = tmp_top_im) ) %>%
                          arrange( SNZ_commodity )
                       
                       ### plot
                       tmp_import_percent_hc <- 
                          highchart() %>%
                          hc_exporting(enabled = TRUE, formAttributes = list(target = "_blank")) %>%
                          hc_xAxis( categories = c( unique( tmp_dtf_percent_line_im$Year) ) ) %>%
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
                       #hc_legend( enabled = FALSE )
                       
                       ### if any services are selected?
                       if( length(tmp_top_g_im)>=1&length(tmp_top_s_im)==0 ) {
                          output$KeyImLinePercent <- 
                             renderHighchart({
                                tmp_import_percent_hc %>%
                                   hc_add_series( data =  tmp_dtf_percent_line_im %>% filter( Type_gs == 'Goods' ) ,
                                                  mapping = hcaes(  x = Year, y = Value, group = SNZ_commodity ),
                                                  type = 'line',
                                                  marker = list(symbol = 'circle') ,
                                                  visible = c(T,rep(F,length(tmp_top_g_im)-1))
                                   )
                             })
                       }
                       if( length(tmp_top_g_im)==0 & length(tmp_top_s_im)>=1 ){
                          output$KeyImLinePercent <- 
                             renderHighchart({
                                tmp_import_percent_hc %>%
                                   hc_add_series( data =  tmp_dtf_percent_line_im %>% filter( Type_gs == 'Services' ),
                                                  mapping = hcaes(  x = Year, y = Value, group = SNZ_commodity ),
                                                  type = 'line', dashStyle = 'DashDot', marker = list(symbol = 'circle') ,
                                                  visible = c(T,rep(F,length(tmp_top_s_im)-1))
                                   )
                             })
                       }
                       if( length(tmp_top_g_im)>=1 & length(tmp_top_s_im)>=1 ){
                          output$KeyImLinePercent <- 
                             renderHighchart({
                                tmp_import_percent_hc %>%
                                   hc_add_series( data =  tmp_dtf_percent_line_im %>% filter( Type_gs == 'Goods' ) ,
                                                  mapping = hcaes(  x = Year, y = Value, group = SNZ_commodity ),
                                                  type = 'line',
                                                  marker = list(symbol = 'circle') ,
                                                  visible = c(T,rep(F,length(tmp_top_g_im)-1))
                                   ) %>%
                                   hc_add_series( data =  tmp_dtf_percent_line_im %>% filter( Type_gs == 'Services' ),
                                                  mapping = hcaes(  x = Year, y = Value, group = SNZ_commodity ),
                                                  type = 'line', dashStyle = 'DashDot', marker = list(symbol = 'circle') ,
                                                  visible = c(T,rep(F,length(tmp_top_s_im)-1))
                                   )
                             })
                       }
                       
                       # 6. Global trading partners glance ---------------------------------------
                       withProgress(message = 'Loading...', value = (i_prog-1)/tot_step, {
                          # Increment the progress bar, and update the detail text.
                          incProgress( i_prog/tot_step, detail = NULL)
                          ##Sys.sleep(0.1)
                          
                       })
                       i_prog <- i_prog + 1
                       
                       output$TradeMap <- 
                          renderUI({
                             tags$iframe(#srcdoc = paste(readLines("www/Twoway_trade_by_country.html"), 
                                #               collapse = '\n'),
                                src = "Twoway_trade_by_country.html",
                                height="550px", width="100%")
                          })
                       
                       # 7.0 FTA timeline ----------------------------
                       withProgress(message = 'Loading...', value = (i_prog-1)/tot_step, {
                          # Increment the progress bar, and update the detail text.
                          incProgress( i_prog/tot_step, detail = NULL)
                          ##Sys.sleep(0.1)
                          
                       })
                       i_prog <- i_prog + 1
                       
                       ### FTA infomration
                       # dtf_fta <- 
                       #    data.frame(
                       #       id = 1:9,
                       #       content = c("<a href = 'https://www.mfat.govt.nz/en/trade/free-trade-agreements/free-trade-agreements-in-force/nz-china-free-trade-agreement/' target = '_blank'> NZ-China FTA </a>",
                       #                   "<a href = 'https://www.mfat.govt.nz/en/trade/free-trade-agreements/free-trade-agreements-in-force/nz-australia-closer-economic-relations-cer/' target = '_blank'> NZ-Australia CER </a>",
                       #                   "<a href = 'https://www.mfat.govt.nz/en/trade/free-trade-agreements/free-trade-agreements-in-force/aanzfta-asean-australia-new-zealand-fta/' target = '_blank'> AANZFTA </a>",
                       #                   "<a href = 'https://www.mfat.govt.nz/en/trade/free-trade-agreements/free-trade-agreements-in-force/hong-kong-fta/' target = '_blank'> NZ-Hong Kong, China CEP </a>",
                       #                   "<a href = 'https://www.mfat.govt.nz/en/trade/free-trade-agreements/free-trade-agreements-in-force/malaysia-fta/' target = '_blank'> NZ-Malaysia FTA </a>",
                       #                   "<a href = 'https://www.mfat.govt.nz/en/trade/free-trade-agreements/free-trade-agreements-in-force/singapore/' target = '_blank'> NZ-Singapore CEP </a>",
                       #                   "<a href = 'https://www.mfat.govt.nz/en/trade/free-trade-agreements/free-trade-agreements-in-force/thailand/' target = '_blank'> NZ-Thailand CEP </a>",
                       #                   "<a href = 'https://www.mfat.govt.nz/en/trade/free-trade-agreements/free-trade-agreements-in-force/p4/' target = '_blank'> P4 </a>",
                       #                   "<a href = 'https://www.mfat.govt.nz/en/trade/free-trade-agreements/free-trade-agreements-in-force/nz-korea-free-trade-agreement/' target = '_blank'> NZ-Korea FTA </a>"),
                       #       ## time when FTAs in forece
                       #       start = c("2008-04-07",# cn
                       #                 "1983-01-01",# aus
                       #                 "2010-01-01",# asean
                       #                 "2011-01-01",# hk
                       #                 "2010-08-01", #my
                       #                 "2001-01-01", #sing
                       #                 "2005-07-01", # Thai
                       #                 "2006-01-01", #p4
                       #                 "2015-12-20"
                       #       )
                       #       
                       #    )
                       # 
                       # output$FTATimeLine <- 
                       #    renderTimevis({ timevis(dtf_fta) })
                       
                       ### FTA infomration
                       groups <- 
                          data.frame( id = c('cn', 'aus', 'asean',
                                             'hk', 'my', 'sin',
                                             'thai', 'p4', 'sk'),
                                      content =c("<a href = 'https://www.mfat.govt.nz/en/trade/free-trade-agreements/free-trade-agreements-in-force/nz-china-free-trade-agreement/' target = '_blank'> NZ-China FTA </a>",
                                                 "<a href = 'https://www.mfat.govt.nz/en/trade/free-trade-agreements/free-trade-agreements-in-force/nz-australia-closer-economic-relations-cer/' target = '_blank'> NZ-Australia CER </a>",
                                                 "<a href = 'https://www.mfat.govt.nz/en/trade/free-trade-agreements/free-trade-agreements-in-force/aanzfta-asean-australia-new-zealand-fta/' target = '_blank'> AANZFTA </a>",
                                                 "<a href = 'https://www.mfat.govt.nz/en/trade/free-trade-agreements/free-trade-agreements-in-force/hong-kong-fta/' target = '_blank'> NZ-Hong Kong, China CEP </a>",
                                                 "<a href = 'https://www.mfat.govt.nz/en/trade/free-trade-agreements/free-trade-agreements-in-force/malaysia-fta/' target = '_blank'> NZ-Malaysia FTA </a>",
                                                 "<a href = 'https://www.mfat.govt.nz/en/trade/free-trade-agreements/free-trade-agreements-in-force/singapore/' target = '_blank'> NZ-Singapore CEP </a>",
                                                 "<a href = 'https://www.mfat.govt.nz/en/trade/free-trade-agreements/free-trade-agreements-in-force/thailand/' target = '_blank'> NZ-Thailand CEP </a>",
                                                 "<a href = 'https://www.mfat.govt.nz/en/trade/free-trade-agreements/free-trade-agreements-in-force/p4/' target = '_blank'> P4 </a>",
                                                 "<a href = 'https://www.mfat.govt.nz/en/trade/free-trade-agreements/free-trade-agreements-in-force/nz-korea-free-trade-agreement/' target = '_blank'> NZ-Korea FTA </a>")
                          )
                       
                       
                       dtf_fta <- 
                          data.frame(
                             id = 1:9,
                             content = c("5 years",
                                         "3 years",
                                         "5 years",
                                         "10 years",
                                         "5 years",
                                         "1 year and 4 months",
                                         "1 year",
                                         "2 years and 4 months",
                                         "6 years and 6 months"
                             ) ,
                             ## talk started
                             start = c("2003-10-01", # cn
                                       "1979-12-31", #aus
                                       "2005-03-01", #asean
                                       "2001-01-01", #hk
                                       "2005-03-01", # my
                                       "1999-09-01", #singapore
                                       "2004-06-01", ## thia
                                       "2003-09-01", #p4
                                       "2009-06-01" #sk
                             ),
                             ## time when FTAs in forece
                             end = c("2008-10-01",# cn
                                     "1983-01-01",# aus
                                     "2010-01-01",# asean
                                     "2011-01-01",# hk
                                     "2010-08-01", #my
                                     "2001-01-01", #sing
                                     "2005-07-01", # Thai
                                     "2006-01-01", #p4
                                     "2015-12-20" # sk
                             ),
                             group = c('cn', 'aus', 'asean',
                                       'hk', 'my', 'sin',
                                       'thai', 'p4', 'sk') #,
                             #type = 'range'
                             
                          )
                       
                       output$FTATimeLine <-
                          renderTimevis({ timevis(data = dtf_fta, groups = groups, options = list(align = 'left'))  })
                       
                       
                       # 7.1 Key exports market trend line ------------------------------------
                       withProgress(message = 'Loading...', value = (i_prog-1)/tot_step, {
                          # Increment the progress bar, and update the detail text.
                          incProgress( i_prog/tot_step, detail = NULL)
                          ##Sys.sleep(0.1)
                          
                       })
                       i_prog <- i_prog + 1
                       
                       tmp_data_country_ex <-
                          dtf_shiny_country_gs %>%
                          filter( Year>=2007 ) %>%
                          group_by( Year, Country, Type_ie, Note, ISO2, lat, lon ) %>%
                          summarise( Value = sum(Value, na.rm=T) ) %>%
                          ungroup %>%
                          filter( Type_ie == 'Exports' )
                       
                       ## export markets over $500 million
                       tmp_top_country_ex <-
                          tmp_data_country_ex %>%
                          filter( Year == max(Year),
                                  Value >= (10^9), 
                                  !Country %in% c("World", 
                                                  "Destination Unknown - EU")
                          ) %>% ## 1 bn commodity
                          arrange( -Value ) %>%
                          dplyr::select( Country ) %>%
                          as.matrix() %>%
                          as.character
                       
                       tmp_data_country_ex  %<>%
                          filter( Country %in% tmp_top_country_ex #,
                                  #Year >=2007
                          ) %>%
                          mutate( Value = round(Value/10^6),
                                  Country = factor(Country, levels = tmp_top_country_ex)
                          ) %>%
                          arrange( Country )
                       
                       ### plot
                       output$ExMarketLine <- renderHighchart({
                          highchart() %>%
                             hc_exporting(enabled = TRUE, formAttributes = list(target = "_blank")) %>%
                             hc_add_series( data =  tmp_data_country_ex ,
                                            mapping = hcaes(  x = Year, y = Value, group = Country),
                                            type = 'line',
                                            marker = list(symbol = 'circle') ,
                                            visible = c( rep(T,5), rep(F,length(tmp_top_country_ex)-5) )
                             ) %>%
                             hc_xAxis( categories = c( unique( tmp_data_country_ex$Year) ) ) %>%
                             hc_yAxis( title = list(text = "$ million, NZD"), #"Exports markets over $1bn"
                                       labels = list( format = "${value:,.0f} m")  ) %>%
                             hc_plotOptions(line = list(
                                dataLabels = list(enabled = F),
                                #stacking = "normal",
                                enableMouseTracking = T)
                             )%>%
                             hc_tooltip(table = TRUE,
                                        sort = TRUE,
                                        pointFormat = paste0( '<br> <span style="color:{point.color}">\u25CF</span>',
                                                              " {series.name}: ${point.y} m"),
                                        headerFormat = '<span style="font-size: 13px">Year {point.key}</span>'
                             ) %>%
                             hc_legend( layout = 'vertical', align = 'left', verticalAlign = 'top', floating = T, x = 100, y = -15 )
                       })
                       
                       # 7.1.1 Key exports market trend line Percent ------------------------------------
                       withProgress(message = 'Loading...', value = (i_prog-1)/tot_step, {
                          # Increment the progress bar, and update the detail text.
                          incProgress( i_prog/tot_step, detail = NULL)
                          ##Sys.sleep(0.1)
                          
                       })
                       i_prog <- i_prog + 1
                       
                       tmp_data_tot_ex <-
                          dtf_shiny_country_gs %>%
                          filter( Year>=2007 ) %>%
                          filter(  Type_ie == 'Exports', Country == 'World' ) %>%
                          group_by( Year, Country, Type_ie, Note ) %>%
                          summarise( Value = sum(Value, na.rm=T) ) %>%
                          ungroup 
                       
                       tmp_data_country_ex_pc <-
                          tmp_data_country_ex %>%
                          bind_rows( tmp_data_tot_ex ) %>%
                          group_by( Year, Type_ie,  Note ) %>%
                          mutate( Share = Value/(Value[Country=='World']/10^6) ) %>%
                          ungroup %>%
                          mutate( Value = Share*100 ) %>%
                          filter( Country != 'World' ) %>%
                          mutate( Country = factor(Country, levels = tmp_top_country_ex) ) %>%
                          arrange( Country, Year )
                       
                       output$ExMarketLinePercent <-
                          renderHighchart({
                             highchart() %>%
                                hc_exporting(enabled = TRUE, formAttributes = list(target = "_blank")) %>%
                                hc_add_series( data =  tmp_data_country_ex_pc ,
                                               mapping = hcaes(  x = Year, y = Value, group = Country),
                                               type = 'line',
                                               marker = list(symbol = 'circle'),
                                               visible = c( rep(T,5), rep(F,length(tmp_top_country_ex)-5) )
                                ) %>%
                                hc_xAxis( categories = c( unique( tmp_data_country_ex_pc$Year) )   ) %>%
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
                       
                       # 7.2 Key imports market trend line ------------------------------------
                       withProgress(message = 'Loading...', value = (i_prog-1)/tot_step, {
                          # Increment the progress bar, and update the detail text.
                          incProgress( i_prog/tot_step, detail = NULL)
                          ##Sys.sleep(0.1)
                          
                       })
                       i_prog <- i_prog + 1
                       
                       tmp_data_country_im <-
                          dtf_shiny_country_gs %>%
                          filter( Year>=2007 ) %>%
                          group_by( Year, Country, Type_ie, Note, ISO2, lat, lon ) %>%
                          summarise( Value = sum(Value, na.rm=T) ) %>%
                          ungroup %>%
                          filter( Type_ie == 'Imports' )
                       
                       ## import markets over $500 million
                       tmp_top_country_im <-
                          tmp_data_country_im %>%
                          filter( Year == max(Year),
                                  Value >= (10^9), 
                                  !Country %in% c("World", 
                                                  "Destination Unknown - EU")
                          ) %>% ## 1 bn commodity
                          arrange( -Value ) %>%
                          dplyr::select( Country ) %>%
                          as.matrix() %>%
                          as.character
                       
                       tmp_data_country_im  %<>%
                          filter( Country %in% tmp_top_country_im #,
                                  #Year >=2007
                          ) %>%
                          mutate( Value = round(Value/10^6),
                                  Country = factor(Country, levels = tmp_top_country_im)
                          ) %>%
                          arrange( Country )
                       
                       ### plot
                       output$ImMarketLine <- renderHighchart({
                          highchart() %>%
                             hc_exporting(enabled = TRUE, formAttributes = list(target = "_blank")) %>%
                             hc_add_series( data =  tmp_data_country_im ,
                                            mapping = hcaes(  x = Year, y = Value, group = Country),
                                            type = 'line',
                                            marker = list(symbol = 'circle') ,
                                            visible = c( rep(T,5), rep(F,length(tmp_top_country_im)-5) )
                             ) %>%
                             hc_xAxis( categories = c( unique( tmp_data_country_im$Year) ) ) %>%
                             hc_yAxis( title = list(text = "$ million, NZD"), # "Imports markets over $1bn"
                                       labels = list( format = "${value:,.0f} m")  ) %>%
                             hc_plotOptions(line = list(
                                dataLabels = list(enabled = F),
                                #stacking = "normal",
                                enableMouseTracking = T)
                             )%>%
                             hc_tooltip(table = TRUE,
                                        sort = TRUE,
                                        pointFormat = paste0( '<br> <span style="color:{point.color}">\u25CF</span>',
                                                              " {series.name}: ${point.y} m"),
                                        headerFormat = '<span style="font-size: 13px">Year {point.key}</span>'
                             ) %>%
                             hc_legend( layout = 'vertical', align = 'left', verticalAlign = 'top', floating = T, x = 100, y = -15 )
                       })
                       
                       # 7.2.1 Key imports market trend line Percent ------------------------------------
                       withProgress(message = 'Loading...', value = (i_prog-1)/tot_step, {
                          # Increment the progress bar, and update the detail text.
                          incProgress( i_prog/tot_step, detail = NULL)
                          ##Sys.sleep(0.1)
                          
                       })
                       i_prog <- i_prog + 1
                       
                       tmp_data_tot_im <-
                          dtf_shiny_country_gs %>%
                          filter( Year>=2007 ) %>%
                          filter(  Type_ie == 'Imports', Country == 'World' ) %>%
                          group_by( Year, Country, Type_ie, Note ) %>%
                          summarise( Value = sum(Value, na.rm=T) ) %>%
                          ungroup 
                       
                       tmp_data_country_im_pc <-
                          tmp_data_country_im %>%
                          bind_rows( tmp_data_tot_im ) %>%
                          group_by( Year, Type_ie,  Note ) %>%
                          mutate( Share = Value/(Value[Country=='World']/10^6) ) %>%
                          ungroup %>%
                          mutate( Value = Share*100 ) %>%
                          filter( Country != 'World' ) %>%
                          mutate( Country = factor(Country, levels = tmp_top_country_im) ) %>%
                          arrange( Country, Year )
                       
                       output$ImMarketLinePercent <-
                          renderHighchart({
                             highchart() %>%
                                hc_exporting(enabled = TRUE, formAttributes = list(target = "_blank")) %>%
                                hc_add_series( data =  tmp_data_country_im_pc ,
                                               mapping = hcaes(  x = Year, y = Value, group = Country),
                                               type = 'line',
                                               marker = list(symbol = 'circle'),
                                               visible = c( rep(T,5), rep(F,length(tmp_top_country_im)-5) )
                                ) %>%
                                hc_xAxis( categories = c( unique( tmp_data_country_im_pc$Year) )   ) %>%
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
                       
                       # 7.3 Key Two way trade market trend line ------------------------------------
                       withProgress(message = 'Loading...', value = (i_prog-1)/tot_step, {
                          # Increment the progress bar, and update the detail text.
                          incProgress( i_prog/tot_step, detail = NULL)
                          ##Sys.sleep(0.1)
                          
                       })
                       i_prog <- i_prog + 1
                       
                       tmp_data_country_twoway <-
                          dtf_shiny_country_gs %>%
                          filter( Year>=2007 ) %>%
                          group_by( Year, Country, Note, ISO2, lat, lon ) %>%
                          summarise( Value = sum(Value, na.rm=T) ) %>%
                          ungroup 
                       
                       ## import markets over $500 million
                       tmp_top_country_twoway <-
                          tmp_data_country_twoway %>%
                          filter( Year == max(Year),
                                  Value >= (10^9)*2, 
                                  !Country %in% c("World", 
                                                  "Destination Unknown - EU")
                          ) %>% ## 1 bn commodity
                          arrange( -Value ) %>%
                          dplyr::select( Country ) %>%
                          as.matrix() %>%
                          as.character
                       
                       tmp_data_country_twoway  %<>%
                          filter( Country %in% tmp_top_country_twoway #,
                                  #Year >=2007
                          ) %>%
                          mutate( Value = round(Value/10^6),
                                  Country = factor(Country, levels = tmp_top_country_twoway)
                          ) %>%
                          arrange( Country )
                       
                       ### plot
                       output$TwowayMarketLine <- renderHighchart({
                          highchart() %>%
                             hc_exporting(enabled = TRUE, formAttributes = list(target = "_blank")) %>%
                             hc_add_series( data =  tmp_data_country_twoway ,
                                            mapping = hcaes(  x = Year, y = Value, group = Country),
                                            type = 'line',
                                            marker = list(symbol = 'circle') ,
                                            visible = c( rep(T,5), rep(F,length(tmp_top_country_twoway)-5) )
                             ) %>%
                             hc_xAxis( categories = c( unique( tmp_data_country_twoway$Year) ) ) %>%
                             hc_yAxis( title = list(text = "$ million, NZD"), # "Markets with two way trade over $2bn"
                                       labels = list( format = "${value:,.0f} m")  ) %>%
                             hc_plotOptions(line = list(
                                dataLabels = list(enabled = F),
                                #stacking = "normal",
                                enableMouseTracking = T)
                             )%>%
                             hc_tooltip(table = TRUE,
                                        sort = TRUE,
                                        pointFormat = paste0( '<br> <span style="color:{point.color}">\u25CF</span>',
                                                              " {series.name}: ${point.y} m"),
                                        headerFormat = '<span style="font-size: 13px">Year {point.key}</span>'
                             ) %>%
                             hc_legend( layout = 'vertical', align = 'left', verticalAlign = 'top', floating = T, x = 100, y = -15 )
                       })
                       
                       # 7.4 Trade balance market trend line ------------------------------------
                       withProgress(message = 'Loading...', value = (i_prog-1)/tot_step, {
                          # Increment the progress bar, and update the detail text.
                          incProgress( i_prog/tot_step, detail = NULL)
                          ##Sys.sleep(0.1)
                          
                       })
                       i_prog <- i_prog + 1
                       
                       tmp_data_country_balance <-
                          dtf_shiny_country_gs %>%
                          filter( Year>=2007 ) %>%
                          group_by( Year, Country, Type_ie, Note, ISO2, lat, lon ) %>%
                          summarise( Value = sum(Value, na.rm=T) ) %>%
                          ungroup %>%
                          spread( Type_ie, Value ) %>%
                          mutate( Exports = ifelse( is.na(Exports), 0, Exports ),
                                  Imports = ifelse( is.na(Imports), 0, Imports ) ) %>%
                          mutate( Value = Exports - Imports )
                       
                       ## import markets over $500 million
                       tmp_top_country_balance_positive <-
                          tmp_data_country_balance %>%
                          filter( Year == max(Year),
                                  Value >= (10^9)/2, 
                                  !Country %in% c("World", 
                                                  "Destination Unknown - EU",
                                                  "Ships' Bunkering" ,
                                                  "Passengers' Effects")
                          ) %>% ## 1 bn commodity
                          arrange( -Value ) %>%
                          dplyr::select( Country ) %>%
                          as.matrix() %>%
                          as.character
                       
                       tmp_top_country_balance_negative <-
                          tmp_data_country_balance %>%
                          filter( Year == max(Year),
                                  Value <= -(10^9)/2, 
                                  !Country %in% c("World", 
                                                  "Destination Unknown - EU",
                                                  "Ships' Bunkering" ,
                                                  "Passengers' Effects")
                          ) %>% ## 1 bn commodity
                          arrange( Value ) %>%
                          dplyr::select( Country ) %>%
                          as.matrix() %>%
                          as.character
                       
                       tmp_top_country_balance <- c(tmp_top_country_balance_positive, tmp_top_country_balance_negative)
                       
                       tmp_data_country_balance  %<>%
                          filter( Country %in% tmp_top_country_balance #,
                                  #Year >=2007
                          ) %>%
                          mutate( Value = round(Value/10^6),
                                  Country = factor(Country, levels = tmp_top_country_balance)
                          ) %>%
                          arrange( Country )
                       
                       ### plot
                       output$BalanceMarketLine <- renderHighchart({
                          highchart() %>%
                             hc_exporting(enabled = TRUE, formAttributes = list(target = "_blank")) %>%
                             hc_add_series( data =  tmp_data_country_balance %>% filter( Country %in% tmp_top_country_balance_positive ) ,
                                            mapping = hcaes(  x = Year, y = Value, group = Country ),
                                            type = 'line',
                                            marker = list(symbol = 'circle') ,
                                            visible = c( rep(T,3), rep(F,length(tmp_top_country_balance_positive)-3))
                             ) %>%
                             hc_add_series( data =  tmp_data_country_balance %>% filter( Country %in% tmp_top_country_balance_negative ) ,
                                            mapping = hcaes(  x = Year, y = Value, group = Country ),
                                            type = 'line', dashStyle = 'DashDot', marker = list(symbol = 'circle') ,
                                            visible = c( rep(T,3),rep(F,length(tmp_top_country_balance_negative)-3))
                             ) %>%
                             hc_xAxis( categories = c( unique( tmp_data_country_balance$Year) ) ) %>%
                             hc_yAxis( title = list(text = "$ million, NZD"), #"Markets with trade balance over $500m and under -$500m"
                                       labels = list( format = "${value:,.0f} m")  ) %>%
                             hc_plotOptions(line = list(
                                dataLabels = list(enabled = F),
                                #stacking = "normal",
                                enableMouseTracking = T)
                             )%>%
                             hc_tooltip(table = TRUE,
                                        sort = TRUE,
                                        pointFormat = paste0( '<br> <span style="color:{point.color}">\u25CF</span>',
                                                              " {series.name}: ${point.y} m"),
                                        headerFormat = '<span style="font-size: 13px">Year {point.key}</span>'
                             ) %>%
                             hc_legend( layout = 'vertical', align = 'left', verticalAlign = 'top', floating = T, x = 100, y = -15 )
                       })
                       
                       #session$allowReconnect(TRUE)
                       # 7.6 Key exports market for goods trend line ------------------------------------
                       withProgress(message = 'Loading...', value = (i_prog-1)/tot_step, {
                          # Increment the progress bar, and update the detail text.
                          incProgress( i_prog/tot_step, detail = NULL)
                          ##Sys.sleep(0.1)
                          
                       })
                       i_prog <- i_prog + 1
                       
                       tmp_data_country_ex_g <-
                          dtf_shiny_country_gs %>%
                          filter( Year>=2007 ) %>%
                          filter( Type_gs == 'Goods', Type_ie == 'Exports' ) 
                       
                       ## export markets over $500 million
                       tmp_top_country_ex_g <-
                          tmp_data_country_ex_g %>%
                          filter( Year == max(Year),
                                  Value >= (10^9)/2, 
                                  !Country %in% c("World", 
                                                  "Destination Unknown - EU")
                          ) %>% ## 1 bn commodity
                          arrange( -Value ) %>%
                          dplyr::select( Country ) %>%
                          as.matrix() %>%
                          as.character
                       
                       tmp_data_country_ex_g  %<>%
                          filter( Country %in% tmp_top_country_ex_g ) %>%
                          mutate( Value = round(Value/10^6),
                                  Country = factor(Country, levels = tmp_top_country_ex_g)
                          ) %>%
                          arrange( Country, Year )
                       
                       ### plot
                       output$ExGMarketLine <- renderHighchart({
                          highchart() %>%
                             hc_exporting(enabled = TRUE, formAttributes = list(target = "_blank")) %>%
                             hc_add_series( data =  tmp_data_country_ex_g ,
                                            mapping = hcaes(  x = Year, y = Value, group = Country),
                                            type = 'line',
                                            marker = list(symbol = 'circle') ,
                                            visible = c( rep(T,5), rep(F,length(tmp_top_country_ex_g)-5) )
                             ) %>%
                             hc_xAxis( categories = c( unique( tmp_data_country_ex_g$Year) ) ) %>%
                             hc_yAxis( title = list(text = "$ million, NZD" ), #"Exports markets over $500mn for goods"
                                       labels = list( format = "${value:,.0f} m")  ) %>%
                             hc_plotOptions(line = list(
                                dataLabels = list(enabled = F),
                                #stacking = "normal",
                                enableMouseTracking = T)
                             )%>%
                             hc_tooltip(table = TRUE,
                                        sort = TRUE,
                                        pointFormat = paste0( '<br> <span style="color:{point.color}">\u25CF</span>',
                                                              " {series.name}: ${point.y} m"),
                                        headerFormat = '<span style="font-size: 13px">Year {point.key}</span>'
                             ) %>%
                             hc_legend( layout = 'vertical', align = 'left', verticalAlign = 'top', floating = T, x = 100, y = -15 )
                       })
                       
                       # 7.6.1 Key exports market for goods trend line Percent ------------------------------------
                       withProgress(message = 'Loading...', value = (i_prog-1)/tot_step, {
                          # Increment the progress bar, and update the detail text.
                          incProgress( i_prog/tot_step, detail = NULL)
                          ##Sys.sleep(0.1)
                          
                       })
                       i_prog <- i_prog + 1
                       
                       tmp_data_tot_ex_g <-
                          dtf_shiny_country_gs %>%
                          filter( Year>=2007 ) %>%
                          filter( Type_gs == 'Goods', Type_ie == 'Exports', Country == 'World' ) 
                       
                       tmp_data_country_ex_g_pc <-
                          tmp_data_country_ex_g %>%
                          bind_rows( tmp_data_tot_ex_g ) %>%
                          group_by( Year, Type_ie, Type_gs, Commodity, Note ) %>%
                          mutate( Share = Value/(Value[Country=='World']/10^6) ) %>%
                          ungroup %>%
                          mutate( Value = Share*100 ) %>%
                          filter( Country != 'World' ) %>%
                          mutate( Country = factor(Country, levels = tmp_top_country_ex_g) ) %>%
                          arrange( Country, Year )
                       
                       output$ExGMarketLinePercent <-
                          renderHighchart({
                             highchart() %>%
                                hc_exporting(enabled = TRUE, formAttributes = list(target = "_blank")) %>%
                                hc_add_series( data =  tmp_data_country_ex_g_pc ,
                                               mapping = hcaes(  x = Year, y = Value, group = Country),
                                               type = 'line',
                                               marker = list(symbol = 'circle'),
                                               visible = c( rep(T,5), rep(F,length(tmp_top_country_ex_g)-5) )
                                ) %>%
                                hc_xAxis( categories = c( unique( tmp_data_country_ex_g_pc$Year) )   ) %>%
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
                       
                       # 7.7 Key exports market for services trend line ------------------------------------
                       withProgress(message = 'Loading...', value = (i_prog-1)/tot_step, {
                          # Increment the progress bar, and update the detail text.
                          incProgress( i_prog/tot_step, detail = NULL)
                          ##Sys.sleep(0.1)
                          
                       })
                       i_prog <- i_prog + 1
                       
                       tmp_data_country_ex_s <-
                          dtf_shiny_country_gs %>%
                          filter( Year>=2007 ) %>%
                          filter( Type_gs == 'Services', Type_ie == 'Exports' ) 
                       
                       ## export markets over $500 million
                       tmp_top_country_ex_s <-
                          tmp_data_country_ex_s %>%
                          filter( Year == max(Year),
                                  Value >= (10^9)/2, 
                                  !Country %in% c("World", 
                                                  "Destination Unknown - EU")
                          ) %>% ## 1 bn commodity
                          arrange( -Value ) %>%
                          dplyr::select( Country ) %>%
                          as.matrix() %>%
                          as.character
                       
                       tmp_data_country_ex_s  %<>%
                          filter( Country %in% tmp_top_country_ex_s ) %>%
                          mutate( Value = round(Value/10^6),
                                  Country = factor(Country, levels = tmp_top_country_ex_s)
                          ) %>%
                          arrange( Country, Year )
                       
                       ### plot
                       output$ExSMarketLine <- 
                          renderHighchart({
                             highchart() %>%
                                hc_exporting(enabled = TRUE, formAttributes = list(target = "_blank")) %>%
                                hc_add_series( data =  tmp_data_country_ex_s ,
                                               mapping = hcaes(  x = Year, y = Value, group = Country),
                                               type = 'line',
                                               marker = list(symbol = 'circle') ,
                                               visible = c( rep(T,5), rep(F,length(tmp_top_country_ex_s)-5) )
                                ) %>%
                                hc_xAxis( categories = c( unique( tmp_data_country_ex_s$Year) ) ) %>%
                                hc_yAxis( title = list(text = "$ million, NZD"), #"Exports markets over $500mn for services"
                                          labels = list( format = "${value:,.0f} m")  ) %>%
                                hc_plotOptions(line = list(
                                   dataLabels = list(enabled = F),
                                   #stacking = "normal",
                                   enableMouseTracking = T)
                                )%>%
                                hc_tooltip(table = TRUE,
                                           sort = TRUE,
                                           pointFormat = paste0( '<br> <span style="color:{point.color}">\u25CF</span>',
                                                                 " {series.name}: ${point.y} m"),
                                           headerFormat = '<span style="font-size: 13px">Year {point.key}</span>'
                                ) %>%
                                hc_legend( layout = 'vertical', align = 'left', verticalAlign = 'top', floating = T, x = 100, y = -15 )
                          })
                       
                       # 7.7.1 Key exports market for services trend line Percent ------------------------------------
                       withProgress(message = 'Loading...', value = (i_prog-1)/tot_step, {
                          # Increment the progress bar, and update the detail text.
                          incProgress( i_prog/tot_step, detail = NULL)
                          ##Sys.sleep(0.1)
                          
                       })
                       i_prog <- i_prog + 1
                       
                       tmp_data_tot_ex_s <-
                          dtf_shiny_country_gs %>%
                          filter( Year>=2007 ) %>%
                          filter( Type_gs == 'Services', Type_ie == 'Exports', Country == 'World' ) 
                       
                       tmp_data_country_ex_s_pc <-
                          tmp_data_country_ex_s %>%
                          bind_rows( tmp_data_tot_ex_s ) %>%
                          group_by( Year, Type_ie, Type_gs, Commodity, Note ) %>%
                          mutate( Share = Value/(Value[Country=='World']/10^6) ) %>%
                          ungroup %>%
                          mutate( Value = Share*100 ) %>%
                          filter( Country != 'World' ) %>%
                          mutate( Country = factor(Country, levels = tmp_top_country_ex_s) ) %>%
                          arrange( Country, Year )
                       
                       output$ExSMarketLinePercent <-
                          renderHighchart({
                             highchart() %>%
                                hc_exporting(enabled = TRUE, formAttributes = list(target = "_blank")) %>%
                                hc_add_series( data =  tmp_data_country_ex_s_pc ,
                                               mapping = hcaes(  x = Year, y = Value, group = Country),
                                               type = 'line',
                                               marker = list(symbol = 'circle'),
                                               visible = c( rep(T,5), rep(F,length(tmp_top_country_ex_s)-5) )
                                ) %>%
                                hc_xAxis( categories = c( unique( tmp_data_country_ex_s_pc$Year) )   ) %>%
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
                       
                       # 7.8 Key imports market for goods trend line ------------------------------------
                       withProgress(message = 'Loading...', value = (i_prog-1)/tot_step, {
                          # Increment the progress bar, and update the detail text.
                          incProgress( i_prog/tot_step, detail = NULL)
                          ##Sys.sleep(0.1)
                          
                       })
                       i_prog <- i_prog + 1
                       
                       tmp_data_country_im_g <-
                          dtf_shiny_country_gs %>%
                          filter( Type_gs == 'Goods', Type_ie == 'Imports', Year >=2007 ) 
                       
                       ## export markets over $500 million
                       tmp_top_country_im_g <-
                          tmp_data_country_im_g %>%
                          filter( Year == max(Year),
                                  Value >= (10^9)/2, 
                                  !Country %in% c("World", 
                                                  "Destination Unknown - EU")
                          ) %>% ## 1 bn commodity
                          arrange( -Value ) %>%
                          dplyr::select( Country ) %>%
                          as.matrix() %>%
                          as.character
                       
                       tmp_data_country_im_g  %<>%
                          filter( Country %in% tmp_top_country_im_g ) %>%
                          mutate( Value = round(Value/10^6),
                                  Country = factor(Country, levels = tmp_top_country_im_g)
                          ) %>%
                          arrange( Country, Year )
                       
                       ### plot
                       output$ImGMarketLine <- renderHighchart({
                          highchart() %>%
                             hc_exporting(enabled = TRUE, formAttributes = list(target = "_blank")) %>%
                             hc_add_series( data =  tmp_data_country_im_g ,
                                            mapping = hcaes(  x = Year, y = Value, group = Country),
                                            type = 'line',
                                            marker = list(symbol = 'circle') ,
                                            visible = c( rep(T,5), rep(F,length(tmp_top_country_im_g)-5) )
                             ) %>%
                             hc_xAxis( categories = c( unique( tmp_data_country_im_g$Year) ) ) %>%
                             hc_yAxis( title = list(text = "$ million, NZD"), #"Imports markets over $500mn for goods"
                                       labels = list( format = "${value:,.0f} m")  ) %>%
                             hc_plotOptions(line = list(
                                dataLabels = list(enabled = F),
                                #stacking = "normal",
                                enableMouseTracking = T)
                             )%>%
                             hc_tooltip(table = TRUE,
                                        sort = TRUE,
                                        pointFormat = paste0( '<br> <span style="color:{point.color}">\u25CF</span>',
                                                              " {series.name}: ${point.y} m"),
                                        headerFormat = '<span style="font-size: 13px">Year {point.key}</span>'
                             ) %>%
                             hc_legend( layout = 'vertical', align = 'left', verticalAlign = 'top', floating = T, x = 100, y = -15 )
                       })
                       
                       # 7.8.1 Key imports market for goods trend line Percent ------------------------------------
                       withProgress(message = 'Loading...', value = (i_prog-1)/tot_step, {
                          # Increment the progress bar, and update the detail text.
                          incProgress( i_prog/tot_step, detail = NULL)
                          ##Sys.sleep(0.1)
                          
                       })
                       i_prog <- i_prog + 1
                       
                       tmp_data_tot_im_g <-
                          dtf_shiny_country_gs %>%
                          filter( Year>=2007 ) %>%
                          filter( Type_gs == 'Goods', Type_ie == 'Imports', Country == 'World' ) 
                       
                       tmp_data_country_im_g_pc <-
                          tmp_data_country_im_g %>%
                          bind_rows( tmp_data_tot_im_g ) %>%
                          group_by( Year, Type_ie, Type_gs, Commodity, Note ) %>%
                          mutate( Share = Value/(Value[Country=='World']/10^6) ) %>%
                          ungroup %>%
                          mutate( Value = Share*100 ) %>%
                          filter( Country != 'World' ) %>%
                          mutate( Country = factor(Country, levels = tmp_top_country_im_g) ) %>%
                          arrange( Country, Year )
                       
                       output$ImGMarketLinePercent <-
                          renderHighchart({
                             highchart() %>%
                                hc_exporting(enabled = TRUE, formAttributes = list(target = "_blank")) %>%
                                hc_add_series( data =  tmp_data_country_im_g_pc ,
                                               mapping = hcaes(  x = Year, y = Value, group = Country),
                                               type = 'line',
                                               marker = list(symbol = 'circle'),
                                               visible = c( rep(T,5), rep(F,length(tmp_top_country_im_g)-5) )
                                ) %>%
                                hc_xAxis( categories = c( unique( tmp_data_country_im_g_pc$Year) )   ) %>%
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
                       
                       # 7.9 Key imports market for services trend line ------------------------------------
                       withProgress(message = 'Loading...', value = (i_prog-1)/tot_step, {
                          # Increment the progress bar, and update the detail text.
                          incProgress( i_prog/tot_step, detail = NULL)
                          ##Sys.sleep(0.1)
                          
                       })
                       i_prog <- i_prog + 1
                       
                       tmp_data_country_im_s <-
                          dtf_shiny_country_gs %>%
                          filter( Type_gs == 'Services', Type_ie == 'Imports', Year >=2007 ) 
                       
                       ## export markets over $500 million
                       tmp_top_country_im_s <-
                          tmp_data_country_im_s %>%
                          filter( Year == max(Year),
                                  Value >= (10^9)/2, 
                                  !Country %in% c("World", 
                                                  "Destination Unknown - EU")
                          ) %>% ## 1 bn commodity
                          arrange( -Value ) %>%
                          dplyr::select( Country ) %>%
                          as.matrix() %>%
                          as.character
                       
                       tmp_data_country_im_s  %<>%
                          filter( Country %in% tmp_top_country_im_s ) %>%
                          mutate( Value = round(Value/10^6),
                                  Country = factor(Country, levels = tmp_top_country_im_s)
                          ) %>%
                          arrange( Country, Year )
                       
                       ### plot
                       output$ImSMarketLine <- 
                          renderHighchart({
                             highchart() %>%
                                hc_exporting(enabled = TRUE, formAttributes = list(target = "_blank")) %>%
                                hc_add_series( data =  tmp_data_country_im_s ,
                                               mapping = hcaes(  x = Year, y = Value, group = Country),
                                               type = 'line',
                                               marker = list(symbol = 'circle') ,
                                               visible = c( rep(T,5), rep(F,length(tmp_top_country_im_s)-5) )
                                ) %>%
                                hc_xAxis( categories = c( unique( tmp_data_country_im_s$Year) ) ) %>%
                                hc_yAxis( title = list(text = "$ million, NZD"), #"Imports markets over $500mn for services"
                                          labels = list( format = "${value:,.0f} m")  ) %>%
                                hc_plotOptions(line = list(
                                   dataLabels = list(enabled = F),
                                   #stacking = "normal",
                                   enableMouseTracking = T)
                                )%>%
                                hc_tooltip(table = TRUE,
                                           sort = TRUE,
                                           pointFormat = paste0( '<br> <span style="color:{point.color}">\u25CF</span>',
                                                                 " {series.name}: ${point.y} m"),
                                           headerFormat = '<span style="font-size: 13px">Year {point.key}</span>'
                                ) %>%
                                hc_legend( layout = 'vertical', align = 'left', verticalAlign = 'top', floating = T, x = 100, y = -15 )
                          })
                       
                       # 7.9.1 Key imports market for services trend line Percent ------------------------------------
                       withProgress(message = 'Loading...', value = (i_prog-1)/tot_step, {
                          # Increment the progress bar, and update the detail text.
                          incProgress( i_prog/tot_step, detail = NULL)
                          ##Sys.sleep(0.1)
                          
                       })
                       i_prog <- i_prog + 1
                       
                       tmp_data_tot_im_s <-
                          dtf_shiny_country_gs %>%
                          filter( Year>=2007 ) %>%
                          filter( Type_gs == 'Services', Type_ie == 'Imports', Country == 'World' ) 
                       
                       tmp_data_country_im_s_pc <-
                          tmp_data_country_im_s %>%
                          bind_rows( tmp_data_tot_im_s ) %>%
                          group_by( Year, Type_ie, Type_gs, Commodity, Note ) %>%
                          mutate( Share = Value/(Value[Country=='World']/10^6) ) %>%
                          ungroup %>%
                          mutate( Value = Share*100 ) %>%
                          filter( Country != 'World' ) %>%
                          mutate( Country = factor(Country, levels = tmp_top_country_im_s) ) %>%
                          arrange( Country, Year )
                       
                       output$ImSMarketLinePercent <-
                          renderHighchart({
                             highchart() %>%
                                hc_exporting(enabled = TRUE, formAttributes = list(target = "_blank")) %>%
                                hc_add_series( data =  tmp_data_country_im_s_pc ,
                                               mapping = hcaes(  x = Year, y = Value, group = Country),
                                               type = 'line',
                                               marker = list(symbol = 'circle'),
                                               visible = c( rep(T,5), rep(F,length(tmp_top_country_im_s)-5) )
                                ) %>%
                                hc_xAxis( categories = c( unique( tmp_data_country_im_s_pc$Year) )   ) %>%
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
                       
                       ## insert all UIs ----------
                       insertUI(
                          selector = '#show_more_detail',
                          ui = div( id = 'conents_for_more_detail',
                                    ## 1.4 Treemap on Commodities and services --------------------
                                    h2(paste0('Key commodities and services')),
                                    tags$a(href = 'http://archive.stats.govt.nz/browse_for_stats/industry_sectors/imports_and_exports.aspx', "Key commodities and services are defined by Stats NZ", target = "_blank"),
                                    fluidRow( highchartOutput('KeyExTM')  ) %>% withSpinner(type=4) ,
                                    fluidRow( highchartOutput('KeyImTM')  ),
                                    
                                    
                                    ## 1.5 Line chart key commodities and services exports that are over $1bn the most recent years -------------
                                    h2(paste0('Trends of key commodities and services')),
                                    p("Click on the commodity or service names in the legend area to show their trends"),
                                    fluidRow( h3("key commodities and services EXPORTS over $1bn", align = 'center'),
                                              column( width = 6, h4("Export values"), highchartOutput('KeyExLine')  ),
                                              column( width = 6, h4("As a percentage of total exports"), highchartOutput('KeyExLinePercent')  ) ),
                                    fluidRow( h3("key commodities and services IMPORTS over $1bn", align = 'center'),
                                              column( width = 6, h4("Import values"), highchartOutput('KeyImLine') ),
                                              column( width = 6, h4("As a percentage of total imports"), highchartOutput('KeyImLinePercent')  ) ),
                                    
                                    ## 1.6 World map on total exports by country -----------------
                                    h2(paste0('Global trading partners at a glance')),
                                    HTML( "<p> The map shows New Zealand's trading partners. The size of bubble area represents the magnitude of two way trade.
                                          <span style='color:green'> Green </span> and <span style='color:red'> red </span> color indicate whether is trade
                                          <span style='color:green'> surplus </span> or <span style='color:red'> deficit. </span> </p>" ),
                                    fluidRow( uiOutput('TradeMap') ),
                                    
                                    ## 1.6.1 FTA time line  -----------------
                                    h2(paste0('Free trade agreements in force')),
                                    tags$p( "The timeline below shows when the FTAs negotiation started and then put into force. 
                                            Click on each FTA's name for more information.",
                                            tags$b( "In addition, you can select the 'FTA in force' market group under the Market Intelligence panel to get more insights." )
                                    ),
                                    tags$p("FTA stands for free trade agreement; CER stands for closer economic relations; CEP stands for closer economic partnership, and P4
                                           is short for the Trans-pacific Strategic Economic Partnership."),
                                    fluidRow( timevisOutput("FTATimeLine")   ),
                                    
                                    ## 1.7 Trend of key export markets --------------------
                                    h2(paste0('Trends of key trading partners')),
                                    p("Only top 5 markets are shown. Click on the country names in the legend area to show/hide their trends"),
                                    
                                    ## Two way trade and trade balance
                                    fluidRow( h3("Two-way trade and trade surplus/deficit", align = 'center'),
                                              column( width = 6, h4("key markets with two-way trade over $2b"), highchartOutput("TwowayMarketLine") ),
                                              column( width = 6, h4("key markets with trade surplus/deficit over $500m"), highchartOutput("BalanceMarketLine") )
                                    ),
                                    
                                    ## total Exports
                                    fluidRow( h3("Total exports, goods exports and services exports" ,align = 'center'),
                                              h4(tags$b("Total exports: key EXPORTS markets over $1b")),
                                              column( width = 6, h4("Export values"), highchartOutput("ExMarketLine") ),
                                              column( width = 6, h4("As a percentage of total exports"), highchartOutput("ExMarketLinePercent") )
                                              #column( width = 6, h4("key IMPORTS markets over $1b"), highchartOutput("ImMarketLine") )
                                    ),
                                    
                                    fluidRow( h4(tags$b("Goods exports: key EXPORTS markets for GOODS over $500m")),
                                              column( width = 6, h4("Export values"), highchartOutput("ExGMarketLine") ),
                                              column( width = 6, h4("As a percentage of goods exports"), highchartOutput("ExGMarketLinePercent") )
                                              #column( width = 6, h4("key IMPORTS markets over $1b"), highchartOutput("ImMarketLine") )
                                    ),
                                    
                                    fluidRow( h4(tags$b("Services exports: key EXPORTS markets for SERVICES over $500m")),
                                              column( width = 6, h4("Export values"), highchartOutput("ExSMarketLine") ),
                                              column( width = 6, h4("As a percentage of services exports"), highchartOutput("ExSMarketLinePercent") )
                                              #column( width = 6, h4("key IMPORTS markets over $1b"), highchartOutput("ImMarketLine") )
                                    ),
                                    
                                    ## total Imports
                                    fluidRow( h3("Total imports, goods imports and services imports" ,align = 'center'),
                                              h4(tags$b("Total imports: key IMPORTS markets over $1b")),
                                              column( width = 6, h4("Import values"), highchartOutput("ImMarketLine") ),
                                              column( width = 6, h4("As a percentage of total imports"), highchartOutput("ImMarketLinePercent") )
                                              #column( width = 6, h4("key IMPORTS markets over $1b"), highchartOutput("ImMarketLine") )
                                    ),
                                    
                                    fluidRow( h4(tags$b("Goods imports: key IMPORTS markets for GOODS over $500m")),
                                              column( width = 6, h4("Import values"), highchartOutput("ImGMarketLine") ),
                                              column( width = 6, h4("As a percentage of goods imports"), highchartOutput("ImGMarketLinePercent") )
                                              #column( width = 6, h4("key IMPORTS markets over $1b"), highchartOutput("ImMarketLine") )
                                    ),
                                    
                                    fluidRow(h4(tags$b("Services imports: key IMPORTS markets for SERVICES over $500m")),
                                             column( width = 6, h4("Import values"), highchartOutput("ImSMarketLine") ),
                                             column( width = 6, h4("As a percentage of services imports"), highchartOutput("ImSMarketLinePercent") )
                                             #column( width = 6, h4("key IMPORTS markets over $1b"), highchartOutput("ImMarketLine") )
                                    )
                                    )
                       )
                       
                       ## hide load more message ---
                       ## --- show loading message ---
                       shinyjs::hide( id = "load_more_message" )
                    })
      
      
      ## II. Commodity intelligence ----------------------
      ### 1.2 Exports ------ when press the Build Report button ------------------
      observeEvent( input$btn_build_commodity_report_ex,
                    {

                       ## 1.1 check the inputs are correct ---------------
                       tmp_execution_pre_define <- tmp_execution_self_define <- FALSE
                       
                       ## 1.2 work on Pre-deinfed Warning if no pre-defined commodity is selected ------------------------
                       if(input$rbtn_prebuilt_diy_ex=='Pre-defined' & is.null(input$select_comodity_ex)) {
                          showModal(modalDialog(
                             title = "Warning",
                             tags$b("Please select one or multiple pre-defined commodities!"),
                             size = 's'
                          ))
                       }
                       
                       ### if test pass
                       if(input$rbtn_prebuilt_diy_ex=='Pre-defined' & !is.null(input$select_comodity_ex)) {
                          tmp_execution_pre_define <- TRUE
                       }
                       
                       ## 1.2.1 Build graphs pre-defined commodity --------------
                       if(tmp_execution_pre_define){
                          ## --- hide howto -----
                          shinyjs::hide(id = 'ci_howto_ex')
                          ## show waite message ----
                          shinyjs::show( id = 'wait_message_ci_ex' )
                          ## disable the buttone ---
                          shinyjs::disable("btn_build_commodity_report_ex")
                          ## disable the selection  ---
                          shinyjs::disable("select_comodity_ex")
                          shinyjs::disable("rbtn_prebuilt_diy_ex")
                          
                          
                          ### work on Data noW!!!!!!!
                          tmp_selected_service <- setdiff( input$select_comodity_ex , list_snz_commodity_ex[['Goods']] )

                          snz_hs <- concord_snz_eg$HS_codes[concord_snz_eg$SNZ_commodity %in% input$select_comodity_ex ]
                          
                          if( length(tmp_selected_service) >=1 ){
                             hs_group <-
                                concord_snz_eg %>%
                                filter( HS_codes %in% snz_hs ) %>%
                                bind_rows( data.frame(HS_codes = tmp_selected_service,
                                                      SNZ_commodity = tmp_selected_service) )
                          }else{
                             hs_group <-
                                concord_snz_eg %>%
                                filter( HS_codes %in% snz_hs )
                          }
                          
                          colnames(hs_group) <- c("HS_code", "HS_group")

                          ## 2.1 Build export value line chart -------------------- 
                          tmp_top_g_ex <-
                             dtf_shiny_commodity_service_ex %>%
                             filter( SNZ_commodity %in% input$select_comodity_ex ,
                                     !SNZ_commodity %in% tmp_selected_service,
                                     SNZ_commodity != 'Confidential data') %>%
                             filter( Year == max(Year)) %>% 
                             arrange( -Value ) %>%
                             dplyr::select( SNZ_commodity ) %>%
                             as.matrix() %>%
                             as.character
                          
                          tmp_top_s_ex <-
                             dtf_shiny_commodity_service_ex %>%
                             filter( SNZ_commodity %in% tmp_selected_service ) %>%
                             filter( Year == max(Year) ) %>%
                             arrange( -Value ) %>%
                             dplyr::select( SNZ_commodity ) %>%
                             as.matrix() %>%
                             as.character
                          
                          ## top selected commodities and top 5services
                          tmp_top_ex <- c( tmp_top_g_ex, tmp_top_s_ex)
                          
                          ## data frame to plot
                          tmp_dtf_key_line_ex <- 
                             dtf_shiny_commodity_service_ex %>%
                             filter( SNZ_commodity %in% tmp_top_ex,
                                     Year >=2007) %>%
                             mutate( Value = round(Value/10^6),
                                     SNZ_commodity = factor(SNZ_commodity, levels = tmp_top_ex)
                             ) %>%
                             arrange( SNZ_commodity )
                          
                          ### plot
                          tmp_export_hc <- 
                             highchart() %>%
                             hc_exporting(enabled = TRUE, formAttributes = list(target = "_blank")) %>%
                             hc_xAxis( categories = c( unique( tmp_dtf_key_line_ex$Year) ) ) %>%
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
                             hc_legend( layout = 'vertical', align = 'left', verticalAlign = 'top', floating = T, x = 100, y = -15 )
                          
                          ### if any services are selected?
                          if( length(tmp_top_g_ex)>=1&length(tmp_top_s_ex)==0 ) {
                             output$CIExportValueLine <- 
                                renderHighchart({
                                   tmp_export_hc %>%
                                      hc_add_series( data =  tmp_dtf_key_line_ex %>% filter( Type_gs == 'Goods' ) ,
                                                     mapping = hcaes(  x = Year, y = Value, group = SNZ_commodity ),
                                                     type = 'line',
                                                     marker = list(symbol = 'circle') #,
                                                     #visible = c(T,rep(F,length(tmp_top_g_ex)-1))
                                      )
                                })
                          }
                          if( length(tmp_top_g_ex)==0 & length(tmp_top_s_ex)>=1 ){
                             output$CIExportValueLine <- 
                                renderHighchart(
                                   tmp_export_hc %>%
                                      hc_add_series( data =  tmp_dtf_key_line_ex %>% filter( Type_gs == 'Services' ),
                                                     mapping = hcaes(  x = Year, y = Value, group = SNZ_commodity ),
                                                     type = 'line', dashStyle = 'DashDot', marker = list(symbol = 'circle') #,
                                                     #visible = c(T,rep(F,length(tmp_top_s_ex)-1))
                                      )
                                )
                          }
                          if( length(tmp_top_g_ex)>=1 & length(tmp_top_s_ex)>=1 ){
                             output$CIExportValueLine <- 
                                renderHighchart(
                                   tmp_export_hc %>%
                                      hc_add_series( data =  tmp_dtf_key_line_ex %>% filter( Type_gs == 'Goods' ) ,
                                                     mapping = hcaes(  x = Year, y = Value, group = SNZ_commodity ),
                                                     type = 'line',
                                                     marker = list(symbol = 'circle') #,
                                                     #visible = c(T,rep(F,length(tmp_top_g_ex)-1))
                                      ) %>%
                                      hc_add_series( data =  tmp_dtf_key_line_ex %>% filter( Type_gs == 'Services' ),
                                                     mapping = hcaes(  x = Year, y = Value, group = SNZ_commodity ),
                                                     type = 'line', dashStyle = 'DashDot', marker = list(symbol = 'circle') #,
                                                     #visible = c(T,rep(F,length(tmp_top_s_ex)-1))
                                      )
                                )
                          }
                          
                          ## 2.2 build export as a percent of total export line chart -----------------------
                          tmp_tot_ex <-
                             dtf_shiny_full %>%
                             filter( Country == 'World',
                                     Type_ie == 'Exports',
                                     Year >= 2007 )  %>%
                             mutate( Value = round(Value/10^6) ) %>%
                             group_by( Year, Country, Type_ie ) %>%
                             summarize( Value = sum(Value, na.rm=T) ) %>%
                             ungroup %>%
                             mutate( SNZ_commodity = 'Total exports' )
                          
                          tmp_dtf_percent_line_ex <-
                             tmp_dtf_key_line_ex %>%
                             bind_rows( tmp_tot_ex ) %>%
                             group_by( Year, Country, Type_ie ) %>%
                             mutate( Share = Value/Value[SNZ_commodity=='Total exports'],
                                     Value = Share*100 ) %>%
                             ungroup %>%
                             filter( SNZ_commodity != 'Total exports' ) %>%
                             mutate( SNZ_commodity = factor(SNZ_commodity, levels = tmp_top_ex) ) %>%
                             arrange( SNZ_commodity )
                          
                          ### plot
                          tmp_export_percent_hc <- 
                             highchart() %>%
                             hc_exporting(enabled = TRUE, formAttributes = list(target = "_blank")) %>%
                             hc_xAxis( categories = c( unique( tmp_dtf_percent_line_ex$Year) ) ) %>%
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
                             #hc_legend( enabled = FALSE )
                          
                          ### if any services are selected?
                          if( length(tmp_top_g_ex)>=1&length(tmp_top_s_ex)==0 ) {
                             output$CIExportPercentLine <- 
                                renderHighchart(
                                   tmp_export_percent_hc %>%
                                      hc_add_series( data =  tmp_dtf_percent_line_ex %>% filter( Type_gs == 'Goods' ) ,
                                                     mapping = hcaes(  x = Year, y = Value, group = SNZ_commodity ),
                                                     type = 'line',
                                                     marker = list(symbol = 'circle') #,
                                                     #visible = c(T,rep(F,length(tmp_top_g_ex)-1))
                                      )
                                )
                          }
                          if( length(tmp_top_g_ex)==0 & length(tmp_top_s_ex)>=1 ){
                             output$CIExportPercentLine <- 
                                renderHighchart(
                                   tmp_export_percent_hc %>%
                                      hc_add_series( data =  tmp_dtf_percent_line_ex %>% filter( Type_gs == 'Services' ),
                                                     mapping = hcaes(  x = Year, y = Value, group = SNZ_commodity ),
                                                     type = 'line', dashStyle = 'DashDot', marker = list(symbol = 'circle') #,
                                                     #visible = c(T,rep(F,length(tmp_top_s_ex)-1))
                                      )
                                )
                          }
                          if( length(tmp_top_g_ex)>=1 & length(tmp_top_s_ex)>=1 ){
                             output$CIExportPercentLine <- 
                                renderHighchart(
                                   tmp_export_percent_hc %>%
                                      hc_add_series( data =  tmp_dtf_percent_line_ex %>% filter( Type_gs == 'Goods' ) ,
                                                     mapping = hcaes(  x = Year, y = Value, group = SNZ_commodity ),
                                                     type = 'line',
                                                     marker = list(symbol = 'circle') #,
                                                     #visible = c(T,rep(F,length(tmp_top_g_ex)-1))
                                      ) %>%
                                      hc_add_series( data =  tmp_dtf_percent_line_ex %>% filter( Type_gs == 'Services' ),
                                                     mapping = hcaes(  x = Year, y = Value, group = SNZ_commodity ),
                                                     type = 'line', dashStyle = 'DashDot', marker = list(symbol = 'circle') #,
                                                     #visible = c(T,rep(F,length(tmp_top_s_ex)-1))
                                      )
                                )
                          }
                          
                          ## !!!!! try UI insert ----------- 
                          insertUI(
                             selector = '#body_ex',
                             ui =   div( id = 'body_ex_line_value_percent',
                                         fluidRow( h1("Exports for selected commodities/services"),
                                                   p("Click on the commodity or service names in the legend area to show their trends"),
                                                   column(6, div(id = "body_value_ex", h4("Export values"), highchartOutput('CIExportValueLine') ) ),
                                                   column(6, div(id = "body_percent_ex", h4("As a percent of total exports"), highchartOutput('CIExportPercentLine') ) ))
                                       )
                          )
                          ## end Try UI insert --------##

                          ## 2.3 build export value change table ----------------
                          ## data frame to plot
                          tmp_dtf_key_tab_ex <- 
                             dtf_shiny_commodity_service_ex %>%
                             filter( SNZ_commodity %in% tmp_top_ex) %>%
                             mutate( SNZ_commodity = factor(SNZ_commodity, levels = tmp_top_ex) ) %>%
                             arrange( SNZ_commodity )
                          
                          tmp_tab <-
                             tmp_dtf_key_tab_ex %>%
                             mutate( Name =  SNZ_commodity ) %>%
                             group_by( Name) %>%
                             mutate( CAGR1 = CAGR( Value[Year == max(Year)]/
                                                      Value[Year == (max(Year)-1)], 1)/100,
                                     CAGR5 = CAGR( Value[Year == max(Year)]/
                                                      Value[Year == (max(Year)-5)], 5)/100,
                                     CAGR10 = CAGR( Value[Year == max(Year)]/
                                                       Value[Year == (max(Year)-10)], 10)/100,
                                     ABS5 = Value[Year == max(Year)] - Value[Year == (max(Year)-5)],
                                     ABS10 = Value[Year == max(Year)] - Value[Year == (max(Year)-10)]
                             ) %>%
                             ungroup %>%
                             filter( Year == max(Year) ) %>%
                             left_join(tmp_dtf_percent_line_ex %>% dplyr::select(-CAGR5, -Value) ) %>%
                             dplyr::select( Name, Value, Share, CAGR1, CAGR5, CAGR10, ABS5, ABS10) %>%
                             mutate( Name = factor(Name, levels = tmp_top_ex),
                                     Value = Value/10^6,
                                     ABS5 = ABS5/10^6,
                                     ABS10 = ABS10/10^6) %>%
                             mutate( Name = factor(Name, levels = tmp_top_ex) ) %>%
                             arrange( Name )
                          
                          ### join back to hs code
                          hs_group_flat <- 
                             hs_group %>%
                             group_by( HS_group ) %>%
                             summarise( HS_code = paste0(HS_code, collapse = '; ') ) %>%
                             ungroup
                             
                          tmp_tab %<>%
                             left_join( hs_group_flat, by = c("Name"= 'HS_group') ) %>%
                             dplyr::select( HS_code, Name, Value, Share, CAGR1, CAGR5, CAGR10, ABS5, ABS10 )
                          

                          ## build table
                          output$GrowthTabSelectedEx <- renderDataTable(
                             datatable( tmp_tab,
                                        rownames = F,
                                        filter = c("top"),
                                        extensions = c('Buttons'
                                                       #, 'FixedColumns'
                                                       ),
                                        options = list(dom = 'Bfltp',# 'Bt', 
                                                       buttons = c('copy', 'csv', 'excel', 'pdf', 'print') #, pageLength = -1, 
                                                       ,scrollX = TRUE
                                                       #,fixedColumns = list(leftColumns = 2) 
                                                       ,autoWidth = T
                                                       ,pageLength = 10
                                                       ,lengthMenu = list(c(10,  -1), list('10', 'All')) ,
                                                       searchHighlight = TRUE,
                                                       search = list(regex = TRUE, caseInsensitive = FALSE )
                                                       ) ,
                                        colnames = c("HS codes", "Classification","Value ($m)", "Share of total exports", 'CAGR1', 'CAGR5', 'CAGR10', 'ABS5', 'ABS10')
                             ) %>%
                              formatStyle(
                                    c('CAGR1', 'CAGR5', 'CAGR10'),
                                    background = styleColorBar( c(0, max(c(tmp_tab$CAGR1,tmp_tab$CAGR5, tmp_tab$CAGR10))*2, na.rm=T) , 'lightblue'),
                                    backgroundSize = '100% 90%',
                                    backgroundRepeat = 'no-repeat',
                                    backgroundPosition = 'center'
                                 ) %>%
                                 formatStyle(c('CAGR1', 'CAGR5', 'CAGR10', 'ABS5', 'ABS10'),
                                             color = JS("value < 0 ? 'darkred' : value > 0 ? 'darkgreen' : 'black'")) %>%
                                 formatPercentage( c('Share','CAGR1', 'CAGR5', 'CAGR10'),digit = 1 ) %>%
                                 formatStyle( columns = c('Name','Value', 'Share', 'CAGR1', 'CAGR5', 'CAGR10', 'ABS5', 'ABS10'), `font-size`= '115%' ) %>%
                                 formatCurrency( columns = c('Value', 'ABS5', 'ABS10'), mark = ' ', digits = 1)
                          )
                          
                          ## !!!!! try UI insert ----------- 
                          insertUI(
                             selector = '#body_growth_ex',
                             ui =   div( id = 'body_ex_growth_tab',
                                         fluidRow( h1("Short, medium, and long term growth for selected commodities/services"),
                                                   p("Compound annual growth rate (CAGR) for the past 1, 5, and 10 years. Absolute value change (ABS) for the past 5 and 10 years."),
                                                   dataTableOutput('GrowthTabSelectedEx')
                                         )
                             )
                          )
                          ## end Try UI insert --------##
                          
                          ## 2.4 Build export by country output groups -------------------
                          ## create a selector for each selected commodity
                          output$CIEXSelectorByMarkets <- renderUI({
                             selectizeInput("select_comodity_ex_for_market_analysis",
                                            tags$p("Please select or search a commodity for its market analysis"), 
                                            choices =  c('Please select a commodity' = "" , 
                                                         tmp_tab$Name[input$GrowthTabSelectedEx_rows_all]
                                                         ), #input$select_comodity_ex,
                                            selected = NULL,  width = "500px",
                                            multiple = F #,
                                            # options = list(
                                            #    placeholder = 'Please select a commodity',
                                            #    onInitialize = I('function() { this.setValue(" "); }')
                                            #             ) 
                                            )
                          })
                          
                          ### build data for market analysis -- these has to be reactive values
                          ## The name of the selected commodity
                          tmp_selected_ex <- 
                             reactive({
                                input$select_comodity_ex_for_market_analysis
                                })
                          
                          ## The HS codes of the selected commodity
                          tmp_hs_ex <- 
                             reactive({
                                hs_group$HS_code[hs_group$HS_group == tmp_selected_ex()]
                                })
                          
                          ## The data from of the selected commodity by markets
                          tmp_dtf_market_ex <- 
                             reactive({
                                dtf_shiny_full %>%
                                   filter( Commodity %in% tmp_hs_ex(), 
                                           Year >= 2007,
                                           Type_ie == 'Exports') %>%
                                   left_join( concord_country_iso_latlon_raw, by = 'Country' ) %>%
                                   group_by( Year, Country, Type_ie, Type_gs, Note, ISO2, lat, lon ) %>%
                                   summarize( Value = sum(Value, na.rm=T) ) %>%
                                   ungroup %>%
                                   mutate( Commodity = as.character( tmp_selected_ex() ) )
                             })
                          
                          ### selcted commodity and service outputs
                          output$SelectedEx <- 
                             renderText({
                                tmp_selected_ex()
                             })
                          
                          ## !!!!! try UI insert ----------- 
                          insertUI(
                             selector = '#body_ci_markets_ex',
                             ui =   div( id = 'body_ci_markets_ex_selector',
                                         fluidRow(h1("Export markets analysis for selected commodity/service"),
                                                  uiOutput("CIEXSelectorByMarkets") ),
                                         fluidRow( shiny::span(h1( HTML(paste0(textOutput("SelectedEx"))), align = "center" ), style = "color:darkblue" ) )
                             )
                          )
                          ## end Try UI insert --------##
                          
                          ## --- show loading message ------------------
                          observe({
                             if( any(input$select_comodity_ex_for_market_analysis %in% tmp_tab$Name)   ){
                                shinyjs::show( id = "body_ci_market_loading_message" )
                             }
                          })
                          ## finish
                          
                          ### 2.4.1 build highchart map  ---------------------------
                          print("--------- Building highchart map -------------")
                          
                          tmp_dtf_market_ex_map <- 
                             reactive({
                                tmp_dtf_market_ex() %>%
                                   filter( Year == max(Year),
                                           !is.na(lat) ) %>%
                                   mutate( Value = Value/10^6,
                                           z= Value,
                                           name = Country)
                             })
                          
                          ## plot map
                          output$MapEXMarket <- 
                             renderHighchart({
                                if( input$select_comodity_ex_for_market_analysis == "" ) 
                                   return(NULL)
                                
                                hcmap( data = tmp_dtf_market_ex_map() ,
                                       value = 'Value',
                                       joinBy = c('iso-a2','ISO2'), 
                                       name="Exports value",
                                       borderWidth = 1,
                                       borderColor = "#fafafa",
                                       nullColor = "lightgrey",
                                       tooltip = list( table = TRUE,
                                                       sort = TRUE,
                                                       headerFormat = '<span style="font-size:13px">{series.name}</span><br/>',
                                                       pointFormat = '{point.name}: <b>${point.value:,.1f} m</b>' )
                                ) %>%
                                hc_add_series(data =  tmp_dtf_market_ex_map(),
                                                 type = "mapbubble",
                                                 color  = hex_to_rgba("#f1c40f", 0.9),
                                                 minSize = 0,
                                                 name="Exports value",
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
                          
                          ## !!!!! try UI insert ----------- 
                          output$H2_map_of_export_title <-
                             renderText({
                                if( input$select_comodity_ex_for_market_analysis == "" ) 
                                   return(NULL)
                                paste0("Map of export values")
                             })
                          
                          output$H2_map_of_export_title_note <-
                             renderText({
                                if( input$select_comodity_ex_for_market_analysis == "" ) 
                                   return(NULL)
                                paste0("The size of bubble area and color both represent the value of exports.")
                             })
                           
                          ## inserter UI here  
                          insertUI(
                             selector = '#body_ci_markets_ex',
                             ui =   div( id = 'body_ci_markets_ex_map',
                                         fluidRow(h2( HTML(paste0(textOutput("H2_map_of_export_title"))) ) ,
                                                  p( HTML(paste0(textOutput("H2_map_of_export_title_note")))  ),
                                                  highchartOutput('MapEXMarket') )
                             )
                          )
                          ## end Try UI insert --------##
                          
                          ### 2.4.2 Top markets for selected commodity line chart ----------------
                          print("--------- Building Top market line chart -------------")
                          tmp_top_country_selected_ex <- 
                             reactive({
                                tmp_dtf_market_ex() %>%
                                   filter( Year == max(Year),
                                           Value > 0 , 
                                           !Country %in% c("World", 
                                                           "Destination Unknown - EU")
                                   ) %>% ## 1 bn commodity
                                   arrange( -Value ) %>%
                                   dplyr::select( Country ) %>%
                                   as.matrix() %>%
                                   as.character
                             })

                          tmp_top10_country_selected_ex <-
                             reactive({
                                tmp_top_country_selected_ex()[1:min(10,length(tmp_top_country_selected_ex()))]
                             })
                         
                          
                          ### derive datafrom for the line plot
                          tmp_dtf_market_ex_line <- 
                             reactive({
                                tmp_dtf_market_ex() %>%
                                   filter( Country %in%  as.character(tmp_top_country_selected_ex()) ) %>%
                                   mutate( Value = Value/10^6 ,
                                           Country = factor(Country, levels = as.character(tmp_top_country_selected_ex()) )
                                          ) %>%
                                   arrange(Country)
                             })
                          

                          ## line plot
                          output$SelectedExMarketLine <- renderHighchart({
                             if( input$select_comodity_ex_for_market_analysis == "" ) 
                                return(NULL)
                             
                             highchart() %>%
                                hc_exporting(enabled = TRUE, formAttributes = list(target = "_blank")) %>%
                                hc_add_series( data =  tmp_dtf_market_ex_line() %>%
                                                  filter( Country %in% as.character(tmp_top10_country_selected_ex()) ),
                                               mapping = hcaes(  x = Year, y = Value, group = Country),
                                               type = 'line',
                                               marker = list(symbol = 'circle'), 
                                               visible = c( rep(T,5), rep(F,length( as.character(tmp_top10_country_selected_ex()) )-5) )
                                ) %>%
                                hc_xAxis( categories = c( unique( tmp_dtf_market_ex_line()$Year) ) ) %>%
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

                          ### 2.4.3 Top markets for selected commodity percent line chart -------------------
                          print("--------- Building Top market line chart (Percent) -------------")
                          tmp_dtf_market_ex_line_percent <- 
                             reactive({
                                tmp_dtf_market_ex_line() %>%
                                   group_by(Year, Type_ie, Type_gs, Note, Commodity) %>%
                                   mutate( Share = Value/sum(Value, na.rm=T)) %>%
                                   ungroup %>%
                                   mutate( Value = Share*100 ) 
                             })
                          
                          output$SelectedExMarketLinePercent <-
                             renderHighchart({
                                if( input$select_comodity_ex_for_market_analysis == "" ) 
                                   return(NULL)
                                
                                highchart() %>%
                                   hc_exporting(enabled = TRUE, formAttributes = list(target = "_blank")) %>%
                                   hc_add_series( data =  tmp_dtf_market_ex_line_percent() %>%
                                                     filter( Country %in% as.character(tmp_top10_country_selected_ex()) ),
                                                  mapping = hcaes(  x = Year, y = Value, group = Country),
                                                  type = 'line',
                                                  marker = list(symbol = 'circle'), 
                                                  visible = c( rep(T,5), rep(F,length( as.character(tmp_top10_country_selected_ex()) )-5) )
                                   ) %>%
                                   hc_xAxis( categories = c( unique( tmp_dtf_market_ex_line_percent()$Year) ) ) %>%
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
                          
                          ## !!!!! try UI insert ----------- 
                          output$H2_export_market_trend_title <-
                             renderText({
                                if( input$select_comodity_ex_for_market_analysis == "" )
                                   return(NULL)
                                paste0("Top 10 export markets trends")
                             })
                          
                          output$H2_export_market_trend_title_note <-
                             renderText({
                                if( input$select_comodity_ex_for_market_analysis == "" ) 
                                   return(NULL)
                                paste0("Click on the country names in the legend area to show their trends")
                             })
                          
                          output$H4_export_market_trend_value_title <-
                             renderText({
                                if( input$select_comodity_ex_for_market_analysis == "" ) 
                                   return(NULL)
                                paste0("Export values")
                             })
                          
                          output$H4_export_market_trend_percent_title <-
                             renderText({
                                if( input$select_comodity_ex_for_market_analysis == "" ) 
                                   return(NULL)
                                paste0("As a percent of total exports of the selected")
                             })
                          
                          insertUI(
                             selector = '#body_ci_markets_ex',
                             ui =   div( id = 'body_ci_markets_ex_top',
                                         fluidRow( h2( HTML(paste0(textOutput("H2_export_market_trend_title"))) ),
                                                   p( HTML(paste0(textOutput("H2_export_market_trend_title_note"))) ),
                                                   column(6, 
                                                          h4( HTML(paste0(textOutput("H4_export_market_trend_value_title"))) ),
                                                          highchartOutput("SelectedExMarketLine") 
                                                   ),
                                                   column(6,
                                                          h4( HTML(paste0(textOutput("H4_export_market_trend_percent_title"))) ),
                                                          highchartOutput("SelectedExMarketLinePercent")
                                                   )
                                         )
                             )
                          )
                          ## end Try UI insert --------##
                          
                          ### 2.4.4 Growth prospective tab ----------------------
                          print("--------- Building Grwoth prospective table -------------")
                          tmp_tab_ex_growth <-
                             reactive({
                                tmp_dtf_market_ex_line() %>%
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
                                   mutate( CAGR1 =  as.numeric(CAGR1),
                                           CAGR5 = as.numeric(CAGR5), 
                                           CAGR10 = as.numeric(CAGR10),
                                           ABS5 = as.numeric(ABS5), 
                                           ABS10 = as.numeric(ABS10) 
                                           ) %>%
                                   #filter( Year == max(Year) ) %>%
                                   left_join( tmp_dtf_market_ex_line() %>% rename(Name = Country) %>% filter( Year == max(Year) )  ) %>%
                                   left_join( tmp_dtf_market_ex_line_percent() %>% dplyr::select( -Value ) %>% rename( Name = Country) %>% filter( Year == max(Year) )  ) %>%
                                   dplyr::select( Name, Value, Share, CAGR1, CAGR5, CAGR10, ABS5, ABS10) %>%
                                   mutate( Name = factor(Name, levels = as.character(tmp_top_country_selected_ex()) ) ) %>%
                                   arrange( Name )
                             })
                          
                          output$SelectedExMarketGrowthTab <- renderDataTable({
                             if( input$select_comodity_ex_for_market_analysis == "" ) 
                                return(NULL)
                             
                             datatable( tmp_tab_ex_growth(),
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
                                   background = styleColorBar( c(0, max(c(tmp_tab_ex_growth()$CAGR1,
                                                                          tmp_tab_ex_growth()$CAGR5,
                                                                          tmp_tab_ex_growth()$CAGR10))*2, na.rm=T) , 'lightblue'),
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
                          
                          ## !!!!! try UI insert ----------- 
                          output$H2_market_ex_growth_tab_title <-
                             renderText({
                                if( input$select_comodity_ex_for_market_analysis == "" ) 
                                   return(NULL)
                                paste0("Top export markets growth prospective")
                             })
                          
                          output$H2_market_ex_growth_tab_title_note <-
                             renderText({
                                if( input$select_comodity_ex_for_market_analysis == "" ) 
                                   return(NULL)
                                paste0("Compound annual growth rate (CAGR) for the past 1, 5, and 10 years. Absolute value change (ABS) for the past 5 and 10 years.")
                             })
                          
                          ## insert ui here
                          insertUI(
                             selector = '#body_ci_markets_ex',
                             ui =   div( id = 'body_ci_markets_ex_growth',
                                         fluidRow( h2( HTML(paste0(textOutput("H2_market_ex_growth_tab_title"))) ),
                                                   p( HTML(paste0(textOutput("H2_market_ex_growth_tab_title_note"))) ),
                                                   dataTableOutput("SelectedExMarketGrowthTab")
                                         )
                             )
                          )
                          ## end Try UI insert --------##
                          
                          
                          ## 2.5 show HS groupings in appendix -------------------
                          # output$HS_pre_ex <- renderDataTable( hs_group,rownames = FALSE, 
                          #                                      extensions = 'Buttons',
                          #                                      options = list(dom = 'Bltp', buttons = c('copy', 'csv', 'excel', 'pdf', 'print'),
                          #                                                     pageLength = 5,
                          #                                                     lengthMenu = list(c(5,  -1), list('5', 'All')) 
                          #                                      ) )
                          ## !!!!! try UI insert ----------- 
                          # insertUI(
                          #    selector = '#body_ci_markets_ex',
                          #    ui =   div( id = 'body_appendix_hs_ex',
                          #                conditionalPanel("input.rbtn_prebuilt_diy_ex == 'Pre-defined'",
                          #                                 fluidRow( tags$h1("Appendix -- HS grouping selected"),
                          #                                           div(id = 'output_hs_pre_ex', dataTableOutput( ("HS_pre_ex") ) )
                          #                                 )
                          #                ),
                          #                
                          #                conditionalPanel( "input.rbtn_prebuilt_diy_ex == 'Self-defined'",
                          #                                  fluidRow( tags$h1("Appendix -- HS grouping uploaded"),
                          #                                            div(id = 'output_hs_ex', dataTableOutput( ("HS_ex") ) )
                          #                                  )
                          #                                  
                          #                )
                          #    )
                          # )
                          ## end Try UI insert --------##
                          
                          
                          
                          
                          
                          ## Tests ------------------------
                          # output$test_ex <- 
                          #    renderText({
                          #       tmp_hs_ex() 
                          #    })
                          # 
                          # output$test_country_ex <- 
                          #    renderDataTable(
                          #       datatable( tmp_dtf_market_ex() )
                          #    )
                          
                          
                          ## 2.6 Data for global situation from UN comtrade (ONLY for Export analysis) ----------------
                          print("--------- Building Reactive values for global analysis -------------")
                          rv_pre_define_ex <- reactiveValues()
                          
                          ## put reactive values into observe  ------
                          observe({
                             ## get data from un com trade using loop--------------
                             ## create a list first
                             print("----------- Download Uncomtrade trade by country --------------")
                             rv_pre_define_ex$Fail_uncomtrade_country <- 
                             try(
                                rv_pre_define_ex$tmp_global_by_country_raw_list <- 
                                   lapply( tmp_hs_ex() ,
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
                             rv_pre_define_ex$Fail_uncomtrade_eu <- 
                             try(
                                rv_pre_define_ex$tmp_global_by_eu_raw_list <- 
                                   lapply( tmp_hs_ex() ,
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
                             if( class(rv_pre_define_ex$Fail_uncomtrade_country) != 'try-error' ){
                                print("----------- Success: Download Uncomtrade trade by country --------------")
                                ## get list to data frame
                                try(
                                rv_pre_define_ex$tmp_global_by_country_raw1 <- 
                                   do.call( rbind, rv_pre_define_ex$tmp_global_by_country_raw_list )
                                )
                                
                                ## change names
                                try(
                                rv_pre_define_ex$tmp_global_by_country_raw <-
                                   rv_pre_define_ex$tmp_global_by_country_raw1 %>%
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
                             
                             
                             # try( 
                             #    rv_pre_define_ex$tmp_global_by_country_raw1 <- 
                             #       do.call( rbind, rv_pre_define_ex$tmp_global_by_country_raw_list )
                             #    )
                             
                             if( class(rv_pre_define_ex$Fail_uncomtrade_eu) != 'try-error' ){
                                print("----------- Success: Download Uncomtrade trade by EU --------------")
                                ## get list to data frame
                                try(
                                rv_pre_define_ex$tmp_global_by_eu_raw1 <- 
                                   do.call( rbind, rv_pre_define_ex$tmp_global_by_eu_raw_list )
                                )
                                
                                ## change names
                                try(
                                rv_pre_define_ex$tmp_global_by_eu_raw <-
                                   rv_pre_define_ex$tmp_global_by_eu_raw1 %>%
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
                             
                             # try( 
                             #    rv_pre_define_ex$tmp_global_by_eu_raw1 <- 
                             #       do.call( rbind, rv_pre_define_ex$tmp_global_by_eu_raw_list )
                             # )   
                             
                             ### get data from un com trade -----
                             # rv_pre_define_ex$Fail_uncomtrade <- 
                             #    try(
                             #       rv_pre_define_ex$tmp_global_by_country_raw <-
                             #          rv_pre_define_ex$tmp_global_by_country_raw1 %>%
                             #          #get.Comtrade(r="all", p="0", rg = "1,2"  ## 1 means imports; 2 means exports (3 is re-exports excluded here)
                             #           #            , ps = paste0(tmp_un_comtrade_max_year, "," ,tmp_un_comtrade_max_year-5)
                             #            #           , cc = paste0(tmp_hs_ex(), collapse = ','), fmt = 'csv' )$data #%>%
                             #          # dplyr::select( yr, cmdCode, rgDesc, rtTitle, rt3ISO, ptTitle, qtDesc,  TradeQuantity, TradeValue) %>%
                             #          # mutate_all( as.character ) %>%
                             #          # mutate( yr = as.numeric(yr),
                             #          #         TradeQuantity = as.numeric( TradeQuantity ),
                             #          #         TradeValue = as.numeric( TradeValue )
                             #          #         ) %>%
                             #          # rename( Year = yr, `Commodity.Code` = cmdCode ,
                             #          #         `Trade.Flow` = rgDesc,
                             #          #         Reporter = rtTitle,
                             #          #         `Reporter.ISO` = rt3ISO,
                             #          #         Partner = ptTitle,
                             #          #         `Qty.Unit` = qtDesc,
                             #          #         `Alt.Qty.Unit` = TradeQuantity,
                             #          #         `Trade.Value..US..` = TradeValue )
                             #       
                             #       # m_ct_search( reporters = "All", partners = 'World', trade_direction = c("imports", "exports"), freq = "annual",
                             #       #              commod_codes = as.character(tmp_hs_ex()),
                             #       #              start_date = tmp_un_comtrade_max_year - 4,
                             #       #              end_date = tmp_un_comtrade_max_year ) %>%
                             #       #    bind_rows( m_ct_search( reporters = "All", partners = 'World', trade_direction = c("imports", "exports"), freq = "annual",
                             #       #                            commod_codes = as.character(tmp_hs_ex()),
                             #       #                            start_date = tmp_un_comtrade_max_year - 5,
                             #       #                            end_date = tmp_un_comtrade_max_year - 5 )
                             #       #               ) %>%
                             #          #filter( year >= tmp_un_comtrade_max_year-5 &
                             #           #          year <= tmp_un_comtrade_max_year ) %>%
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
                             
                             ## Eu export to world data
                             # rv_pre_define_ex$Fail_uncomtrade_eu <- 
                             #    try(
                             #       rv_pre_define_ex$tmp_global_by_eu_raw <-
                             #          rv_pre_define_ex$tmp_global_by_eu_raw1 %>%
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
                             
                             ## 
                             if( class(rv_pre_define_ex$Fail_uncomtrade_country) == "try-error" )
                                print(rv_pre_define_ex$Fail_uncomtrade_country)
                             
                             if( class(rv_pre_define_ex$Fail_uncomtrade_eu) == "try-error" )
                                print(rv_pre_define_ex$Fail_uncomtrade_eu)
                             
                             
                             ## when both data downloaded successfully then do -------
                             if( class(rv_pre_define_ex$Fail_uncomtrade_country) != "try-error" & 
                                 class(rv_pre_define_ex$Fail_uncomtrade_eu) != "try-error" &
                                 !is.null(rv_pre_define_ex$tmp_global_by_country_raw)  ){
                                ## 1. format the data -----
                                
                                ## global import and export of A commodity (sum over all HS code under this commodity) by country
                                rv_pre_define_ex$tmp_global_by_country <- 
                                   rv_pre_define_ex$tmp_global_by_country_raw %>%
                                   dplyr::select( Year,`Commodity.Code` , `Trade.Flow`, Reporter, `Reporter.ISO`, Partner, `Qty.Unit`, `Alt.Qty.Unit`, `Trade.Value..US..`) %>%
                                   #group_by(Year, `Trade.Flow`, Reporter, `Reporter.ISO`, Partner, `Qty.Unit`) %>%
                                   group_by(Year, `Trade.Flow`, Reporter, `Reporter.ISO`, Partner) %>%
                                   summarise( `Alt.Qty.Unit` = sum(`Alt.Qty.Unit`, na.rm=T),
                                              `Trade.Value..US..` = sum(`Trade.Value..US..`, na.rm=T)
                                   ) %>%
                                   ungroup %>%
                                   mutate( Price = `Trade.Value..US..`/ `Alt.Qty.Unit`) 
                                
                                ## EU import and export of A commodity from world
                                rv_pre_define_ex$tmp_eu_trade_extra_raw <- 
                                   rv_pre_define_ex$tmp_global_by_eu_raw %>%
                                   dplyr::select( Year,`Commodity.Code` , `Trade.Flow`, Reporter, `Reporter.ISO`, Partner, `Qty.Unit`, `Alt.Qty.Unit`, `Trade.Value..US..`) %>%
                                   #group_by(Year, `Trade.Flow`, Reporter, `Reporter.ISO`, Partner, `Qty.Unit`) %>%
                                   group_by(Year, `Trade.Flow`, Reporter, `Reporter.ISO`, Partner) %>%
                                   summarise( `Alt.Qty.Unit` = sum(`Alt.Qty.Unit`, na.rm=T),
                                              `Trade.Value..US..` = sum(`Trade.Value..US..`, na.rm=T)
                                   ) %>%
                                   ungroup #%>%
                                   #mutate( Price = `Trade.Value..US..`/ `Alt.Qty.Unit`) 
                                
                                ## 5 yr change in value and prices % and abs 
                                rv_pre_define_ex$tmp_global_by_country_change <-    
                                   rv_pre_define_ex$tmp_global_by_country %>%
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
                                rv_pre_define_ex$tmp_global_by_country_all <- 
                                   rv_pre_define_ex$tmp_global_by_country %>%
                                   filter( Year == tmp_un_comtrade_max_year ) %>%
                                   left_join( rv_pre_define_ex$tmp_global_by_country_change ) %>%
                                   group_by( Year, Trade.Flow  ) %>%
                                   mutate( Share = as.numeric(`Trade.Value..US..`)/ sum(as.numeric(`Trade.Value..US..`), na.rm=T ) ) %>%
                                   ungroup %>%
                                   arrange( `Trade.Flow`, -`Trade.Value..US..`) 
                                
                                ## 1.1 formate data -- get Eu28 intra and extra trade for later use in table ------
                                rv_pre_define_ex$tmp_eu_trade_all <- 
                                   rv_pre_define_ex$tmp_global_by_country %>%
                                   filter( Reporter.ISO %in% concord_eu28$ISO3 ) %>%
                                   #group_by( Year , `Trade.Flow`, Partner, `Qty.Unit` ) %>%
                                   group_by( Year , `Trade.Flow`, Partner ) %>%
                                   summarise(  `Alt.Qty.Unit` = sum( as.numeric(`Alt.Qty.Unit`), na.rm=T ),
                                               `Trade.Value..US..` = sum( as.numeric(`Trade.Value..US..`), na.rm=T ) ) %>%
                                   ungroup %>%
                                   mutate( Reporter = "EU-28", Reporter.ISO = 'EU2'   )
                                
                                ## derive EU trade intra
                                rv_pre_define_ex$tmp_eu_trade_intra_raw <-
                                   rv_pre_define_ex$tmp_eu_trade_all %>%
                                   left_join( rv_pre_define_ex$tmp_eu_trade_extra_raw,
                                              #by = c("Year", "Trade.Flow","Reporter", "Reporter.ISO", "Partner","Qty.Unit" )
                                              by = c("Year", "Trade.Flow","Reporter", "Reporter.ISO", "Partner" )
                                   ) %>%
                                   mutate( `Alt.Qty.Unit` = Alt.Qty.Unit.x - Alt.Qty.Unit.y, 
                                           `Trade.Value..US..` =  `Trade.Value..US...x` - `Trade.Value..US...y` ) %>%
                                   dplyr::select( -Alt.Qty.Unit.x, -Alt.Qty.Unit.y, 
                                                  -`Trade.Value..US...x`,  -`Trade.Value..US...y`) #%>%
                                   #mutate( Partner = "EU-28") 
                                
                                ### formate data
                                rv_pre_define_ex$tmp_eu_trade_intra <- 
                                   rv_pre_define_ex$tmp_eu_trade_intra_raw %>%
                                   mutate( Reporter = 'EU-28-Intra', Reporter.ISO = 'EU2-intra' )
                                
                                rv_pre_define_ex$tmp_eu_trade_extra <- 
                                   rv_pre_define_ex$tmp_eu_trade_extra_raw %>%
                                   mutate( Reporter = 'EU-28-Extra', Reporter.ISO = 'EU2-extra' )
                                 
                                ## join EU intra and extra back
                                rv_pre_define_ex$tmp_global_by_country_and_eu <-
                                   rv_pre_define_ex$tmp_global_by_country_raw %>%
                                   filter( !Reporter.ISO %in% concord_eu28$ISO3 ) %>%
                                   dplyr::select( Year,`Commodity.Code` , `Trade.Flow`, Reporter, `Reporter.ISO`, Partner, `Qty.Unit`, `Alt.Qty.Unit`, `Trade.Value..US..`) %>%
                                   #group_by(Year, `Trade.Flow`, Reporter, `Reporter.ISO`, Partner, `Qty.Unit`) %>%
                                   group_by(Year, `Trade.Flow`, Reporter, `Reporter.ISO`, Partner) %>%
                                   summarise( `Alt.Qty.Unit` = sum(`Alt.Qty.Unit`, na.rm=T),
                                              `Trade.Value..US..` = sum(`Trade.Value..US..`, na.rm=T)
                                   ) %>%
                                   ungroup %>%
                                   bind_rows( rv_pre_define_ex$tmp_eu_trade_intra ) %>%
                                   bind_rows( rv_pre_define_ex$tmp_eu_trade_extra  ) %>%
                                   mutate( Price = `Trade.Value..US..`/ `Alt.Qty.Unit`)
                                
                                ## 5 yr change in value and prices % and abs 
                                rv_pre_define_ex$tmp_global_by_country_and_eu_change <-    
                                   rv_pre_define_ex$tmp_global_by_country_and_eu %>%
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
                                rv_pre_define_ex$tmp_global_by_country_and_eu_all <- 
                                   rv_pre_define_ex$tmp_global_by_country_and_eu %>%
                                   filter( Year == tmp_un_comtrade_max_year ) %>%
                                   left_join( rv_pre_define_ex$tmp_global_by_country_and_eu_change ) %>%
                                   group_by( Year, Trade.Flow  ) %>%
                                   mutate( Share = as.numeric(`Trade.Value..US..`)/ sum(as.numeric(`Trade.Value..US..`), na.rm=T ) ) %>%
                                   ungroup %>%
                                   arrange( `Trade.Flow`, -`Trade.Value..US..`) 
                                
                                ## 2. calculate values for later use ------------   
                                ## Global market size -- value now
                                rv_pre_define_ex$tmp_global_size_value_now <- 
                                   rv_pre_define_ex$tmp_global_by_country %>%
                                   group_by(Year, `Trade.Flow`,  Partner ) %>%
                                   summarise(`Trade.Value..US..` = sum(as.numeric(`Trade.Value..US..`), na.rm=T) ) %>%
                                   ungroup %>%
                                   filter( Year == tmp_un_comtrade_max_year,
                                           `Trade.Flow` == 'Import') %>%
                                   dplyr::select( `Trade.Value..US..` ) %>%
                                   as.numeric()
                                
                                ## Global market size -- value 5 years ago
                                rv_pre_define_ex$tmp_global_size_value_pre <- 
                                   rv_pre_define_ex$tmp_global_by_country %>%
                                   group_by(Year, `Trade.Flow`,  Partner ) %>%
                                   summarise(`Trade.Value..US..` = sum(as.numeric(`Trade.Value..US..`), na.rm=T) ) %>%
                                   ungroup %>%
                                   filter( Year == tmp_un_comtrade_max_year-5,
                                           `Trade.Flow` == 'Import') %>%
                                   dplyr::select( `Trade.Value..US..` ) %>%
                                   as.numeric()
                                
                                ## Global market size -- value change %
                                rv_pre_define_ex$tmp_global_size_value_change <-
                                   CAGR( rv_pre_define_ex$tmp_global_size_value_now/
                                            rv_pre_define_ex$tmp_global_size_value_pre, 5)/100
                                
                                ## Global market size -- value change abs
                                rv_pre_define_ex$tmp_global_size_value_change_abs <-
                                   rv_pre_define_ex$tmp_global_size_value_now - rv_pre_define_ex$tmp_global_size_value_pre 
                                
                                ## Top 3 importers share
                                rv_pre_define_ex$tmp_top3_importers_share <-
                                   rv_pre_define_ex$tmp_global_by_country_all %>%
                                   filter( `Trade.Flow` == 'Import' ) %>%
                                   arrange( -Share ) %>%
                                   slice(1:3) %>%
                                   group_by(Year) %>%
                                   summarise( Share = sum(Share, na.rm=T) ) %>%
                                   ungroup %>%
                                   dplyr::select(Share) %>%
                                   as.numeric
                                
                                ## Top 10 importers share
                                rv_pre_define_ex$tmp_top10_importers_share <-
                                   rv_pre_define_ex$tmp_global_by_country_all %>%
                                   filter( `Trade.Flow` == 'Import' ) %>%
                                   arrange( -Share ) %>%
                                   slice(1:10) %>%
                                   group_by(Year) %>%
                                   summarise( Share = sum(Share, na.rm=T) ) %>%
                                   ungroup %>%
                                   dplyr::select(Share) %>%
                                   as.numeric
                                
                                ##  of top 20 markets -- number of high growth market
                                rv_pre_define_ex$tmp_number_high_growth_importers <-
                                   nrow(
                                      rv_pre_define_ex$tmp_global_by_country_all %>%
                                         filter( `Trade.Flow` == 'Import' ) %>%
                                         arrange( -Share ) %>%
                                         slice(1:20) %>%
                                         filter( Value_per_change >= 0.1 )
                                   )
                                
                                ## Top 3 exporters share
                                rv_pre_define_ex$tmp_top3_exporters_share <-
                                   rv_pre_define_ex$tmp_global_by_country_all %>%
                                   filter( `Trade.Flow` == 'Export' ) %>%
                                   arrange( -Share ) %>%
                                   slice(1:3) %>%
                                   group_by(Year) %>%
                                   summarise( Share = sum(Share, na.rm=T) ) %>%
                                   ungroup %>%
                                   dplyr::select(Share) %>%
                                   as.numeric
                                
                                ## Top 10 exporters share
                                rv_pre_define_ex$tmp_top10_exporters_share <-
                                   rv_pre_define_ex$tmp_global_by_country_all %>%
                                   filter( `Trade.Flow` == 'Export' ) %>%
                                   arrange( -Share ) %>%
                                   slice(1:10) %>%
                                   group_by(Year) %>%
                                   summarise( Share = sum(Share, na.rm=T) ) %>%
                                   ungroup %>%
                                   dplyr::select(Share) %>%
                                   as.numeric
                                
                                ## NZ's share
                                rv_pre_define_ex$tmp_nz_share <-
                                   rv_pre_define_ex$tmp_global_by_country_all %>%
                                   filter( `Trade.Flow` == 'Export' ) %>%
                                   filter( Reporter == 'New Zealand' ) %>%
                                   dplyr::select(Share) %>%
                                   as.numeric
                                
                                ## 3. build data for importers and exporter maps -------------------
                                rv_pre_define_ex$tmp_un_comtrade_importer_map <- 
                                   rv_pre_define_ex$tmp_global_by_country_all %>%
                                   filter( `Trade.Flow` == "Import" ) %>%
                                   left_join( concord_uncomtrade_country, by = c('Reporter.ISO' = 'ISO3') ) %>%
                                   filter( !is.na(lat) ) %>%
                                   mutate( Value = `Trade.Value..US..`/10^6,
                                           z= Value,
                                           name = Reporter)
                                
                                rv_pre_define_ex$tmp_un_comtrade_exporter_map <- 
                                   rv_pre_define_ex$tmp_global_by_country_all %>%
                                   filter( `Trade.Flow` == "Export" ) %>%
                                   left_join( concord_uncomtrade_country, by = c('Reporter.ISO' = 'ISO3') ) %>%
                                   filter( !is.na(lat) ) %>%
                                   mutate( Value = `Trade.Value..US..`/10^6,
                                           z= Value,
                                           name = Reporter)
                                
                                ## 4. Build data for the summary table -----------------
                                ## import tab
                                rv_pre_define_ex$tmp_un_comtrade_import_summary_tab <- 
                                   rv_pre_define_ex$tmp_global_by_country_and_eu_all %>%
                                   filter( `Trade.Flow` == 'Import' ) %>%
                                   dplyr::select( Reporter, Share, 
                                                  `Trade.Value..US..` ,Value_per_change, Value_abs_change,  
                                                  Price, Price_per_change ) %>%
                                   mutate( `Trade.Value..US..` = `Trade.Value..US..`/10^6,
                                           Value_abs_change = Value_abs_change/10^6)
                                
                                ## export tab
                                rv_pre_define_ex$tmp_un_comtrade_export_summary_tab <- 
                                   rv_pre_define_ex$tmp_global_by_country_and_eu_all %>%
                                   filter( `Trade.Flow` == 'Export' ) %>%
                                   dplyr::select( Reporter, Share, 
                                                  `Trade.Value..US..` ,Value_per_change, Value_abs_change,  
                                                  Price, Price_per_change ) %>%
                                   mutate( `Trade.Value..US..` = `Trade.Value..US..`/10^6,
                                           Value_abs_change = Value_abs_change/10^6)
                             }
                          })
                          
                          ## 2.6.1 IF hourly query reach 100 ------------
                          # print("--------- Building Fail messages if no data -------------")
                          # output$Un_comtrade_fail_msg <- 
                          #    renderUI({
                          #       if( is.null(rv_pre_define_ex$tmp_global_by_country_raw)  )
                          #          tags$h1( "Global analysis cannot be performed due to reaching usage limit of 100 requests per hour. Please come back in a hour time." )
                          #    })
                          # 
                          # insertUI(selector = '#body_ci_markets_ex',
                          #          ui = div(id = "#body_ci_markets_ex_fail_msg",
                          #                   uiOutput("Un_comtrade_fail_msg")
                          #                   )
                          #          )
   
                          
                          ## 2.7 UN com Trade data analysis starts here Key facts table ----------
                          ## world market size
                          print("--------- Building facts value boxes -------------")
                          output$Un_comtrade_world_market_size_pre_define <-
                             renderInfoBox({
                                if( is.null(rv_pre_define_ex$tmp_global_by_country_raw)  )
                                   return(NULL)
                                infoBox( "World market size",
                                         paste0("$", 
                                                format(round(rv_pre_define_ex$tmp_global_size_value_now/10^6), big.mark = ","),
                                                " m"
                                         )
                                         , icon = icon('globe', lib = "glyphicon")
                                         
                                )
                             })
                          
                          ## 5 year growth
                          output$Un_comtrade_world_market_change_pre_define <-
                             renderInfoBox({
                                if( is.null(rv_pre_define_ex$tmp_global_by_country_raw)  )
                                   return(NULL)
                                
                                if( is.null(rv_pre_define_ex$tmp_global_size_value_change) )
                                   infoBox( "CAGR (5 years)",
                                            HTML(paste0( "Not available" )), 
                                            icon = icon('minus'))
                                
                                if(rv_pre_define_ex$tmp_global_size_value_change>0 ){
                                   infoBox( "CAGR (5 years)",
                                            HTML(paste0( "<font color='green'> +",
                                                         round(abs(rv_pre_define_ex$tmp_global_size_value_change)*100,1),
                                                         "% </font>"
                                            )), 
                                            icon = icon('arrow-up'), color = 'green')
                                }else{
                                   infoBox( "CAGR (5 years)",
                                            HTML(paste0( "<font color='red'> -",
                                                         round(abs(rv_pre_define_ex$tmp_global_size_value_change)*100,1),
                                                         "% </font>"
                                            )), 
                                            icon = icon('arrow-down'), color = 'red')
                                }
                                
                             })
                          
                          ## 5 yr abs change
                          output$Un_comtrade_world_market_change_abs_pre_define <-
                             renderInfoBox({
                                if(  is.null(rv_pre_define_ex$tmp_global_by_country_raw) )
                                   return(NULL)
                                
                                if( is.null(rv_pre_define_ex$tmp_global_size_value_change_abs) )
                                   infoBox( "ABS (5 years)",
                                            HTML(paste0( "Not available" )), 
                                            icon = icon('minus'))
                                
                                if(rv_pre_define_ex$tmp_global_size_value_change_abs>0 ){
                                   infoBox( "ABS (5 years)",
                                            HTML(paste0("<font color='green'> +$", 
                                                        format(round(rv_pre_define_ex$tmp_global_size_value_change_abs/10^6), big.mark = ","),
                                                        " m </font>"
                                            )),
                                            icon = icon('arrow-up'), color = 'green')
                                }else{
                                   infoBox( "ABS (5 years)",
                                            HTML(paste0("<font color='red'> -$", 
                                                        format(round(abs(rv_pre_define_ex$tmp_global_size_value_change_abs)/10^6), big.mark = ","),
                                                        " m </font>"
                                            )),
                                            icon = icon('arrow-down'), color = 'red')
                                }
                             })
                          
                          ## top 3 importer share
                          output$Un_comtrade_top3_importers_share_pre_define <-
                             renderInfoBox({
                                if( is.null(rv_pre_define_ex$tmp_global_by_country_raw)  )
                                   return(NULL)
                                infoBox( HTML("Top 3 importers <br> share"),
                                         paste0( 
                                            round(abs(rv_pre_define_ex$tmp_top3_importers_share)*100,1),
                                            "%"
                                         ),
                                         icon = icon('import', lib = "glyphicon"))
                             })
                          
                          ## top 10 importer share
                          output$Un_comtrade_top10_importers_share_pre_define <-
                             renderInfoBox({
                                if(  is.null(rv_pre_define_ex$tmp_global_by_country_raw)  )
                                   return(NULL)
                                infoBox( HTML("Top 10 importers <br> share"),
                                         paste0( 
                                            round(abs(rv_pre_define_ex$tmp_top10_importers_share)*100,1),
                                            "%"
                                         ),
                                         icon = icon('import', lib = "glyphicon"))
                             })
                          
                          ##  of top 20 markets -- number of high growth market
                          output$Un_comtrade_high_growth_importers_pre_define <-
                             renderInfoBox({
                                if(  is.null(rv_pre_define_ex$tmp_global_by_country_raw)  )
                                   return(NULL)
                                infoBox( HTML("Top 20 importers <br> with CAGR>10%"),
                                         paste0( rv_pre_define_ex$tmp_number_high_growth_importers) ,
                                         icon = icon('import', lib = "glyphicon"))
                             })
                          
                          
                          ## top 3 exporter share
                          output$Un_comtrade_top3_exporters_share_pre_define <-
                             renderInfoBox({
                                if(  is.null(rv_pre_define_ex$tmp_global_by_country_raw) )
                                   return(NULL)
                                infoBox( HTML("Top 3 exporters <br> share"),
                                         paste0( 
                                            round(abs(rv_pre_define_ex$tmp_top3_exporters_share)*100,1),
                                            "%"
                                         ),
                                         icon = icon('export', lib = "glyphicon"))
                             })
                          
                          ## top 10 exporter share
                          output$Un_comtrade_top10_exporters_share_pre_define <-
                             renderInfoBox({
                                if(  is.null(rv_pre_define_ex$tmp_global_by_country_raw)  )
                                   return(NULL)
                                infoBox( HTML("Top 10 exporters <br> share"),
                                         paste0( 
                                            round(abs(rv_pre_define_ex$tmp_top10_exporters_share)*100,1),
                                            "%"
                                         ),
                                         icon = icon('export', lib = "glyphicon"))
                             })
                          
                          ## new zealand share
                          output$Un_comtrade_nz_share_pre_define <-
                             renderInfoBox({
                                if(  is.null(rv_pre_define_ex$tmp_global_by_country_raw)  )
                                   return(NULL)
                                if( rv_pre_define_ex$tmp_nz_share < 0.001 ){
                                   infoBox( HTML("New Zealand <br> share"),
                                            paste0( "Less than 0.1%" ),
                                            icon = icon('export', lib = "glyphicon"))
                                }else{
                                   infoBox( HTML("New Zealand <br> share"),
                                            paste0( 
                                               round(abs(rv_pre_define_ex$tmp_nz_share)*100,1),
                                               "%"
                                            ),
                                            icon = icon('export', lib = "glyphicon"))
                                }
                                
                             })
                          
                          
                          ##!!!!! try UI insert: value box for global market facts ----------- 
                          output$H1_title_global_facts_pre_define <-
                             renderText({
                                if( is.null(rv_pre_define_ex$tmp_global_by_country_raw)   )
                                   return(NULL)
                                paste0( "Global market analysis (", tmp_un_comtrade_max_year ,")" )
                             })
                          
                          output$H1_title_global_facts_note_pre_define <-
                             renderText({
                                if(  is.null(rv_pre_define_ex$tmp_global_by_country_raw)  )
                                   return(NULL)
                                paste0( "All values undner the global market analysis are reported in current US dollar" )
                             })
                          
                          output$H3_title_global_facts_summary_pre_define <-
                             renderText({
                                if(  is.null(rv_pre_define_ex$tmp_global_by_country_raw)   )
                                   return(NULL)
                                paste0( "Key facts and summary" )
                             })
                          
                          ### insert global market key facts and summary value boxe
                          insertUI(
                             selector = '#body_ci_markets_ex',
                             ui =   div( id = 'body_ci_markets_ex_global_facts',
                                         fluidRow( 
                                            h1( HTML(paste0(textOutput("H1_title_global_facts_pre_define"))) ),
                                            p( HTML(paste0(textOutput("H1_title_global_facts_note_pre_define"))) ),
                                            h3( HTML(paste0(textOutput("H3_title_global_facts_summary_pre_define"))) ),
                                            infoBoxOutput("Un_comtrade_world_market_size_pre_define") ,
                                            infoBoxOutput("Un_comtrade_world_market_change_pre_define" ) ,
                                            infoBoxOutput("Un_comtrade_world_market_change_abs_pre_define" ) 
                                         ),
                                         fluidRow(
                                            infoBoxOutput("Un_comtrade_top3_importers_share_pre_define" ) ,
                                            infoBoxOutput("Un_comtrade_top10_importers_share_pre_define" ) ,
                                            infoBoxOutput("Un_comtrade_high_growth_importers_pre_define" ) 
                                         ),
                                         fluidRow(
                                            infoBoxOutput("Un_comtrade_top3_exporters_share_pre_define" ) ,
                                            infoBoxOutput("Un_comtrade_top10_exporters_share_pre_define" ) ,
                                            infoBoxOutput("Un_comtrade_nz_share_pre_define" ) 
                                         )
                             )
                          )
                          
                          
                          ## 2.8 Quick glance at both importers and exporters map --------
                          print("--------- Building importer and exporter map -------------")
                          output$UN_comtrade_importer_Map_pre_define <- 
                             renderHighchart({
                                if(  is.null(rv_pre_define_ex$tmp_global_by_country_raw)  )
                                   return(NULL)
                                hcmap( data = rv_pre_define_ex$tmp_un_comtrade_importer_map ,
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
                                   hc_add_series(data =  rv_pre_define_ex$tmp_un_comtrade_importer_map ,
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
                          output$UN_comtrade_exporter_Map_pre_define <- 
                             renderHighchart({
                                if(  is.null(rv_pre_define_ex$tmp_global_by_country_raw)  )
                                   return(NULL)
                                hcmap( data = rv_pre_define_ex$tmp_un_comtrade_exporter_map ,
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
                                   hc_add_series(data =  rv_pre_define_ex$tmp_un_comtrade_exporter_map ,
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
                          output$H3_title_un_comtrade_map_pre_define <-
                             renderText({
                                if(  is.null(rv_pre_define_ex$tmp_global_by_country_raw) )
                                   return(NULL)
                                paste0("Global importers and exporters at a glance")
                             })
                          
                          output$H3_title_un_comtrade_map_note_pre_define <-
                             renderText({
                                if(  is.null(rv_pre_define_ex$tmp_global_by_country_raw)  )
                                   return(NULL)
                                paste0( "The size of bubble area and color both represent the value of imports or exports" ) 
                             })
                          
                          output$H4_title_un_comtrade_importer_map_pre_define <-
                             renderText({
                                if(  is.null(rv_pre_define_ex$tmp_global_by_country_raw)  )
                                   return(NULL)
                                paste0("Global IMPORT markets")
                             })
                          
                          output$H4_title_un_comtrade_exporter_map_pre_define <-
                             renderText({
                                if(  is.null(rv_pre_define_ex$tmp_global_by_country_raw)  )
                                   return(NULL)
                                paste0("Global EXPORT markets")
                             })
                          
                          ## Insert ui here
                          insertUI(
                             selector = '#body_ci_markets_ex',
                             ui =   div( id = 'body_ci_markets_ex_un_comtrade_map',
                                         fluidRow(h3( HTML(paste0(textOutput("H3_title_un_comtrade_map_pre_define"))) ) ,
                                                  p( HTML(paste0(textOutput("H3_title_un_comtrade_map_note_pre_define"))) ),
                                                  column(6, div(id = "body_ci_markets_ex_un_comtrade_map_import", h4( HTML(paste0(textOutput("H4_title_un_comtrade_importer_map_pre_define"))) ), highchartOutput('UN_comtrade_importer_Map_pre_define') ) ),
                                                  column(6, div(id = "body_ci_markets_ex_un_comtrade_map_export", h4( HTML(paste0(textOutput("H4_title_un_comtrade_exporter_map_pre_define"))) ), highchartOutput('UN_comtrade_exporter_Map_pre_define') ) )
                                         )
                             )
                          )
                          ## end Try UI insert --------##
                          
                          
                          ## 2.8.1 Sankey plot for a commodity ---------------
                          print("--------- Building Sankey data -------------")
                          
                          observe({
                             ## check if able to get sankey data
                             rv_pre_define_ex$Fail_sankey_data <-
                                try(
                                   rv_pre_define_ex$sankey_plot_data <-
                                      get_data_sankey_uncomtrade( cc = tmp_hs_ex(), max_year = tmp_un_comtrade_max_year, eu_internal = "No" )
                                )

                             if( class(rv_pre_define_ex$Fail_sankey_data) == 'try-error' )
                                print("--------- FAIL: building Sankey data !!! -------------")
                          })
                          
                          print("--------- Building Sankey plots -------------")
                          output$Sankey_trade <-
                             renderSankeyNetwork({
                                if(  is.null(rv_pre_define_ex$tmp_global_by_country_raw) | 
                                     length(tmp_hs_ex())>1 |  
                                     class(rv_pre_define_ex$Fail_sankey_data) == 'try-error' ){
                                   return(NULL)
                                }else{
                                   print("--------- Plotting Sankey plots -------------")
                                   sankey_uncomtrade( cc = tmp_hs_ex(), max_year = tmp_un_comtrade_max_year,eu_internal = as.character(input$btn_eu_internal)  )
                                }
                             })
                          
                          ## !!!!! try UI insert: Sankey plot ----------- 
                          output$H3_title_sankey <-
                             renderText({
                                # if( is.null(rv_pre_define_ex$tmp_global_by_country_raw) | 
                                #     length(tmp_hs_ex())>1 |  
                                #     class(rv_pre_define_ex$Fail_sankey_data) == 'try-error' )
                                #    #return(NULL)
                                # {paste0("Unable to perform global trade flow analyasis due to data query limits. Please wait for a hour.")}else{
                                #    paste0( "Global trade flow analysis" )
                                # }
                                
                                if( class(rv_pre_define_ex$Fail_sankey_data) == 'try-error' & 
                                    input$select_comodity_ex_for_market_analysis != "" &
                                    length(tmp_hs_ex()) == 1 ){
                                   paste0("Unable to perform global trade flow analyasis due to data query limits. Please wait for a hour.")
                                }
                                
                                if( class(rv_pre_define_ex$Fail_sankey_data) == 'try-error' & 
                                    input$select_comodity_ex_for_market_analysis == ""  ){
                                   return(NULL)
                                }
                                
                                if( class(rv_pre_define_ex$Fail_sankey_data) != 'try-error' &
                                    input$select_comodity_ex_for_market_analysis != "" &
                                    length(tmp_hs_ex()) ==  1){
                                   paste0( "Global trade flow analysis" )
                                }
                             })

                          output$H3_title_sankey_note <-
                             renderUI({
                                if( is.null(rv_pre_define_ex$tmp_global_by_country_raw) | 
                                    length(tmp_hs_ex())>1 |  
                                    class(rv_pre_define_ex$Fail_sankey_data) == 'try-error' )
                                   return(NULL)
                                tags$p("This sankey plot shows trade flows of the selected commodity from expoters to importers. The displayed markets coverage is equal to or greater than 90% of global exports. The displayed trade flows are equal to or greater than 0.5% of global exports. Different colors are used to distinguish",
                                   tags$span( "EXPORTERS", style = "color: #97D700; font-weight: bold" ),
                                   ", ",
                                   tags$span( "IMPORTERS", style = "color: #CD5B45; font-weight: bold"),
                                  ", and ",
                                   tags$span( "BOTH", style = "color: #FBE122; font-weight: bold"), "." )

                             })

                          ## button to choose show/hide EU internal trade
                          output$Btn_EU_Internal <-
                             renderUI({
                                if( is.null(rv_pre_define_ex$tmp_global_by_country_raw) |
                                    length(tmp_hs_ex())>1 |
                                    class(rv_pre_define_ex$Fail_sankey_data) == 'try-error' )
                                   return(NULL)
                                radioButtons("btn_eu_internal",
                                             p("Display EU internal trade: " ),
                                             choiceNames = list(icon("check"), icon("times")),
                                             choiceValues = list( "Yes" , "No"),
                                             #c( "Yes" = "Yes", "No" = "No"),
                                             inline=T,
                                             selected="No")
                             })

                          output$Btn_EU_Internal_note <-
                             renderUI({
                                if( is.null(rv_pre_define_ex$tmp_global_by_country_raw) | 
                                    length(tmp_hs_ex())>1 |  
                                    class(rv_pre_define_ex$Fail_sankey_data) == 'try-error')
                                   return(NULL)
                                tags$p( "You may choose to show or hide EU internal trade in the sankey plot by using the buttons below." )
                             })

                          ## Insert ui here
                          insertUI(
                             selector = '#body_ci_markets_ex',
                             ui =   div( id = 'body_ci_markets_ex_un_comtrade_sankey',
                                         fluidRow(h3( HTML(paste0(textOutput("H3_title_sankey"))) ) ,
                                                  #p( HTML(paste0(textOutput("H2_title_sankey_note"))) ),
                                                  uiOutput("H3_title_sankey_note"),
                                                  uiOutput("Btn_EU_Internal_note"),
                                                  uiOutput("Btn_EU_Internal"),
                                                  sankeyNetworkOutput( "Sankey_trade" )
                                         )
                             )
                          )
                          ## end Try UI insert --------##
                          
                          ## 2.9 Generating summary tables for both importers and exporters -------
                          # container of the table -- importers 
                          print("--------- Building importer and exporter tabels -------------")
                          
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
                          output$UN_com_trade_importer_summary_pre_define <-
                             renderDataTable({
                                if( is.null(rv_pre_define_ex$tmp_global_by_country_raw)   )
                                   return(NULL)
                                datatable( rv_pre_define_ex$tmp_un_comtrade_import_summary_tab,
                                           container = sketch_uncomtrade_im,
                                           rownames = FALSE,
                                           extensions = 'Buttons',
                                           options = list(dom = 'Bltp',
                                                          buttons = c('copy', 'csv', 'excel', 'pdf', 'print'),
                                                          scrollX = TRUE,
                                                          pageLength = 10,
                                                          lengthMenu = list(c(10, 30 , -1), list('10','30' ,'All')),
                                                          columnDefs = list(list(className = 'dt-center', 
                                                                                 targets = 0:(ncol(rv_pre_define_ex$tmp_un_comtrade_import_summary_tab)-1) ) )
                                           )
                                ) %>%
                                   formatPercentage( c('Share', 'Value_per_change', 'Price_per_change' ) , digit = 1 ) %>%
                                   formatCurrency( columns = c('Trade.Value..US..','Value_abs_change'), digits = 0 ) %>%
                                   formatCurrency( columns = c('Price'), digits = 2 ) %>%
                                   formatStyle(
                                      c('Value_per_change' ),
                                      background = styleColorBar( c(0,max(rv_pre_define_ex$tmp_un_comtrade_import_summary_tab[1:min(20,nrow(rv_pre_define_ex$tmp_un_comtrade_import_summary_tab)),c('Value_per_change' )],na.rm=T)*2) ,
                                                                  'lightblue'),
                                      backgroundSize = '100% 90%',
                                      backgroundRepeat = 'no-repeat',
                                      backgroundPosition = 'center',
                                      color = JS("value < 0 ? 'darkred' : value > 0 ? 'darkgreen' : 'black'")
                                   ) %>%
                                   formatStyle(
                                      c('Price_per_change' ),
                                      background = styleColorBar( c(0,max(rv_pre_define_ex$tmp_un_comtrade_import_summary_tab[1:min(20,nrow(rv_pre_define_ex$tmp_un_comtrade_import_summary_tab)),c('Price_per_change' )],na.rm=T)*2) ,
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
                                   formatStyle( 1:ncol(rv_pre_define_ex$tmp_un_comtrade_import_summary_tab), 'vertical-align'='center', 'text-align' = 'center' )
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
                          output$UN_com_trade_exporter_summary_pre_define <-
                             renderDataTable({
                                if( is.null(rv_pre_define_ex$tmp_global_by_country_raw) )
                                   return(NULL)
                                datatable( rv_pre_define_ex$tmp_un_comtrade_export_summary_tab,
                                           container = sketch_uncomtrade_ex,
                                           rownames = FALSE,
                                           extensions = 'Buttons',
                                           options = list(dom = 'Bltp', 
                                                          buttons = c('copy', 'csv', 'excel', 'pdf', 'print'),
                                                          scrollX = TRUE,
                                                          pageLength = 10,
                                                          lengthMenu = list(c(10, 30, -1), list('10', '30' ,'All')),
                                                          columnDefs = list(list(className = 'dt-center', targets = 0:(ncol(rv_pre_define_ex$tmp_un_comtrade_export_summary_tab)-1) ) )
                                           )
                                ) %>%
                                   formatPercentage( c('Share', 'Value_per_change', 'Price_per_change' ) , digit = 1 ) %>%
                                   formatCurrency( columns = c('Trade.Value..US..','Value_abs_change'), digits = 0 ) %>%
                                   formatCurrency( columns = c('Price'), digits = 2 ) %>%
                                   formatStyle(
                                      c('Value_per_change' ),
                                      background = styleColorBar( c(0,max(rv_pre_define_ex$tmp_un_comtrade_export_summary_tab[1:min(20,nrow(rv_pre_define_ex$tmp_un_comtrade_export_summary_tab)),c('Value_per_change' )],na.rm=T)*2) ,
                                                                  'lightblue'),
                                      backgroundSize = '100% 90%',
                                      backgroundRepeat = 'no-repeat',
                                      backgroundPosition = 'center',
                                      color = JS("value < 0 ? 'darkred' : value > 0 ? 'darkgreen' : 'black'")
                                   ) %>%
                                   formatStyle(
                                      c('Price_per_change' ),
                                      background = styleColorBar( c(0,max(rv_pre_define_ex$tmp_un_comtrade_export_summary_tab[1:min(20,nrow(rv_pre_define_ex$tmp_un_comtrade_export_summary_tab)),c('Price_per_change' )],na.rm=T)*2) ,
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
                                   formatStyle( 1:ncol(rv_pre_define_ex$tmp_un_comtrade_export_summary_tab), 'vertical-align'='center', 'text-align' = 'center' )
                             })
                          
                          ## Insert ui here: summary tables  ----------------
                          output$H3_title_un_comtrade_summary_tab_pre_define <-
                             renderText({
                                if(  is.null(rv_pre_define_ex$tmp_global_by_country_raw) )
                                   return(NULL)
                                paste0("Summary tables for importers and exporters")
                             })
                          
                          output$H3_title_un_comtrade_summary_tab_pre_define_note <-
                             renderText({
                                if(  is.null(rv_pre_define_ex$tmp_global_by_country_raw) )
                                   return(NULL)
                                paste0("EU-28-Intra means all internal trade among the EU28 countries. EU-28-Extra means trade between EU28 as a whole to the rest of the world. Compound annual growth rate (CAGR) for the past 1, 5, and 10 years. Absolute value change (ABS) for the past 5 and 10 years. Import or export prices will be displayed when quantity of the selected commodity is available.")
                             })
                          
                          
                          output$H4_title_un_comtrade_importer_sum_tab_pre_define <-
                             renderText({
                                if(  is.null(rv_pre_define_ex$tmp_global_by_country_raw)  )
                                   return(NULL)
                                paste0("Global IMPORT markets")
                             })
                          
                          output$H4_title_un_comtrade_exporter_sum_tab_pre_define <-
                             renderText({
                                if(  is.null(rv_pre_define_ex$tmp_global_by_country_raw)  )
                                   return(NULL)
                                paste0("Global EXPORT markets")
                             })
                          
                          insertUI(
                             selector = '#body_ci_markets_ex',
                             ui =   div( id = 'body_ci_markets_ex_un_comtrade_summary_tab',
                                         fluidRow(h3( HTML(paste0(textOutput("H3_title_un_comtrade_summary_tab_pre_define"))) ) ,
                                                  p( HTML(paste0(textOutput("H3_title_un_comtrade_summary_tab_pre_define_note"))) ),
                                                  column(6, div(id = "body_ci_markets_ex_un_comtrade_import_summary_tab", h4( HTML(paste0(textOutput("H4_title_un_comtrade_importer_sum_tab_pre_define"))) ), dataTableOutput('UN_com_trade_importer_summary_pre_define') ) ),
                                                  column(6, div(id = "body_ci_markets_ex_un_comtrade_export_summary_tab", h4( HTML(paste0(textOutput("H4_title_un_comtrade_exporter_sum_tab_pre_define"))) ), dataTableOutput('UN_com_trade_exporter_summary_pre_define') ) )
                                         )
                             )
                          )
                          ## end Try UI insert --------##
                          

                          
                          ## 3.0 Get the leftover quota and reset time ---------
                          output$Un_comtrade_msg <-
                             renderUI({
                                #if(  is.null(rv_pre_define_ex$tmp_global_by_country_raw) )
                                 #  return(NULL)
                                # tags$p(paste0( "Note: ",ct_get_remaining_hourly_queries(), 
                                #                " number of queries are left for the global analysis section from the UN Comtrade. The reset time will be at ", 
                                #                ct_get_reset_time() ,
                                #                ", while the current time is ", format(Sys.time()) , "."
                                #                )
                                #          )
                                
                                if(  input$select_comodity_ex_for_market_analysis == "" ){
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
                          
                          insertUI( selector = '#body_ci_markets_ex',
                                    ui = div( id = 'body_ci_markets_ex_un_comtrade_msg',
                                              fluidRow( #tags$hr(),
                                                        uiOutput("Un_comtrade_msg") ) 
                                              )
                                    )
                          
                          ## 4.15 Hide generating report message ----------
                          observe({
                             if( any(input$select_comodity_ex_for_market_analysis %in% tmp_tab$Name)   ){
                                shinyjs::hide( id = "body_ci_market_loading_message" )
                             }
                          })
                          
                          ## hide waite message ----
                          shinyjs::hide( id = 'wait_message_ci_ex' )
                       }
                       
                       ## 1.3 work on Self defined warning if no .csv HS code and grouping uploaded ------------------
                       if(input$rbtn_prebuilt_diy_ex=='Self-defined' & 
                          is.null(input$file_comodity_ex) ) {
                          showModal(modalDialog(
                             title = "Warning",
                             tags$b("Please upload an appropriate CSV file with HS codes and groupsings!"),
                             size = 's'
                          ))
                       }
                       
                       ## Now if a csv file is uploaded -- check HS groupings
                       if(input$rbtn_prebuilt_diy_ex=='Self-defined' & 
                          !is.null(input$file_comodity_ex) ){
                          ## warning if not a CSV file
                          if( !grepl(".csv",input$file_comodity_ex$datapath) ){
                             showModal(modalDialog(
                                title = "Warning",
                                tags$b("Only CSV files are accepted!"),
                                size = 's'
                             ))
                          }else{
                             ## read the grouping
                             hs_group <-  read.csv(input$file_comodity_ex$datapath, row.names = NULL) 
                                                   
                             ## check if the first column is HS code
                             tmp_hs_c1 <- gsub("[`]", "", hs_group[,1])
                             if( ncol(hs_group) >2 ){
                                showModal(modalDialog(
                                   title = "Warning",
                                   tags$p("Please check your uploaded HS groupings and make sure", 
                                          tags$b("it contains TWO columns only!")),
                                   size = 's'
                                ))
                             }else if( any( is.na( as.numeric(tmp_hs_c1) )  ) ){
                                showModal(modalDialog(
                                   title = "Warning",
                                   tags$p("Please check your uploaded HS groupings and make sure", 
                                           tags$b("the first column is HS codes!")),
                                   size = 's'
                                ))
                             }else if( any( nchar(tmp_hs_c1) > 6 ) ){
                                showModal(modalDialog(
                                   title = "Warning",
                                   tags$p("Please check your uploaded HS groupings and make sure", 
                                          tags$b("all HS codes are within level 6!") ),
                                   size = 's'
                                ))
                             }else{
                                ### can rn self define
                                tmp_execution_self_define <- TRUE
                             }
                             
                             ## 1.3.1 Build graphs self-defined commodity --------------
                             if(tmp_execution_self_define){
                                ## --- hide howto -----
                                shinyjs::hide(id = 'ci_howto_ex')
                                ## show waite message ----
                                shinyjs::show( id = 'wait_message_ci_ex' )
                                ## disable the buttone ---
                                shinyjs::disable("btn_build_commodity_report_ex")
                                ## disable the upload button ---
                                shinyjs::disable("file_comodity_ex")
                                shinyjs::disable("rbtn_prebuilt_diy_ex")
                                
                                ## now To build report if checks are all good ----------------
                                ## make sure the HS codes become characters and HS 1 has 01 format
                                ## standerdise column names
                                colnames(hs_group) <- c("HS_code", "HS_group")
                                ## make columns characters and make sure HS code has 01, and 0122 etc format
                                hs_group %<>%
                                   mutate_all( funs(as.character) ) %>%
                                   mutate( HS_code = gsub("[`]","",HS_code) ) %>%
                                   mutate( HS_code = if_else(nchar(HS_code)%in%c(1,3,5), paste0("0", HS_code), HS_code  )  )
                                
                                
                                ## 2.0.1 Self-define Build the main data.frame -- all selected commodity by country ------
                                tmp_dtf_shiny_full <-
                                   dtf_shiny_full %>%
                                   filter( Type_ie == 'Exports', 
                                           Commodity %in% hs_group$HS_code ) %>%
                                   left_join( hs_group, by = c('Commodity' = 'HS_code') ) %>%
                                   left_join( concord_country_iso_latlon_raw, by = 'Country' ) %>%
                                   group_by( Year, Country, Type_ie, Type_gs, HS_group, ISO2, lat, lon, Note ) %>%
                                   summarise( Value = sum(Value, na.rm=T) ) %>%
                                   ungroup
                                
                                #output$test_full_shiny <- renderDataTable(tmp_dtf_shiny_full)
                                
                                ## commodity only -- sum all countires
                                tmp_dtf_shiny_full_commodity_only <-
                                   tmp_dtf_shiny_full %>%
                                   group_by( Year,  Type_ie, Type_gs, HS_group, Note ) %>%
                                   summarise( Value = sum(Value, na.rm=T) ) %>%
                                   ungroup %>%
                                   mutate( Country = 'World' )
                                
                                ## 2.1 Self-defined: Build export value line chart -------------------- 
                                tmp_top_g_ex <-
                                   tmp_dtf_shiny_full_commodity_only %>%
                                   filter( Year == max(Year)) %>% 
                                   arrange( -Value ) %>%
                                   dplyr::select( HS_group ) %>%
                                   as.matrix() %>%
                                   as.character
                                
                                
                                ## top selected commodities and top 5services
                                tmp_top_ex <- c( tmp_top_g_ex) #, tmp_top_s_ex)
                                
                                ## data frame to plot
                                tmp_dtf_key_line_ex <- 
                                   tmp_dtf_shiny_full_commodity_only%>%
                                   filter( HS_group %in% tmp_top_ex,
                                           Year >=2007) %>%
                                   mutate( Value = round(Value/10^6),
                                           HS_group = factor(HS_group, levels = tmp_top_ex)
                                   ) %>%
                                   arrange( HS_group )
                                
                                ### plot
                                output$CIExportValueLine <- 
                                   renderHighchart(
                                      highchart() %>%
                                         hc_exporting(enabled = TRUE, formAttributes = list(target = "_blank")) %>%
                                         hc_xAxis( categories = c( unique( tmp_dtf_key_line_ex$Year) ) ) %>%
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
                                         hc_add_series( data =  tmp_dtf_key_line_ex %>% filter( Type_gs == 'Goods' ) ,
                                                        mapping = hcaes(  x = Year, y = Value, group = HS_group ),
                                                        type = 'line',
                                                        marker = list(symbol = 'circle') #,
                                                        #visible = c(T,rep(F,length(tmp_top_g_ex)-1))
                                         )
                                   )
                                
                                ## 2.2 Self-defined: build export as a percent of total export line chart -----------------------
                                tmp_tot_ex <-
                                   dtf_shiny_full %>%
                                   filter( Country == 'World',
                                           Type_ie == 'Exports',
                                           Year >= 2007 )  %>%
                                   mutate( Value = round(Value/10^6) ) %>%
                                   group_by( Year, Country, Type_ie ) %>%
                                   summarize( Value = sum(Value, na.rm=T) ) %>%
                                   ungroup %>%
                                   mutate( HS_group = 'Total exports' )
                                
                                tmp_dtf_percent_line_ex <-
                                   tmp_dtf_key_line_ex %>%
                                   bind_rows( tmp_tot_ex ) %>%
                                   group_by( Year, Country, Type_ie ) %>%
                                   mutate( Share = Value/Value[HS_group=='Total exports'],
                                           Value = Share*100 ) %>%
                                   ungroup %>%
                                   filter( HS_group != 'Total exports' ) %>%
                                   mutate( HS_group = factor(HS_group, levels = tmp_top_ex) ) %>%
                                   arrange( HS_group )
                                
                                # ### plot
                                output$CIExportPercentLine <-
                                   renderHighchart(
                                      highchart() %>%
                                         hc_exporting(enabled = TRUE, formAttributes = list(target = "_blank")) %>%
                                         hc_xAxis( categories = c( unique( tmp_dtf_percent_line_ex$Year) ) ) %>%
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
                                         hc_add_series( data =  tmp_dtf_percent_line_ex %>% filter( Type_gs == 'Goods' ) ,
                                                        mapping = hcaes(  x = Year, y = Value, group = HS_group ),
                                                        type = 'line',
                                                        marker = list(symbol = 'circle') #,
                                                        #visible = c(T,rep(F,length(tmp_top_g_ex)-1))
                                         )
                                   )
                                
                                ## !!!!! try UI insert ONLY when not may commodities selected ----------- 
                                if( length(unique(hs_group$HS_group)) < 111 ){
                                   insertUI(
                                      selector = '#body_ex_self_defined',
                                      ui =   div( id = 'body_ex_line_value_percent_self_defined',
                                                  fluidRow( h1("Exports for selected commodities/services"),
                                                            p("Click on the commodity or service names in the legend area to show their trends"),
                                                            column(6, div(id = "body_value_ex", h4("Export values"), highchartOutput('CIExportValueLine') ) ),
                                                            column(6, div(id = "body_percent_ex", h4("As a percent of total exports"), highchartOutput('CIExportPercentLine') ) ))
                                      )
                                   )
                                }
                                ## end Try UI insert --------##
                                
                                ## 2.3 Self-defined: build export value change table ----------------
                                ## data frame to plot
                                tmp_dtf_key_tab_ex <- 
                                   tmp_dtf_shiny_full_commodity_only %>%
                                   filter( HS_group %in% tmp_top_ex) %>%
                                   mutate( HS_group = factor(HS_group, levels = tmp_top_ex) ) %>%
                                   arrange( HS_group )
                                
                                tmp_tab <-
                                   tmp_dtf_key_tab_ex %>%
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
                                   left_join( tmp_dtf_key_tab_ex , 
                                              by =c('Name'='HS_group') ) %>%
                                   left_join( tmp_dtf_percent_line_ex %>% dplyr::select( -Value) %>% rename(Name = HS_group) ) %>%
                                   filter( Year == max(Year) ) %>%
                                   mutate( Value = Value/10^6, ABS5 = ABS5/10^6, ABS10 = ABS10/10^6 ) %>%
                                   dplyr::select( Name, Value, Share, CAGR1, CAGR5, CAGR10, ABS5, ABS10) %>%
                                   mutate( Name = factor(Name, levels = tmp_top_ex),
                                           CAGR1 = ifelse(CAGR1 %in% c(Inf,-Inf), NA, CAGR1),
                                           CAGR5 = ifelse(CAGR5 %in% c(Inf,-Inf), NA, CAGR5),
                                           CAGR10 = ifelse(CAGR10 %in% c(Inf,-Inf), NA, CAGR10)
                                   ) %>%
                                   arrange( Name )
                                
                                
                                ### join back to hs code
                                hs_group_flat <- 
                                   hs_group %>%
                                   group_by( HS_group ) %>%
                                   summarise( HS_code = paste0(HS_code, collapse = '; ') ) %>%
                                   ungroup
                                
                                tmp_tab %<>%
                                   left_join( hs_group_flat, by = c("Name"= 'HS_group') ) %>%
                                   dplyr::select( HS_code, Name, Value, Share, CAGR1, CAGR5, CAGR10, ABS5, ABS10 )
                                
                                output$GrowthTabSelectedEx <- renderDataTable(
                                   datatable( tmp_tab,
                                              rownames = F,
                                              filter = c("top"),
                                              extensions = c('Buttons'
                                                             #, 'FixedColumns'
                                              ),
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
                                              colnames = c("HS codes", "Classification","Value ($m)", "Share of total exports", 'CAGR1', 'CAGR5', 'CAGR10', 'ABS5', 'ABS10')
                                   ) %>%
                                      formatStyle(
                                         c('CAGR1', 'CAGR5', 'CAGR10'),
                                         background = styleColorBar( c(0, max(c(tmp_tab$CAGR1,tmp_tab$CAGR5, tmp_tab$CAGR10))*2, na.rm=T) , 'lightblue'),
                                         backgroundSize = '100% 90%',
                                         backgroundRepeat = 'no-repeat',
                                         backgroundPosition = 'center'
                                      ) %>%
                                      formatStyle(c('CAGR1', 'CAGR5', 'CAGR10', 'ABS5', 'ABS10'),
                                                  color = JS("value < 0 ? 'darkred' : value > 0 ? 'darkgreen' : 'black'")) %>%
                                      formatPercentage( c('Share','CAGR1', 'CAGR5', 'CAGR10'),digit = 1 ) %>%
                                      formatStyle( columns = c('Name','Value', 'Share', 'CAGR1', 'CAGR5', 'CAGR10', 'ABS5', 'ABS10'), `font-size`= '115%' ) %>%
                                      formatCurrency( columns = c('Value', 'ABS5', 'ABS10'), mark = ' ', digits = 1)
                                )
                                
                                ## !!!!! try UI insert ----------- 
                                insertUI(
                                   selector = '#body_growth_ex_self_defined',
                                   ui =   div( id = 'body_ex_growth_tab_self_defined',
                                               fluidRow( h1("Short, medium, and long term growth for selected commodities/services"),
                                                         p("Compound annual growth rate (CAGR) for the past 1, 5, and 10 years. Absolute value change (ABS) for the past 5 and 10 years."),
                                                         dataTableOutput('GrowthTabSelectedEx')
                                               )
                                   )
                                )
                                ## end Try UI insert --------##
                                
                                ## 2.4 Self-defined: Build export by country output groups -------------------
                                ## create a selector for each selected commodity -----------------
                                output$CIEXSelectorByMarkets <- renderUI({
                                   selectizeInput("select_comodity_ex_for_market_analysis",
                                                   tags$p("Please select or search a commodity for its market analysis"), 
                                                  # choices = tmp_tab$Name[input$GrowthTabSelectedEx_rows_all], # tmp_top_ex, 
                                                  # selected = NULL, #tmp_top_ex[1], 
                                                  # width = "500px",
                                                  # multiple = F,
                                                  # options = list(
                                                  #    placeholder = 'Please select a commodity',
                                                  #    onInitialize = I('function() { this.setValue(""); }')
                                                  # ) 
                                                  choices =  c('Please select a commodity' = "" , 
                                                               tmp_tab$Name[input$GrowthTabSelectedEx_rows_all]), #input$select_comodity_ex,
                                                  selected = NULL,  width = "500px",
                                                  multiple = F #,
                                                  # options = list(
                                                  #    placeholder = 'Please select a commodity',
                                                  #    onInitialize = I('function() { this.setValue(" "); }')
                                                  #             ) 
                                                  )
                                })
                                
                                ### build data for market analysis -- these has to be reactive values
                                ## The name of the selected commodity
                                tmp_selected_ex <- 
                                   reactive({
                                      input$select_comodity_ex_for_market_analysis
                                   })
                                
                                ## The HS codes of the selected commodity
                                tmp_hs_ex <- 
                                   reactive({
                                      hs_group$HS_code[hs_group$HS_group == tmp_selected_ex()]
                                   })
                                
                                ## The data from of the selected commodity by markets
                                tmp_dtf_market_ex <- 
                                   reactive({
                                      dtf_shiny_full %>%
                                         filter( Commodity %in% tmp_hs_ex(), 
                                                 Year >= 2007,
                                                 Type_ie == 'Exports') %>%
                                         left_join( concord_country_iso_latlon_raw, by = 'Country' ) %>%
                                         left_join( hs_group, by = c('Commodity' = 'HS_code') ) %>%
                                         group_by( Year, Country, Type_ie, Type_gs, Note, ISO2, lat, lon ) %>%
                                         summarize( Value = sum(Value, na.rm=T) ) %>%
                                         ungroup %>%
                                         mutate( Commodity = as.character( tmp_selected_ex() ) )
                                   })
                                
                                ### selcted commodity and service outputs
                                output$SelectedEx <- 
                                   renderText({
                                      tmp_selected_ex()
                                   })
                                
                                ## !!!!! try UI insert ---------------
                                insertUI(
                                   selector = '#body_ci_markets_ex_self_defined',
                                   ui =   div( id = 'body_ci_markets_ex_selector_self_defined',
                                               fluidRow(h1("Export markets analysis for selected commodity/service"),
                                                        uiOutput("CIEXSelectorByMarkets") ),
                                               fluidRow( shiny::span(h1( HTML(paste0(textOutput("SelectedEx"))), align = "center" ), style = "color:darkblue" ) )
                                   )
                                )
                                ## end Try UI insert -----------## 
                                
                                ## --- show loading message ------------------
                                observe({
                                   if( any(input$select_comodity_ex_for_market_analysis %in% tmp_tab$Name)  ){
                                      shinyjs::show( id = "body_ci_market_loading_message_self_define" )
                                   }
                                })
                                ## finish
                                
                                ### 2.4.0 Value Line and Percentage line for selected commodities ----------------
                                tmp_dtf_line_selected_ex <-
                                   reactive({
                                      tmp_dtf_key_line_ex %>%
                                         filter( HS_group %in% as.character( tmp_selected_ex() ) )
                                   })
                                
                                ### plot
                                output$CISelectedExportValueLine <- 
                                   renderHighchart({
                                      if( input$select_comodity_ex_for_market_analysis == "" ) 
                                         return(NULL)
                                      
                                      highchart() %>%
                                         hc_exporting(enabled = TRUE, formAttributes = list(target = "_blank")) %>%
                                         hc_xAxis( categories = c( unique( tmp_dtf_line_selected_ex()$Year) ) ) %>%
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
                                         hc_add_series( data =  tmp_dtf_line_selected_ex() %>% filter( Type_gs == 'Goods' ) ,
                                                        mapping = hcaes(  x = Year, y = Value, group = HS_group ),
                                                        type = 'line',
                                                        marker = list(symbol = 'circle') #,
                                                        #visible = c(T,rep(F,length(tmp_top_g_ex)-1))
                                         )
                                   })
                                
                                ## percentage line
                                tmp_dtf_percent_selected_line_ex <-
                                   reactive({
                                      tmp_dtf_percent_line_ex %>%
                                         filter( HS_group %in% as.character( tmp_selected_ex() ) )
                                   })
                                
                                # ### plot
                                output$CISelectedExportPercentLine <-
                                   renderHighchart({
                                      if( input$select_comodity_ex_for_market_analysis == "" ) 
                                         return(NULL)
                                      
                                      highchart() %>%
                                         hc_exporting(enabled = TRUE, formAttributes = list(target = "_blank")) %>%
                                         hc_xAxis( categories = c( unique( tmp_dtf_percent_selected_line_ex()$Year) ) ) %>%
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
                                         hc_add_series( data =  tmp_dtf_percent_selected_line_ex() %>% filter( Type_gs == 'Goods' ) ,
                                                        mapping = hcaes(  x = Year, y = Value, group = HS_group ),
                                                        type = 'line',
                                                        marker = list(symbol = 'circle') #,
                                                        #visible = c(T,rep(F,length(tmp_top_g_ex)-1))
                                         )
                                   })
                                
                                ## !!!!! try UI insert ----------- 
                                output$H1_export_trend_self_define_title <-
                                   renderText({
                                      if( input$select_comodity_ex_for_market_analysis == "" ) 
                                         return(NULL)
                                      paste0("Exports trend")
                                   })
                                
                                output$H1_export_trend_self_define_title_note <-
                                   renderText({
                                      if( input$select_comodity_ex_for_market_analysis == "" ) 
                                         return(NULL)
                                      paste0("Click on the commodity or service names in the legend area to show their trends")
                                   })
                                
                                output$H4_export_trend_self_define_value_title <-
                                   renderText({
                                      if( input$select_comodity_ex_for_market_analysis == "" ) 
                                         return(NULL)
                                      paste0("Export values")
                                   })
                                
                                output$H4_export_trend_self_define_percent_title <-
                                   renderText({
                                      if( input$select_comodity_ex_for_market_analysis == "" ) 
                                         return(NULL)
                                      paste0("As a percent of total exports")
                                   })
                                
                                ## insert ui here
                                insertUI(
                                   selector = '#body_ci_markets_ex_self_defined',
                                   ui =   div( id = 'body_selected_ex_line_value_percent_self_defined',
                                               fluidRow( h1( HTML(paste0(textOutput("H1_export_trend_self_define_title"))) ),
                                                         p( HTML(paste0(textOutput("H1_export_trend_self_define_title_note"))) ),
                                                         column(6, div(id = "body_value_selected_ex", 
                                                                       h4( HTML(paste0(textOutput("H4_export_trend_self_define_value_title"))) ), 
                                                                       highchartOutput('CISelectedExportValueLine') ) ),
                                                         column(6, div(id = "body_percent_selected_ex", 
                                                                       h4( HTML(paste0(textOutput("H4_export_trend_self_define_percent_title"))) ),
                                                                       highchartOutput('CISelectedExportPercentLine') ) ))
                                   )
                                )
                                ## end Try UI insert --------##
                                
                                ### 2.4.1 Self-defined: build highchart map  ---------------------------
                                print("--------- Building highchart map -------------")
                                tmp_dtf_market_ex_map <- 
                                   reactive({
                                      tmp_dtf_market_ex() %>%
                                         filter( Year == max(Year),
                                                 !is.na(lat) ) %>%
                                         mutate( Value = Value/10^6,
                                                 z= Value,
                                                 name = Country)
                                   })
                                
                                ## plot map
                                output$MapEXMarket <- 
                                   renderHighchart({
                                      if( input$select_comodity_ex_for_market_analysis == "" ) 
                                         return(NULL)
                                      
                                      hcmap( data = tmp_dtf_market_ex_map() ,
                                             value = 'Value',
                                             joinBy = c('iso-a2','ISO2'), 
                                             name="Exports value",
                                             borderWidth = 1,
                                             borderColor = "#fafafa",
                                             nullColor = "lightgrey",
                                             tooltip = list( table = TRUE,
                                                             sort = TRUE,
                                                             headerFormat = '<span style="font-size:13px">{series.name}</span><br/>',
                                                             pointFormat = '{point.name}: <b>${point.value:,.1f} m</b>' )
                                      ) %>%
                                         hc_add_series(data =  tmp_dtf_market_ex_map(),
                                                       type = "mapbubble",
                                                       color  = hex_to_rgba("#f1c40f", 0.9),
                                                       minSize = 0,
                                                       name="Exports value",
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
                                
                                ## !!!!! try UI insert ----------- 
                                output$H2_map_of_export_title <-
                                   renderText({
                                      if( input$select_comodity_ex_for_market_analysis == "" ) 
                                         return(NULL)
                                      paste0("Map of export values")
                                   })
                                
                                output$H2_map_of_export_title_note <-
                                   renderText({
                                      if( input$select_comodity_ex_for_market_analysis == "" ) 
                                         return(NULL)
                                      paste0("The size of bubble area and color both represent the value of exports.")
                                   })
                                
                                ## inserter UI here  
                                insertUI(
                                   selector = '#body_ci_markets_ex_self_defined',
                                   ui =   div( id = 'body_ci_markets_ex_map_self_defined',
                                               fluidRow(h2( HTML(paste0(textOutput("H2_map_of_export_title")))  ) ,
                                                        p( HTML(paste0(textOutput("H2_map_of_export_title_note")))  ),
                                                        highchartOutput('MapEXMarket') )
                                   )
                                )
                                ## end Try UI insert --------##
                                
                                ### 2.4.2 Self-defined: Top markets for selected commodity line chart ----------------
                                print("--------- Building Top market line chart -------------")
                                tmp_top_country_selected_ex <- 
                                   reactive({
                                      tmp_dtf_market_ex() %>%
                                         filter( Year == max(Year),
                                                 Value > 0 , 
                                                 !Country %in% c("World", 
                                                                 "Destination Unknown - EU")
                                         ) %>% ## 1 bn commodity
                                         arrange( -Value ) %>%
                                         dplyr::select( Country ) %>%
                                         as.matrix() %>%
                                         as.character
                                   })
                                
                                ### only show top 10 countries 
                                tmp_top10_country_selected_ex <-
                                   reactive({
                                      tmp_top_country_selected_ex()[1:min(10,length(tmp_top_country_selected_ex()))]
                                   })
                                
                                ## test the see top countries
                                # output$test_top_country_ex <- 
                                #    renderText({
                                #       tmp_top_country_selected_ex()
                                #    })
                                ### derive datafrom for the line plot
                                tmp_dtf_market_ex_line <- 
                                   reactive({
                                      tmp_dtf_market_ex() %>%
                                         filter( Country %in%  as.character(tmp_top_country_selected_ex()) ) %>%
                                         mutate( Value = Value/10^6 ,
                                                 Country = factor(Country, levels = as.character(tmp_top_country_selected_ex()) )
                                         ) %>%
                                         arrange(Country)
                                   })
                                
                                ## test the see top countries
                                # output$test_top_country_ex_dtf <- 
                                #    renderDataTable({
                                #       tmp_dtf_market_ex_line()
                                #    })
                                
                                ## line plot
                                output$SelectedExMarketLine <- renderHighchart({
                                   if( input$select_comodity_ex_for_market_analysis == "" ) 
                                      return(NULL)
                                   
                                   highchart() %>%
                                      hc_exporting(enabled = TRUE, formAttributes = list(target = "_blank")) %>%
                                      hc_add_series( data =  tmp_dtf_market_ex_line() %>%
                                                        filter( Country %in% as.character(tmp_top10_country_selected_ex()) ),
                                                     mapping = hcaes(  x = Year, y = Value, group = Country),
                                                     type = 'line',
                                                     marker = list(symbol = 'circle'), 
                                                     visible = c( rep(T,5), rep(F,length( as.character(tmp_top10_country_selected_ex()) )-5) )
                                      ) %>%
                                      hc_xAxis( categories = c( unique( tmp_dtf_market_ex_line()$Year) ) ) %>%
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
                                
                                
                                ### 2.4.3 Self-defined: Top markets for selected commodity percent line chart -------------------
                                print("--------- Building Top market line chart (Percent) -------------")
                                tmp_dtf_market_ex_line_percent <- 
                                   reactive({
                                      tmp_dtf_market_ex_line() %>%
                                         group_by(Year, Type_ie, Type_gs, Note, Commodity) %>%
                                         mutate( Share = Value/sum(Value, na.rm=T)) %>%
                                         ungroup %>%
                                         mutate( Value = Share*100 ) 
                                   })
                                
                                output$SelectedExMarketLinePercent <-
                                   renderHighchart({
                                      if( input$select_comodity_ex_for_market_analysis == "" ) 
                                         return(NULL)
                                      
                                      highchart() %>%
                                         hc_exporting(enabled = TRUE, formAttributes = list(target = "_blank")) %>%
                                         hc_add_series( data =  tmp_dtf_market_ex_line_percent() %>%
                                                           filter( Country %in% as.character(tmp_top10_country_selected_ex()) ),
                                                        mapping = hcaes(  x = Year, y = Value, group = Country),
                                                        type = 'line',
                                                        marker = list(symbol = 'circle'), 
                                                        visible = c( rep(T,5), rep(F,length( as.character(tmp_top10_country_selected_ex()) )-5) )
                                         ) %>%
                                         hc_xAxis( categories = c( unique( tmp_dtf_market_ex_line_percent()$Year) ) ) %>%
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
                                
                                ## !!!!! try UI insert ----------- 
                                output$H2_export_market_trend_title <-
                                   renderText({
                                      if( input$select_comodity_ex_for_market_analysis == "" ) 
                                         return(NULL)
                                      paste0("Top 10 export markets trends")
                                   })
                                
                                output$H2_export_market_trend_title_note <-
                                   renderText({
                                      if( input$select_comodity_ex_for_market_analysis == "" ) 
                                         return(NULL)
                                      paste0("Click on the country names in the legend area to show their trends")
                                   })
                                
                                output$H4_export_market_trend_value_title <-
                                   renderText({
                                      if( input$select_comodity_ex_for_market_analysis == "" ) 
                                         return(NULL)
                                      paste0("Export values")
                                   })
                                
                                output$H4_export_market_trend_percent_title <-
                                   renderText({
                                      if( input$select_comodity_ex_for_market_analysis == "" ) 
                                         return(NULL)
                                      paste0("As a percent of total exports of the selected")
                                   })
                                
                                ## insert ui here
                                insertUI(
                                   selector = '#body_ci_markets_ex_self_defined',
                                   ui =   div( id = 'body_ci_markets_ex_top_self_defined',
                                               fluidRow( h2( HTML(paste0(textOutput("H2_export_market_trend_title"))) ),
                                                         p( HTML(paste0(textOutput("H2_export_market_trend_title_note"))) ),
                                                         column(6, 
                                                                h4( HTML(paste0(textOutput("H4_export_market_trend_value_title"))) ),
                                                                highchartOutput("SelectedExMarketLine") 
                                                         ),
                                                         column(6,
                                                                h4( HTML(paste0(textOutput("H4_export_market_trend_percent_title"))) ),
                                                                highchartOutput("SelectedExMarketLinePercent")
                                                         )
                                               )
                                   )
                                )
                                ## end Try UI insert --------##
                                
                                ### 2.4.4 Self-defined: Growth prospective tab ----------------------
                                print("--------- Building Grwoth prospective table -------------")
                                tmp_tab_ex_growth <-
                                   reactive({
                                      tmp_dtf_market_ex_line() %>%
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
                                         left_join( tmp_dtf_market_ex_line() %>% rename(Name = Country) %>% filter( Year == max(Year) )  ) %>%
                                         left_join( tmp_dtf_market_ex_line_percent() %>% dplyr::select( -Value ) %>% rename( Name = Country) %>% filter( Year == max(Year) )  ) %>%
                                         dplyr::select( Name, Value, Share, CAGR1, CAGR5, CAGR10, ABS5, ABS10) %>%
                                         mutate( Name = factor(Name, levels = as.character(tmp_top_country_selected_ex()) ) ) %>%
                                         arrange( Name )
                                   })
                                
                                output$SelectedExMarketGrowthTab <- renderDataTable({
                                   if( input$select_comodity_ex_for_market_analysis == "" ) 
                                      return(NULL)
                                   
                                   datatable( tmp_tab_ex_growth(),
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
                                         background = styleColorBar( c(0, max(c(tmp_tab_ex_growth()$CAGR1,
                                                                                tmp_tab_ex_growth()$CAGR5,
                                                                                tmp_tab_ex_growth()$CAGR10))*2, na.rm=T) , 'lightblue'),
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
                                
                                
                                ## !!!!! try UI insert ----------- 
                                output$H2_market_ex_growth_tab_title <-
                                   renderText({
                                      if( input$select_comodity_ex_for_market_analysis == "" ) 
                                         return(NULL)
                                      paste0("Top export markets growth prospective")
                                   })
                                
                                output$H2_market_ex_growth_tab_title_note <-
                                   renderText({
                                      if( input$select_comodity_ex_for_market_analysis == "" ) 
                                         return(NULL)
                                      paste0("Compound annual growth rate (CAGR) for the past 1, 5, and 10 years. Absolute value change (ABS) for the past 5 and 10 years.")
                                   })
                                
                                ## insert ui here
                                insertUI(
                                   selector = '#body_ci_markets_ex_self_defined',
                                   ui =   div( id = 'body_ci_markets_ex_growth_self_defined',
                                               fluidRow( h2( HTML(paste0(textOutput("H2_market_ex_growth_tab_title"))) ),
                                                         p( HTML(paste0(textOutput("H2_market_ex_growth_tab_title_note"))) ),
                                                         dataTableOutput("SelectedExMarketGrowthTab")
                                               )
                                   )
                                )
                                ## end Try UI insert --------##
                                
                                
                                ## 2.5 Self-defined: show HS groupings in appendix -------------------
                                # output$HS_ex <- renderDataTable( hs_group,rownames = FALSE, 
                                #                                  extensions = 'Buttons',
                                #                                  options = list(dom = 'Bltp', buttons = c('copy', 'csv', 'excel', 'pdf', 'print'),
                                #                                                 pageLength = 5,
                                #                                                 lengthMenu = list(c(5,  -1), list('5', 'All')) 
                                #                                  ) 
                                # )
                                ## !!!!! try UI insert ----------- 
                                # insertUI(
                                #    selector = '#body_ci_markets_ex_self_defined',
                                #    ui =   div( id = 'body_appendix_hs_ex_self_defined',
                                #                conditionalPanel("input.rbtn_prebuilt_diy_ex == 'Pre-defined'",
                                #                                 fluidRow( tags$h1("Appendix -- HS grouping selected"),
                                #                                           div(id = 'output_hs_pre_ex', dataTableOutput( ("HS_pre_ex") ) )
                                #                                 )
                                #                ),
                                #                
                                #                conditionalPanel( "input.rbtn_prebuilt_diy_ex == 'Self-defined'",
                                #                                  fluidRow( tags$h1("Appendix -- HS grouping uploaded"),
                                #                                            div(id = 'output_hs_ex', dataTableOutput( ("HS_ex") ) )
                                #                                  )
                                #                                  
                                #                )
                                #    )
                                # )
                                ## end Try UI insert --------##
                                ## 2.6 Data for global situation from UN comtrade (ONLY for Export analysis) ----------------
                                print("--------- Building Reactive values for global analysis -------------")
                                rv_self_define_ex <- reactiveValues()
                                
                                ## put reactive values into observe  ------
                                observe({
                                   ## get data from un com trade using loop
                                   ## create a list first
                                   print("----------- Download Uncomtrade trade by country --------------")
                                   rv_self_define_ex$Fail_uncomtrade_country <- 
                                   try(
                                      rv_self_define_ex$tmp_global_by_country_raw_list <- 
                                         lapply( tmp_hs_ex() ,
                                                 function(i){
                                                    m_ct_search( reporters = "All", partners = 'World', trade_direction = c("imports", "exports"), freq = "annual",
                                                                 commod_codes = i,
                                                                 start_date = tmp_un_comtrade_max_year ,
                                                                 end_date = tmp_un_comtrade_max_year )  %>%
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
                                   rv_self_define_ex$Fail_uncomtrade_eu <- 
                                   try(
                                      rv_self_define_ex$tmp_global_by_eu_raw_list <- 
                                         lapply( tmp_hs_ex() ,
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
                                   if( class(rv_self_define_ex$Fail_uncomtrade_country) != 'try-error' ){
                                      print("----------- Success: Download Uncomtrade trade by country --------------")
                                      ## get list to data frame
                                      try(
                                      rv_self_define_ex$tmp_global_by_country_raw1 <- 
                                         do.call( rbind, rv_self_define_ex$tmp_global_by_country_raw_list )
                                      )
                                      
                                      ## change names
                                      try(
                                      rv_self_define_ex$tmp_global_by_country_raw <-
                                         rv_self_define_ex$tmp_global_by_country_raw1 %>%
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
                                   
                                   # try( 
                                   #    rv_self_define_ex$tmp_global_by_country_raw1 <- 
                                   #       do.call( rbind, rv_self_define_ex$tmp_global_by_country_raw_list )
                                   # )
                                   
                                   
                                   if( class(rv_self_define_ex$Fail_uncomtrade_eu) != 'try-error' ){
                                      print("----------- Success: Download Uncomtrade trade by EU --------------")
                                      ## get list to data frame
                                      try(
                                      rv_self_define_ex$tmp_global_by_eu_raw1 <- 
                                         do.call( rbind, rv_self_define_ex$tmp_global_by_eu_raw_list )
                                      )
                                      
                                      ## change names
                                      try(
                                      rv_self_define_ex$tmp_global_by_eu_raw <-
                                         rv_self_define_ex$tmp_global_by_eu_raw1 %>%
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
                                   
                                   # try( 
                                   #    rv_self_define_ex$tmp_global_by_eu_raw1 <- 
                                   #       do.call( rbind, rv_self_define_ex$tmp_global_by_eu_raw_list )
                                   # )   
                                   
                                   ### get data from un com trade -----
                                   # rv_self_define_ex$Fail_uncomtrade <- 
                                   #    try(
                                   #       rv_self_define_ex$tmp_global_by_country_raw <-
                                   #          rv_self_define_ex$tmp_global_by_country_raw1 %>%
                                   #          #get.Comtrade(r="all", p="0", rg = "1,2"  ## 1 means imports; 2 means exports (3 is re-exports excluded here)
                                   #          #            , ps = paste0(tmp_un_comtrade_max_year, "," ,tmp_un_comtrade_max_year-5)
                                   #          #            , cc = paste0(tmp_hs_ex(), collapse = ','), fmt = 'csv' )$data #%>%
                                   #          # dplyr::select( yr, cmdCode, rgDesc, rtTitle, rt3ISO, ptTitle, qtDesc,  TradeQuantity, TradeValue) %>%
                                   #          # mutate_all( as.character ) %>%
                                   #          # mutate( yr = as.numeric(yr),
                                   #          #         TradeQuantity = as.numeric( TradeQuantity ),
                                   #          #         TradeValue = as.numeric( TradeValue )
                                   #          #         ) %>%
                                   #          # rename( Year = yr, `Commodity.Code` = cmdCode ,
                                   #          #         `Trade.Flow` = rgDesc,
                                   #       #         Reporter = rtTitle,
                                   #       #         `Reporter.ISO` = rt3ISO,
                                   #       #         Partner = ptTitle,
                                   #       #         `Qty.Unit` = qtDesc,
                                   #       #         `Alt.Qty.Unit` = TradeQuantity,
                                   #       #         `Trade.Value..US..` = TradeValue )
                                   #       
                                   #       # m_ct_search( reporters = "All", partners = 'World', trade_direction = c("imports", "exports"), freq = "annual",
                                   #       #              commod_codes = as.character(tmp_hs_ex()),
                                   #       #              start_date = tmp_un_comtrade_max_year - 4,
                                   #       #              end_date = tmp_un_comtrade_max_year ) %>%
                                   #       #    bind_rows( m_ct_search( reporters = "All", partners = 'World', trade_direction = c("imports", "exports"), freq = "annual",
                                   #       #                            commod_codes = as.character(tmp_hs_ex()),
                                   #       #                            start_date = tmp_un_comtrade_max_year - 5,
                                   #       #                            end_date = tmp_un_comtrade_max_year - 5 )
                                   #       #               ) %>%
                                   #       #filter( year >= tmp_un_comtrade_max_year-5 &
                                   #       #          year <= tmp_un_comtrade_max_year ) %>%
                                   #       dplyr::select( year, commodity_code, trade_flow, reporter, reporter_iso, partner, qty_unit,  qty, trade_value_usd) %>%
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
                                   
                                   
                                   ## Eu export to world data
                                   # rv_self_define_ex$Fail_uncomtrade_eu <- 
                                   #    try(
                                   #       rv_self_define_ex$tmp_global_by_eu_raw <-
                                   #          rv_self_define_ex$tmp_global_by_eu_raw1 %>%
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
                                   
                                   ## 
                                   if( class(rv_self_define_ex$Fail_uncomtrade_country) == "try-error" )
                                      print(rv_self_define_ex$Fail_uncomtrade_country)
                                   
                                   if( class(rv_self_define_ex$Fail_uncomtrade_eu) == "try-error" )
                                      print(rv_self_define_ex$Fail_uncomtrade_eu)
                                   
                                   ## when both data downloaded successfully then do -------
                                   if( class(rv_self_define_ex$Fail_uncomtrade_country) != "try-error" & 
                                       class(rv_self_define_ex$Fail_uncomtrade_eu) != "try-error" & 
                                       !is.null(rv_self_define_ex$tmp_global_by_country_raw)  ){
                                      ## 1. format the data -----
                                      
                                      ## global import and export of A commodity (sum over all HS code under this commodity) by country
                                      rv_self_define_ex$tmp_global_by_country <- 
                                         rv_self_define_ex$tmp_global_by_country_raw %>%
                                         dplyr::select( Year,`Commodity.Code` , `Trade.Flow`, Reporter, `Reporter.ISO`, Partner, `Qty.Unit`, `Alt.Qty.Unit`, `Trade.Value..US..`) %>%
                                         #group_by(Year, `Trade.Flow`, Reporter, `Reporter.ISO`, Partner, `Qty.Unit`) %>%
                                         group_by(Year, `Trade.Flow`, Reporter, `Reporter.ISO`, Partner ) %>%
                                         summarise( `Alt.Qty.Unit` = sum(`Alt.Qty.Unit`, na.rm=T),
                                                    `Trade.Value..US..` = sum(`Trade.Value..US..`, na.rm=T) 
                                         ) %>%
                                         ungroup %>%
                                         mutate( Price = `Trade.Value..US..`/ `Alt.Qty.Unit`) 
                                      
                                      ## EU import and export of A commodity from world
                                      rv_self_define_ex$tmp_eu_trade_extra_raw <- 
                                         rv_self_define_ex$tmp_global_by_eu_raw %>%
                                         dplyr::select( Year,`Commodity.Code` , `Trade.Flow`, Reporter, `Reporter.ISO`, Partner, `Qty.Unit`, `Alt.Qty.Unit`, `Trade.Value..US..`) %>%
                                         #group_by(Year, `Trade.Flow`, Reporter, `Reporter.ISO`, Partner, `Qty.Unit`) %>%
                                         group_by(Year, `Trade.Flow`, Reporter, `Reporter.ISO`, Partner ) %>%
                                         summarise( `Alt.Qty.Unit` = sum(`Alt.Qty.Unit`, na.rm=T),
                                                    `Trade.Value..US..` = sum(`Trade.Value..US..`, na.rm=T)
                                         ) %>%
                                         ungroup 
                                      
                                      ## 5 yr change in value and prices % and abs 
                                      rv_self_define_ex$tmp_global_by_country_change <-    
                                         rv_self_define_ex$tmp_global_by_country %>%
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
                                      rv_self_define_ex$tmp_global_by_country_all <- 
                                         rv_self_define_ex$tmp_global_by_country %>%
                                         filter( Year == tmp_un_comtrade_max_year ) %>%
                                         left_join( rv_self_define_ex$tmp_global_by_country_change ) %>%
                                         group_by( Year, Trade.Flow  ) %>%
                                         mutate( Share = as.numeric(`Trade.Value..US..`)/ sum(as.numeric(`Trade.Value..US..`), na.rm=T ) ) %>%
                                         ungroup %>%
                                         arrange( `Trade.Flow`, -`Trade.Value..US..`) 
                                      
                                      ## 1.1 formate data -- get Eu28 intra and extra trade for later use in table ------
                                      rv_self_define_ex$tmp_eu_trade_all <- 
                                         rv_self_define_ex$tmp_global_by_country %>%
                                         filter( Reporter.ISO %in% concord_eu28$ISO3 ) %>%
                                         #group_by( Year , `Trade.Flow`, Partner, `Qty.Unit` ) %>%
                                         group_by( Year , `Trade.Flow`, Partner) %>%
                                         summarise(  `Alt.Qty.Unit` = sum( as.numeric(`Alt.Qty.Unit`), na.rm=T ),
                                                     `Trade.Value..US..` = sum( as.numeric(`Trade.Value..US..`), na.rm=T ) ) %>%
                                         ungroup %>%
                                         mutate( Reporter = "EU-28", Reporter.ISO = 'EU2'   )
                                      
                                      ## derive EU trade intra
                                      rv_self_define_ex$tmp_eu_trade_intra_raw <-
                                         rv_self_define_ex$tmp_eu_trade_all %>%
                                         left_join( rv_self_define_ex$tmp_eu_trade_extra_raw,
                                                    #by = c("Year", "Trade.Flow","Reporter", "Reporter.ISO", "Partner","Qty.Unit" )
                                                    by = c("Year", "Trade.Flow","Reporter", "Reporter.ISO", "Partner" )
                                         ) %>%
                                         mutate( `Alt.Qty.Unit` = Alt.Qty.Unit.x - Alt.Qty.Unit.y, 
                                                 `Trade.Value..US..` =  `Trade.Value..US...x` - `Trade.Value..US...y` ) %>%
                                         dplyr::select( -Alt.Qty.Unit.x, -Alt.Qty.Unit.y, 
                                                        -`Trade.Value..US...x`,  -`Trade.Value..US...y`) #%>%
                                      #mutate( Partner = "EU-28") 
                                      
                                      ### formate data
                                      rv_self_define_ex$tmp_eu_trade_intra <- 
                                         rv_self_define_ex$tmp_eu_trade_intra_raw %>%
                                         mutate( Reporter = 'EU-28-Intra', Reporter.ISO = 'EU2-intra' )
                                      
                                      rv_self_define_ex$tmp_eu_trade_extra <- 
                                         rv_self_define_ex$tmp_eu_trade_extra_raw %>%
                                         mutate( Reporter = 'EU-28-Extra', Reporter.ISO = 'EU2-extra' )
                                      
                                      ## join EU intra and extra back
                                      rv_self_define_ex$tmp_global_by_country_and_eu <-
                                         rv_self_define_ex$tmp_global_by_country_raw %>%
                                         filter( !Reporter.ISO %in% concord_eu28$ISO3 ) %>%
                                         dplyr::select( Year,`Commodity.Code` , `Trade.Flow`, Reporter, `Reporter.ISO`, Partner, `Qty.Unit`, `Alt.Qty.Unit`, `Trade.Value..US..`) %>%
                                         #group_by(Year, `Trade.Flow`, Reporter, `Reporter.ISO`, Partner, `Qty.Unit`) %>%
                                         group_by(Year, `Trade.Flow`, Reporter, `Reporter.ISO`, Partner) %>%
                                         summarise( `Alt.Qty.Unit` = sum(`Alt.Qty.Unit`, na.rm=T),
                                                    `Trade.Value..US..` = sum(`Trade.Value..US..`, na.rm=T)
                                         ) %>%
                                         ungroup %>%
                                         bind_rows( rv_self_define_ex$tmp_eu_trade_intra ) %>%
                                         bind_rows( rv_self_define_ex$tmp_eu_trade_extra  ) %>%
                                         mutate( Price = `Trade.Value..US..`/ `Alt.Qty.Unit`)
                                      
                                      ## 5 yr change in value and prices % and abs 
                                      rv_self_define_ex$tmp_global_by_country_and_eu_change <-    
                                         rv_self_define_ex$tmp_global_by_country_and_eu %>%
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
                                      rv_self_define_ex$tmp_global_by_country_and_eu_all <- 
                                         rv_self_define_ex$tmp_global_by_country_and_eu %>%
                                         filter( Year == tmp_un_comtrade_max_year ) %>%
                                         left_join( rv_self_define_ex$tmp_global_by_country_and_eu_change ) %>%
                                         group_by( Year, Trade.Flow  ) %>%
                                         mutate( Share = as.numeric(`Trade.Value..US..`)/ sum(as.numeric(`Trade.Value..US..`), na.rm=T ) ) %>%
                                         ungroup %>%
                                         arrange( `Trade.Flow`, -`Trade.Value..US..`) 
                                      
                                      
                                      ## 2. calculate values for later use ------------   
                                      ## Global market size -- value now
                                      rv_self_define_ex$tmp_global_size_value_now <- 
                                         rv_self_define_ex$tmp_global_by_country %>%
                                         group_by(Year, `Trade.Flow`,  Partner ) %>%
                                         summarise(`Trade.Value..US..` = sum(as.numeric(`Trade.Value..US..`), na.rm=T) ) %>%
                                         ungroup %>%
                                         filter( Year == tmp_un_comtrade_max_year,
                                                 `Trade.Flow` == 'Import') %>%
                                         dplyr::select( `Trade.Value..US..` ) %>%
                                         as.numeric()
                                      
                                      ## Global market size -- value 5 years ago
                                      rv_self_define_ex$tmp_global_size_value_pre <- 
                                         rv_self_define_ex$tmp_global_by_country %>%
                                         group_by(Year, `Trade.Flow`,  Partner ) %>%
                                         summarise(`Trade.Value..US..` = sum(as.numeric(`Trade.Value..US..`), na.rm=T) ) %>%
                                         ungroup %>%
                                         filter( Year == tmp_un_comtrade_max_year-5,
                                                 `Trade.Flow` == 'Import') %>%
                                         dplyr::select( `Trade.Value..US..` ) %>%
                                         as.numeric()
                                      
                                      ## Global market size -- value change %
                                      rv_self_define_ex$tmp_global_size_value_change <-
                                         CAGR( rv_self_define_ex$tmp_global_size_value_now/
                                                  rv_self_define_ex$tmp_global_size_value_pre, 5)/100
                                      
                                      ## Global market size -- value change abs
                                      rv_self_define_ex$tmp_global_size_value_change_abs <-
                                         rv_self_define_ex$tmp_global_size_value_now - rv_self_define_ex$tmp_global_size_value_pre 
                                      
                                      ## Top 3 importers share
                                      rv_self_define_ex$tmp_top3_importers_share <-
                                         rv_self_define_ex$tmp_global_by_country_all %>%
                                         filter( `Trade.Flow` == 'Import' ) %>%
                                         arrange( -Share ) %>%
                                         slice(1:3) %>%
                                         group_by(Year) %>%
                                         summarise( Share = sum(Share, na.rm=T) ) %>%
                                         ungroup %>%
                                         dplyr::select(Share) %>%
                                         as.numeric
                                      
                                      ## Top 10 importers share
                                      rv_self_define_ex$tmp_top10_importers_share <-
                                         rv_self_define_ex$tmp_global_by_country_all %>%
                                         filter( `Trade.Flow` == 'Import' ) %>%
                                         arrange( -Share ) %>%
                                         slice(1:10) %>%
                                         group_by(Year) %>%
                                         summarise( Share = sum(Share, na.rm=T) ) %>%
                                         ungroup %>%
                                         dplyr::select(Share) %>%
                                         as.numeric
                                      
                                      ##  of top 20 markets -- number of high growth market
                                      rv_self_define_ex$tmp_number_high_growth_importers <-
                                         nrow(
                                            rv_self_define_ex$tmp_global_by_country_all %>%
                                               filter( `Trade.Flow` == 'Import' ) %>%
                                               arrange( -Share ) %>%
                                               slice(1:20) %>%
                                               filter( Value_per_change >= 0.1 )
                                         )
                                      
                                      ## Top 3 exporters share
                                      rv_self_define_ex$tmp_top3_exporters_share <-
                                         rv_self_define_ex$tmp_global_by_country_all %>%
                                         filter( `Trade.Flow` == 'Export' ) %>%
                                         arrange( -Share ) %>%
                                         slice(1:3) %>%
                                         group_by(Year) %>%
                                         summarise( Share = sum(Share, na.rm=T) ) %>%
                                         ungroup %>%
                                         dplyr::select(Share) %>%
                                         as.numeric
                                      
                                      ## Top 10 exporters share
                                      rv_self_define_ex$tmp_top10_exporters_share <-
                                         rv_self_define_ex$tmp_global_by_country_all %>%
                                         filter( `Trade.Flow` == 'Export' ) %>%
                                         arrange( -Share ) %>%
                                         slice(1:10) %>%
                                         group_by(Year) %>%
                                         summarise( Share = sum(Share, na.rm=T) ) %>%
                                         ungroup %>%
                                         dplyr::select(Share) %>%
                                         as.numeric
                                      
                                      ## NZ's share
                                      rv_self_define_ex$tmp_nz_share <-
                                         rv_self_define_ex$tmp_global_by_country_all %>%
                                         filter( `Trade.Flow` == 'Export' ) %>%
                                         filter( Reporter == 'New Zealand' ) %>%
                                         dplyr::select(Share) %>%
                                         as.numeric
                                      
                                      ## 3. build data for importers and exporter maps -------------------
                                      rv_self_define_ex$tmp_un_comtrade_importer_map <- 
                                         rv_self_define_ex$tmp_global_by_country_all %>%
                                         filter( `Trade.Flow` == "Import" ) %>%
                                         left_join( concord_uncomtrade_country, by = c('Reporter.ISO' = 'ISO3') ) %>%
                                         filter( !is.na(lat) ) %>%
                                         mutate( Value = `Trade.Value..US..`/10^6,
                                                 z= Value,
                                                 name = Reporter)
                                      
                                      rv_self_define_ex$tmp_un_comtrade_exporter_map <- 
                                         rv_self_define_ex$tmp_global_by_country_all %>%
                                         filter( `Trade.Flow` == "Export" ) %>%
                                         left_join( concord_uncomtrade_country, by = c('Reporter.ISO' = 'ISO3') ) %>%
                                         filter( !is.na(lat) ) %>%
                                         mutate( Value = `Trade.Value..US..`/10^6,
                                                 z= Value,
                                                 name = Reporter)
                                      
                                      ## 4. Build data for the summary table -----------------
                                      ## import tab
                                      rv_self_define_ex$tmp_un_comtrade_import_summary_tab <- 
                                         rv_self_define_ex$tmp_global_by_country_and_eu_all %>%
                                         filter( `Trade.Flow` == 'Import' ) %>%
                                         dplyr::select( Reporter, Share, 
                                                        `Trade.Value..US..` ,Value_per_change, Value_abs_change,  
                                                        Price, Price_per_change ) %>%
                                         mutate( `Trade.Value..US..` = `Trade.Value..US..`/10^6,
                                                 Value_abs_change = Value_abs_change/10^6)
                                      
                                      ## export tab
                                      rv_self_define_ex$tmp_un_comtrade_export_summary_tab <- 
                                         rv_self_define_ex$tmp_global_by_country_and_eu_all %>%
                                         filter( `Trade.Flow` == 'Export' ) %>%
                                         dplyr::select( Reporter, Share, 
                                                        `Trade.Value..US..` ,Value_per_change, Value_abs_change,  
                                                        Price, Price_per_change ) %>%
                                         mutate( `Trade.Value..US..` = `Trade.Value..US..`/10^6,
                                                 Value_abs_change = Value_abs_change/10^6)
                                   }
                                })
                                
                                ## 2.6.1 IF hourly query reach 100 ------------
                                # output$Un_comtrade_fail_msg_self_define <- 
                                #    renderUI({
                                #       if( is.null(rv_self_define_ex$tmp_global_by_country_raw)  )
                                #          tags$h1( "Global analysis cannot be performed due to reaching usage limit of 100 requests per hour. Please come back in a hour time." )
                                #    })
                                # 
                                # insertUI(selector = '#body_ci_markets_ex_self_defined',
                                #          ui = div(id = "#body_ci_markets_ex_fail_msg_self_define",
                                #                   uiOutput("Un_comtrade_fail_msg_self_define")
                                #          )
                                # )
                                
                                ## 2.7 UN com Trade data analysis starts here Key facts table ----------
                                ## world market size
                                print("--------- Building facts value boxes -------------")
                                output$Un_comtrade_world_market_size_self_define <-
                                   renderInfoBox({
                                      if( is.null(rv_self_define_ex$tmp_global_by_country_raw)  )
                                         return(NULL)
                                      infoBox( "World market size",
                                               paste0("$", 
                                                      format(round(rv_self_define_ex$tmp_global_size_value_now/10^6), big.mark = ","),
                                                      " m"
                                               )
                                               , icon = icon('globe', lib = "glyphicon")
                                               
                                      )
                                   })
                                
                                ## 5 year growth
                                output$Un_comtrade_world_market_change_self_define <-
                                   renderInfoBox({
                                      if( is.null(rv_self_define_ex$tmp_global_by_country_raw)  )
                                         return(NULL)
                                      
                                      if( is.null(rv_self_define_ex$tmp_global_size_value_change) )
                                         infoBox( "CAGR (5 years)",
                                                  HTML(paste0( "Not available" )), 
                                                  icon = icon('minus'))
                                      
                                      if(rv_self_define_ex$tmp_global_size_value_change>0 ){
                                         infoBox( "CAGR (5 years)",
                                                  HTML(paste0( "<font color='green'> +",
                                                               round(abs(rv_self_define_ex$tmp_global_size_value_change)*100,1),
                                                               "% </font>"
                                                  )), 
                                                  icon = icon('arrow-up'), color = 'green')
                                      }else{
                                         infoBox( "CAGR (5 years)",
                                                  HTML(paste0( "<font color='red'> -",
                                                               round(abs(rv_self_define_ex$tmp_global_size_value_change)*100,1),
                                                               "% </font>"
                                                  )), 
                                                  icon = icon('arrow-down'), color = 'red')
                                      }
                                      
                                   })
                                
                                ## 5 yr abs change
                                output$Un_comtrade_world_market_change_abs_self_define <-
                                   renderInfoBox({
                                      if(  is.null(rv_self_define_ex$tmp_global_by_country_raw) )
                                         return(NULL)
                                      
                                      if( is.null(rv_self_define_ex$tmp_global_size_value_change_abs) )
                                         infoBox( "ABS (5 years)",
                                                  HTML(paste0( "Not available" )), 
                                                  icon = icon('minus'))
                                      
                                      if(rv_self_define_ex$tmp_global_size_value_change_abs>0 ){
                                         infoBox( "ABS (5 years)",
                                                  HTML(paste0("<font color='green'> +$", 
                                                              format(round(rv_self_define_ex$tmp_global_size_value_change_abs/10^6), big.mark = ","),
                                                              " m </font>"
                                                  )),
                                                  icon = icon('arrow-up'), color = 'green')
                                      }else{
                                         infoBox( "ABS (5 years)",
                                                  HTML(paste0("<font color='red'> -$", 
                                                              format(round(abs(rv_self_define_ex$tmp_global_size_value_change_abs)/10^6), big.mark = ","),
                                                              " m </font>"
                                                  )),
                                                  icon = icon('arrow-down'), color = 'red')
                                      }
                                   })
                                
                                ## top 3 importer share
                                output$Un_comtrade_top3_importers_share_self_define <-
                                   renderInfoBox({
                                      if( is.null(rv_self_define_ex$tmp_global_by_country_raw)  )
                                         return(NULL)
                                      infoBox( HTML("Top 3 importers <br> share"),
                                               paste0( 
                                                  round(abs(rv_self_define_ex$tmp_top3_importers_share)*100,1),
                                                  "%"
                                               ),
                                               icon = icon('import', lib = "glyphicon"))
                                   })
                                
                                ## top 10 importer share
                                output$Un_comtrade_top10_importers_share_self_define <-
                                   renderInfoBox({
                                      if(  is.null(rv_self_define_ex$tmp_global_by_country_raw)  )
                                         return(NULL)
                                      infoBox( HTML("Top 10 importers <br> share"),
                                               paste0( 
                                                  round(abs(rv_self_define_ex$tmp_top10_importers_share)*100,1),
                                                  "%"
                                               ),
                                               icon = icon('import', lib = "glyphicon"))
                                   })
                                
                                ##  of top 20 markets -- number of high growth market
                                output$Un_comtrade_high_growth_importers_self_define <-
                                   renderInfoBox({
                                      if(  is.null(rv_self_define_ex$tmp_global_by_country_raw)  )
                                         return(NULL)
                                      infoBox( HTML("Top 20 importers <br> with CAGR>10%"),
                                               paste0( rv_self_define_ex$tmp_number_high_growth_importers) ,
                                               icon = icon('import', lib = "glyphicon"))
                                   })
                                
                                
                                ## top 3 exporter share
                                output$Un_comtrade_top3_exporters_share_self_define <-
                                   renderInfoBox({
                                      if(  is.null(rv_self_define_ex$tmp_global_by_country_raw) )
                                         return(NULL)
                                      infoBox( HTML("Top 3 exporters <br> share"),
                                               paste0( 
                                                  round(abs(rv_self_define_ex$tmp_top3_exporters_share)*100,1),
                                                  "%"
                                               ),
                                               icon = icon('export', lib = "glyphicon"))
                                   })
                                
                                ## top 10 exporter share
                                output$Un_comtrade_top10_exporters_share_self_define <-
                                   renderInfoBox({
                                      if(  is.null(rv_self_define_ex$tmp_global_by_country_raw)  )
                                         return(NULL)
                                      infoBox( HTML("Top 10 exporters <br> share"),
                                               paste0( 
                                                  round(abs(rv_self_define_ex$tmp_top10_exporters_share)*100,1),
                                                  "%"
                                               ),
                                               icon = icon('export', lib = "glyphicon"))
                                   })
                                
                                ## new zealand share
                                output$Un_comtrade_nz_share_self_define <-
                                   renderInfoBox({
                                      if(  is.null(rv_self_define_ex$tmp_global_by_country_raw)  )
                                         return(NULL)
                                      if( rv_self_define_ex$tmp_nz_share < 0.001 ){
                                         infoBox( HTML("New Zealand <br> share"),
                                                  paste0( "Less than 0.1%" ),
                                                  icon = icon('export', lib = "glyphicon"))
                                      }else{
                                         infoBox( HTML("New Zealand <br> share"),
                                                  paste0( 
                                                     round(abs(rv_self_define_ex$tmp_nz_share)*100,1),
                                                     "%"
                                                  ),
                                                  icon = icon('export', lib = "glyphicon"))
                                      }
                                      
                                   })
                                
                                
                                ##!!!!! try UI insert: value box for global market facts ----------- 
                                output$H1_title_global_facts_self_define <-
                                   renderText({
                                      if( is.null(rv_self_define_ex$tmp_global_by_country_raw)   )
                                         return(NULL)
                                      paste0( "Global market analysis (", tmp_un_comtrade_max_year ,")" )
                                   })
                                
                                output$H1_title_global_facts_note_self_define <-
                                   renderText({
                                      if(  is.null(rv_self_define_ex$tmp_global_by_country_raw)  )
                                         return(NULL)
                                      paste0( "All values undner the global market analysis are reported in current US dollar" )
                                   })
                                
                                output$H3_title_global_facts_summary_self_define <-
                                   renderText({
                                      if(  is.null(rv_self_define_ex$tmp_global_by_country_raw)   )
                                         return(NULL)
                                      paste0( "Key facts and summary" )
                                   })
                                
                                ### insert global market key facts and summary value boxe
                                insertUI(
                                   selector = '#body_ci_markets_ex_self_defined',
                                   ui =   div( id = 'body_ci_markets_ex_global_facts_self_define',
                                               fluidRow( 
                                                  h1( HTML(paste0(textOutput("H1_title_global_facts_self_define"))) ),
                                                  p( HTML(paste0(textOutput("H1_title_global_facts_note_self_define"))) ),
                                                  h3( HTML(paste0(textOutput("H3_title_global_facts_summary_self_define"))) ),
                                                  infoBoxOutput("Un_comtrade_world_market_size_self_define") ,
                                                  infoBoxOutput("Un_comtrade_world_market_change_self_define" ) ,
                                                  infoBoxOutput("Un_comtrade_world_market_change_abs_self_define" ) 
                                               ),
                                               fluidRow(
                                                  infoBoxOutput("Un_comtrade_top3_importers_share_self_define" ) ,
                                                  infoBoxOutput("Un_comtrade_top10_importers_share_self_define" ) ,
                                                  infoBoxOutput("Un_comtrade_high_growth_importers_self_define" ) 
                                               ),
                                               fluidRow(
                                                  infoBoxOutput("Un_comtrade_top3_exporters_share_self_define" ) ,
                                                  infoBoxOutput("Un_comtrade_top10_exporters_share_self_define" ) ,
                                                  infoBoxOutput("Un_comtrade_nz_share_self_define" ) 
                                               )
                                   )
                                )
                                
                                
                                ## 2.8 Quick glance at both importers and exporters map --------
                                print("--------- Building importer and exporter map -------------")
                                output$UN_comtrade_importer_Map_self_define <- 
                                   renderHighchart({
                                      if(  is.null(rv_self_define_ex$tmp_global_by_country_raw)  )
                                         return(NULL)
                                      hcmap( data = rv_self_define_ex$tmp_un_comtrade_importer_map ,
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
                                         hc_add_series(data =  rv_self_define_ex$tmp_un_comtrade_importer_map ,
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
                                output$UN_comtrade_exporter_Map_self_define <- 
                                   renderHighchart({
                                      if(  is.null(rv_self_define_ex$tmp_global_by_country_raw)  )
                                         return(NULL)
                                      hcmap( data = rv_self_define_ex$tmp_un_comtrade_exporter_map ,
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
                                         hc_add_series(data =  rv_self_define_ex$tmp_un_comtrade_exporter_map ,
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
                                output$H3_title_un_comtrade_map_self_define <-
                                   renderText({
                                      if(  is.null(rv_self_define_ex$tmp_global_by_country_raw) )
                                         return(NULL)
                                      paste0("Global importers and exporters at a glance")
                                   })
                                
                                output$H3_title_un_comtrade_map_note_self_define <-
                                   renderText({
                                      if(  is.null(rv_self_define_ex$tmp_global_by_country_raw)  )
                                         return(NULL)
                                      paste0( "The size of bubble area and color both represent the value of imports or exports" ) 
                                   })
                                
                                output$H4_title_un_comtrade_importer_map_self_define <-
                                   renderText({
                                      if(  is.null(rv_self_define_ex$tmp_global_by_country_raw)  )
                                         return(NULL)
                                      paste0("Global IMPORT markets")
                                   })
                                
                                output$H4_title_un_comtrade_exporter_map_self_define <-
                                   renderText({
                                      if(  is.null(rv_self_define_ex$tmp_global_by_country_raw)  )
                                         return(NULL)
                                      paste0("Global EXPORT markets")
                                   })
                                
                                ## Insert ui here
                                insertUI(
                                   selector = '#body_ci_markets_ex_self_defined',
                                   ui =   div( id = 'body_ci_markets_ex_un_comtrade_map_self_define',
                                               fluidRow(h3( HTML(paste0(textOutput("H3_title_un_comtrade_map_self_define"))) ) ,
                                                        p( HTML(paste0(textOutput("H3_title_un_comtrade_map_note_self_define"))) ),
                                                        column(6, div(id = "body_ci_markets_ex_un_comtrade_map_import_self_define", h4( HTML(paste0(textOutput("H4_title_un_comtrade_importer_map_self_define"))) ), highchartOutput('UN_comtrade_importer_Map_self_define') ) ),
                                                        column(6, div(id = "body_ci_markets_ex_un_comtrade_map_export_self_define", h4( HTML(paste0(textOutput("H4_title_un_comtrade_exporter_map_self_define"))) ), highchartOutput('UN_comtrade_exporter_Map_self_define') ) )
                                               )
                                   )
                                )
                                ## end Try UI insert --------##
                                
                                
                                ## 2.8.1 Sankey plot for a commodity ---------------
                                print("--------- Building Sankey data -------------")
                                
                                observe({
                                   ## check if able to get sankey data
                                   rv_self_define_ex$Fail_sankey_data <-
                                      try(
                                         rv_self_define_ex$sankey_plot_data <-
                                            get_data_sankey_uncomtrade( cc = tmp_hs_ex(), max_year = tmp_un_comtrade_max_year, eu_internal = "No" )
                                      )
                                   
                                   if( class(rv_self_define_ex$Fail_sankey_data) == 'try-error' )
                                      print("--------- FAIL: building Sankey data !!! -------------")
                                })
                                
                                print("--------- Building Sankey plots -------------")
                                output$Sankey_trade_self_define <-
                                   renderSankeyNetwork({
                                      if(  is.null(rv_self_define_ex$tmp_global_by_country_raw) | 
                                           length(tmp_hs_ex())>1 |  
                                           class(rv_self_define_ex$Fail_sankey_data) == 'try-error' ){
                                         return(NULL)
                                      }else{
                                         print("--------- Plotting Sankey plots -------------")
                                         sankey_uncomtrade( cc = tmp_hs_ex(), max_year = tmp_un_comtrade_max_year,eu_internal = as.character(input$btn_eu_internal_self_define)  )
                                      }
                                   })
                                
                                ## !!!!! try UI insert: Sankey plot ----------- 
                                output$H3_title_sankey_self_define <-
                                   renderText({
                                      # if( is.null(rv_self_define_ex$tmp_global_by_country_raw) | 
                                      #     length(tmp_hs_ex())>1 |  
                                      #     class(rv_self_define_ex$Fail_sankey_data) == 'try-error' )
                                      #    #return(NULL)
                                      # {paste0("Unable to perform global trade flow analyasis due to data query limits. Please wait for a hour.")}else{
                                      #    paste0( "Global trade flow analysis" )
                                      # }
                                      
                                      if( class(rv_self_define_ex$Fail_sankey_data) == 'try-error' & 
                                          input$select_comodity_ex_for_market_analysis != "" &
                                          length(tmp_hs_ex()) == 1 ){
                                         paste0("Unable to perform global trade flow analyasis due to data query limits. Please wait for a hour.")
                                      }
                                      
                                      if( class(rv_self_define_ex$Fail_sankey_data) == 'try-error' & 
                                          input$select_comodity_ex_for_market_analysis == ""  ){
                                         return(NULL)
                                      }
                                      
                                      if( class(rv_self_define_ex$Fail_sankey_data) != 'try-error' &
                                          input$select_comodity_ex_for_market_analysis != "" &
                                          length(tmp_hs_ex()) ==  1){
                                         paste0( "Global trade flow analysis" )
                                      }
                                   })

                                output$H3_title_sankey_self_define_note <-
                                   renderUI({
                                      if( is.null(rv_self_define_ex$tmp_global_by_country_raw) | 
                                          length(tmp_hs_ex())>1 |  
                                          class(rv_self_define_ex$Fail_sankey_data) == 'try-error' ) 
                                         return(NULL)
                                      tags$p("This sankey plot shows trade flows of the selected commodity from expoters to importers. The displayed markets coverage is equal to or greater than 90% of global exports. The displayed trade flows are equal to or greater than 0.5% of global exports. Different colors are used to distinguish",
                                             tags$span( "EXPORTERS", style = "color: #97D700; font-weight: bold" ),
                                             ", ",
                                             tags$span( "IMPORTERS", style = "color: #CD5B45; font-weight: bold"),
                                             ", and ",
                                             tags$span( "BOTH", style = "color: #FBE122; font-weight: bold"), "." )

                                   })

                                ## button to choose show/hide EU internal trade
                                output$Btn_EU_Internal_self_define <-
                                   renderUI({
                                      if( is.null(rv_self_define_ex$tmp_global_by_country_raw) | 
                                          length(tmp_hs_ex())>1 |  
                                          class(rv_self_define_ex$Fail_sankey_data) == 'try-error' ) 
                                         return(NULL)
                                      radioButtons("btn_eu_internal_self_define",
                                                   p("Display EU internal trade: " ),
                                                   choiceNames = list(icon("check"), icon("times")),
                                                   choiceValues = list( "Yes" , "No"),
                                                   #c( "Yes" = "Yes", "No" = "No"),
                                                   inline=T,
                                                   selected="No")
                                   })

                                output$Btn_EU_Internal_self_define_note <-
                                   renderUI({
                                      if( is.null(rv_self_define_ex$tmp_global_by_country_raw) | 
                                          length(tmp_hs_ex())>1 |  
                                          class(rv_self_define_ex$Fail_sankey_data) == 'try-error' ) 
                                         return(NULL)
                                      tags$p( "You may choose to show or hide EU internal trade in the sankey plot by using the buttons below." )
                                   })

                                ## Insert ui here
                                insertUI(
                                   selector = '#body_ci_markets_ex_self_defined',
                                   ui =   div( id = 'body_ci_markets_ex_un_comtrade_sankey_self_define',
                                               fluidRow(h3( HTML(paste0(textOutput("H3_title_sankey_self_define"))) ) ,
                                                        #p( HTML(paste0(textOutput("H2_title_sankey_note"))) ),
                                                        uiOutput("H3_title_sankey_self_define_note"),
                                                        uiOutput("Btn_EU_Internal_self_define_note"),
                                                        uiOutput("Btn_EU_Internal_self_define"),
                                                        sankeyNetworkOutput( "Sankey_trade_self_define" )
                                               )
                                   )
                                )
                                ## end Try UI insert --------##
                                
                                ## 2.9 Generating summary tables for both importers and exporters -------
                                # container of the table -- importers 
                                print("--------- Building importer and exporter tabels -------------")
                                
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
                                output$UN_com_trade_importer_summary_self_define <-
                                   renderDataTable({
                                      if(  is.null(rv_self_define_ex$tmp_global_by_country_raw) )
                                         return(NULL)
                                      datatable( rv_self_define_ex$tmp_un_comtrade_import_summary_tab,
                                                 container = sketch_uncomtrade_im,
                                                 rownames = FALSE,
                                                 extensions = 'Buttons',
                                                 options = list(dom = 'Bltp', 
                                                                buttons = c('copy', 'csv', 'excel', 'pdf', 'print'),
                                                                scrollX = TRUE,
                                                                pageLength = 10,
                                                                lengthMenu = list(c(10, 30 , -1), list('10','30' ,'All')),
                                                                columnDefs = list(list(className = 'dt-center', targets = 0:(ncol(rv_self_define_ex$tmp_un_comtrade_import_summary_tab)-1) ) )
                                                 )
                                      ) %>%
                                         formatPercentage( c('Share', 'Value_per_change', 'Price_per_change' ) , digit = 1 ) %>%
                                         formatCurrency( columns = c('Trade.Value..US..','Value_abs_change'), digits = 0 ) %>%
                                         formatCurrency( columns = c('Price'), digits = 2 ) %>%
                                         formatStyle(
                                            c('Value_per_change' ),
                                            background = styleColorBar( c(0,max(rv_self_define_ex$tmp_un_comtrade_import_summary_tab[1:min(20,nrow(rv_self_define_ex$tmp_un_comtrade_import_summary_tab)),c('Value_per_change' )],na.rm=T)*2) ,
                                                                        'lightblue'),
                                            backgroundSize = '100% 90%',
                                            backgroundRepeat = 'no-repeat',
                                            backgroundPosition = 'center',
                                            color = JS("value < 0 ? 'darkred' : value > 0 ? 'darkgreen' : 'black'")
                                         ) %>%
                                         formatStyle(
                                            c('Price_per_change' ),
                                            background = styleColorBar( c(0,max(rv_self_define_ex$tmp_un_comtrade_import_summary_tab[1:min(20,nrow(rv_self_define_ex$tmp_un_comtrade_import_summary_tab)),c('Price_per_change' )],na.rm=T)*2) ,
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
                                         formatStyle( 1:ncol(rv_self_define_ex$tmp_un_comtrade_import_summary_tab), 'vertical-align'='center', 'text-align' = 'center' )
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
                                output$UN_com_trade_exporter_summary_self_define <-
                                   renderDataTable({
                                      if(  is.null(rv_self_define_ex$tmp_global_by_country_raw) )
                                         return(NULL)
                                      datatable( rv_self_define_ex$tmp_un_comtrade_export_summary_tab,
                                                 container = sketch_uncomtrade_ex,
                                                 rownames = FALSE,
                                                 extensions = 'Buttons',
                                                 options = list(dom = 'Bltp', 
                                                                buttons = c('copy', 'csv', 'excel', 'pdf', 'print'),
                                                                scrollX = TRUE,
                                                                pageLength = 10,
                                                                lengthMenu = list(c(10, 30, -1), list('10', '30' ,'All')),
                                                                columnDefs = list(list(className = 'dt-center', targets = 0:(ncol(rv_self_define_ex$tmp_un_comtrade_export_summary_tab)-1) ) )
                                                 )
                                      ) %>%
                                         formatPercentage( c('Share', 'Value_per_change', 'Price_per_change' ) , digit = 1 ) %>%
                                         formatCurrency( columns = c('Trade.Value..US..','Value_abs_change'), digits = 0 ) %>%
                                         formatCurrency( columns = c('Price'), digits = 2 ) %>%
                                         formatStyle(
                                            c('Value_per_change' ),
                                            background = styleColorBar( c(0,max(rv_self_define_ex$tmp_un_comtrade_export_summary_tab[1:min(20,nrow(rv_self_define_ex$tmp_un_comtrade_export_summary_tab)),c('Value_per_change' )],na.rm=T)*2) ,
                                                                        'lightblue'),
                                            backgroundSize = '100% 90%',
                                            backgroundRepeat = 'no-repeat',
                                            backgroundPosition = 'center',
                                            color = JS("value < 0 ? 'darkred' : value > 0 ? 'darkgreen' : 'black'")
                                         ) %>%
                                         formatStyle(
                                            c('Price_per_change' ),
                                            background = styleColorBar( c(0,max(rv_self_define_ex$tmp_un_comtrade_export_summary_tab[1:min(20,nrow(rv_self_define_ex$tmp_un_comtrade_export_summary_tab)),c('Price_per_change' )],na.rm=T)*2) ,
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
                                         formatStyle( 1:ncol(rv_self_define_ex$tmp_un_comtrade_export_summary_tab), 'vertical-align'='center', 'text-align' = 'center' )
                                   })
                                
                                ## Insert ui here: summary tables  ----------------
                                output$H3_title_un_comtrade_summary_tab_self_define <-
                                   renderText({
                                      if(  is.null(rv_self_define_ex$tmp_global_by_country_raw) )
                                         return(NULL)
                                      paste0("Summary tables for importers and exporters")
                                   })
                                
                                
                                output$H4_title_un_comtrade_importer_sum_tab_self_define <-
                                   renderText({
                                      if(  is.null(rv_self_define_ex$tmp_global_by_country_raw)  )
                                         return(NULL)
                                      paste0("Global IMPORT markets")
                                   })
                                
                                output$H4_title_un_comtrade_exporter_sum_tab_self_define <-
                                   renderText({
                                      if(  is.null(rv_self_define_ex$tmp_global_by_country_raw)  )
                                         return(NULL)
                                      paste0("Global EXPORT markets")
                                   })
                                
                                insertUI(
                                   selector = '#body_ci_markets_ex_self_defined',
                                   ui =   div( id = 'body_ci_markets_ex_un_comtrade_summary_tab_self_define',
                                               fluidRow(h3( HTML(paste0(textOutput("H3_title_un_comtrade_summary_tab_self_define"))) ) ,
                                                        #p( HTML(paste0(textOutput("H3_title_un_comtrade_map_note"))) ),
                                                        column(6, div(id = "body_ci_markets_ex_un_comtrade_import_summary_tab_self_define", h4( HTML(paste0(textOutput("H4_title_un_comtrade_importer_sum_tab_self_define"))) ), dataTableOutput('UN_com_trade_importer_summary_self_define') ) ),
                                                        column(6, div(id = "body_ci_markets_ex_un_comtrade_export_summary_tab_self_define", h4( HTML(paste0(textOutput("H4_title_un_comtrade_exporter_sum_tab_self_define"))) ), dataTableOutput('UN_com_trade_exporter_summary_self_define') ) )
                                               )
                                   )
                                )
                                ## end Try UI insert --------##
                                
                                
                                ## 3.0 Get the leftover quota and reset time ---------
                                output$Un_comtrade_msg_self_define <-
                                   renderUI({
                                      #if(  is.null(rv_self_define_ex$tmp_global_by_country_raw) )
                                       #  return(NULL)
                                      # tags$p(paste0( "Note: ",ct_get_remaining_hourly_queries(), 
                                      #                " number of queries are left for the global analysis section from the UN Comtrade. The reset time will be at ", 
                                      #                ct_get_reset_time() ,
                                      #                ", while the current time is ", format(Sys.time()) , "."
                                      #                )
                                      #        )
                                      
                                      if(  input$select_comodity_ex_for_market_analysis == "" ){
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
                                
                                insertUI( selector = '#body_ci_markets_ex_self_defined',
                                          ui = div( id = 'body_ci_markets_ex_un_comtrade_msg_self_define',
                                                    fluidRow( #tags$hr(),
                                                              uiOutput("Un_comtrade_msg_self_define") )
                                                    ) 
                                )
                                ## 4.15 Hide generating report message ----------
                                observe({
                                   if( any(input$select_comodity_ex_for_market_analysis %in% tmp_tab$Name)  ){
                                      shinyjs::hide( id = "body_ci_market_loading_message_self_define" )
                                  }
                                })
                                
                                ## hide wait message ------
                                shinyjs::hide( id = 'wait_message_ci_ex' )
                             }
                             
                          }
                       }
                     }
                   )
      
      ### 1.2 Imports--- when press the Build Report button ------------------
      observeEvent( input$btn_build_commodity_report_im,
                    {
                       ## 1.1 check the inputs are correct -------------
                       ## first both checked not passed
                       tmp_execution_pre_define <- tmp_execution_self_define <- FALSE
                       
                       ## 1.2 work on Pre-deinfed Warning if no pre-defined commodity is selected ------------------------
                       if(input$rbtn_prebuilt_diy_im=='Pre-defined' & is.null(input$select_comodity_im)) {
                          showModal(modalDialog(
                             title = "Warning",
                             tags$b("Please select one or multiple pre-defined commodities!"),
                             size = 's'
                          ))
                       }
                       
                       ## if test pass
                       if( input$rbtn_prebuilt_diy_im=='Pre-defined' & !is.null(input$select_comodity_im) ){
                          tmp_execution_pre_define <- TRUE
                       }
                       
                       ## 1.2.1 Build graphs pre-defined commodity --------------
                       if(tmp_execution_pre_define){
                          ## --- hide howto -----
                          shinyjs::hide(id = 'ci_howto_im')
                          ## show waite message ----
                          shinyjs::show( id = 'wait_message_ci_im' )
                          ## disable the buttone ---
                          shinyjs::disable("btn_build_commodity_report_im")
                          ## disable the selection  ---
                          shinyjs::disable("select_comodity_im")
                          shinyjs::disable("rbtn_prebuilt_diy_im")
                          
                          ## first both checked not passed
                          checked_pre_defined_im <- checked_self_defined_im <- TRUE
                          
                          ### work on Data noW!!!!!!!
                          tmp_selected_service <- setdiff( input$select_comodity_im , list_snz_commodity_im[['Goods']] )
                          
                          snz_hs <- concord_snz_ig$HS_codes[concord_snz_ig$SNZ_commodity %in% input$select_comodity_im ]
                          
                          if( length(tmp_selected_service) >=1 ){
                             hs_group <-
                                concord_snz_ig %>%
                                filter( HS_codes %in% snz_hs ) %>%
                                bind_rows( data.frame(HS_codes = tmp_selected_service,
                                                      SNZ_commodity = tmp_selected_service) )
                          }else{
                             hs_group <-
                                concord_snz_ig %>%
                                filter( HS_codes %in% snz_hs )
                          }
                          
                          colnames(hs_group) <- c("HS_code", "HS_group")
                          ## make columns characters and make sure HS code has 01, and 0122 etc format
      
                          ## 3.1 Build import value line chart -------------------- 
                          tmp_top_g_im <-
                             dtf_shiny_commodity_service_im %>%
                             filter( SNZ_commodity %in% input$select_comodity_im ,
                                     !SNZ_commodity %in% tmp_selected_service,
                                     SNZ_commodity != 'Confidential data' ) %>%
                             filter( Year == max(Year)) %>% 
                             arrange( -Value ) %>%
                             dplyr::select( SNZ_commodity ) %>%
                             as.matrix() %>%
                             as.character
                          
                          tmp_top_s_im <-
                             dtf_shiny_commodity_service_im %>%
                             filter( SNZ_commodity %in% tmp_selected_service) %>%
                             filter( Year == max(Year) ) %>%
                             arrange( -Value ) %>%
                             dplyr::select( SNZ_commodity ) %>%
                             as.matrix() %>%
                             as.character
                          
                          ## top selected commodities and top 5services
                          tmp_top_im <- c( tmp_top_g_im, tmp_top_s_im)
                          
                          ## data frame to plot
                          tmp_dtf_key_line_im <- 
                             dtf_shiny_commodity_service_im %>%
                             filter( SNZ_commodity %in% tmp_top_im,
                                     Year >=2007) %>%
                             mutate( Value = round(Value/10^6),
                                     SNZ_commodity = factor(SNZ_commodity, levels = tmp_top_im)
                             ) %>%
                             arrange( SNZ_commodity )
                          
                          ### plot
                          tmp_import_hc <- 
                             highchart() %>%
                             hc_exporting(enabled = TRUE, formAttributes = list(target = "_blank")) %>%
                             # hc_add_series( data =  tmp_dtf_key_line_im %>% filter( Type_gs == 'Goods' ) ,
                             #                mapping = hcaes(  x = Year, y = Value, group = SNZ_commodity ),
                             #                type = 'line',
                             #                marker = list(symbol = 'circle') #,
                             #                #visible = c(T,rep(F,length(tmp_top_g_im)-1))
                             # ) %>%
                             hc_xAxis( categories = c( unique( tmp_dtf_key_line_im$Year) ) ) %>%
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
                                                              " {series.name}: ${point.y} m"),
                                        headerFormat = '<span style="font-size: 13px">Year {point.key}</span>'
                             ) %>%
                             hc_legend( layout = 'vertical', align = 'left', verticalAlign = 'top', floating = T, x = 100, y = -15 )
                          
                          ### if any services are selected?
                          if( length(tmp_top_g_im)>=1 & length(tmp_top_s_im)==0 ){
                             output$CIImportValueLine <- 
                                renderHighchart(
                                   tmp_import_hc %>%
                                      hc_add_series( data =  tmp_dtf_key_line_im %>% filter( Type_gs == 'Goods' ) ,
                                                     mapping = hcaes(  x = Year, y = Value, group = SNZ_commodity ),
                                                     type = 'line',
                                                     marker = list(symbol = 'circle') #,
                                                     #visible = c(T,rep(F,length(tmp_top_g_im)-1))
                                      )
                                )
                          }
                          if( length(tmp_top_s_im)>=1 & length(tmp_top_g_im)==0 ){
                             output$CIImportValueLine <- 
                                renderHighchart(
                                   tmp_import_hc %>%
                                      hc_add_series( data =  tmp_dtf_key_line_im %>% filter( Type_gs == 'Services' ),
                                                     mapping = hcaes(  x = Year, y = Value, group = SNZ_commodity ),
                                                     type = 'line', dashStyle = 'DashDot', marker = list(symbol = 'circle') #,
                                                     #visible = c(T,rep(F,length(tmp_top_s_im)-1))
                                      )
                                )
                          }
                          if( length(tmp_top_s_im)>=1 & length(tmp_top_g_im) >= 1 ) {
                             output$CIImportValueLine <- 
                                renderHighchart(
                                   tmp_import_hc %>%
                                      hc_add_series( data =  tmp_dtf_key_line_im %>% filter( Type_gs == 'Goods' ) ,
                                                     mapping = hcaes(  x = Year, y = Value, group = SNZ_commodity ),
                                                     type = 'line',
                                                     marker = list(symbol = 'circle') #,
                                                     #visible = c(T,rep(F,length(tmp_top_g_im)-1))
                                      ) %>%
                                      hc_add_series( data =  tmp_dtf_key_line_im %>% filter( Type_gs == 'Services' ),
                                                     mapping = hcaes(  x = Year, y = Value, group = SNZ_commodity ),
                                                     type = 'line', dashStyle = 'DashDot', marker = list(symbol = 'circle') #,
                                                     #visible = c(T,rep(F,length(tmp_top_s_im)-1))
                                      )
                                )
                          }
                          ## 3.2 build import as a percent of total import line chart -----------------------
                          tmp_tot_im <-
                             dtf_shiny_full %>%
                             filter( Country == 'World',
                                     Type_ie == 'Imports',
                                     Year >= 2007 )  %>%
                             mutate( Value = round(Value/10^6) ) %>%
                             group_by( Year, Country, Type_ie ) %>%
                             summarize( Value = sum(Value, na.rm=T) ) %>%
                             ungroup %>%
                             mutate( SNZ_commodity = 'Total imports' )
                          
                          tmp_dtf_percent_line_im <-
                             tmp_dtf_key_line_im %>%
                             bind_rows( tmp_tot_im ) %>%
                             group_by( Year, Country, Type_ie ) %>%
                             mutate( Share = Value/Value[SNZ_commodity=='Total imports'],
                                     Value = Share*100 ) %>%
                             ungroup %>%
                             filter( SNZ_commodity != 'Total imports' ) %>%
                             mutate( SNZ_commodity = factor(SNZ_commodity, levels = tmp_top_im) ) %>%
                             arrange( SNZ_commodity )
                          
                          ### plot
                          tmp_import_percent_hc <- 
                             highchart() %>%
                             hc_exporting(enabled = TRUE, formAttributes = list(target = "_blank")) %>%
                             hc_xAxis( categories = c( unique( tmp_dtf_percent_line_im$Year) ) ) %>%
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
                          #hc_legend( enabled = FALSE )
                          
                          ### if any services are selected?
                          if( length(tmp_top_g_im)>=1&length(tmp_top_s_im)==0 ) {
                             output$CIImportPercentLine <- 
                                renderHighchart(
                                   tmp_import_percent_hc %>%
                                      hc_add_series( data =  tmp_dtf_percent_line_im %>% filter( Type_gs == 'Goods' ) ,
                                                     mapping = hcaes(  x = Year, y = Value, group = SNZ_commodity ),
                                                     type = 'line',
                                                     marker = list(symbol = 'circle') #,
                                                     #visible = c(T,rep(F,length(tmp_top_g_ex)-1))
                                      )
                                )
                          }
                          if( length(tmp_top_g_im)==0 & length(tmp_top_s_im)>=1 ){
                             output$CIImportPercentLine <- 
                                renderHighchart(
                                   tmp_import_percent_hc %>%
                                      hc_add_series( data =  tmp_dtf_percent_line_im %>% filter( Type_gs == 'Services' ),
                                                     mapping = hcaes(  x = Year, y = Value, group = SNZ_commodity ),
                                                     type = 'line', dashStyle = 'DashDot', marker = list(symbol = 'circle') #,
                                                     #visible = c(T,rep(F,length(tmp_top_s_ex)-1))
                                      )
                                )
                          }
                          if( length(tmp_top_g_im)>=1 & length(tmp_top_s_im)>=1 ){
                             output$CIImportPercentLine <- 
                                renderHighchart(
                                   tmp_import_percent_hc %>%
                                      hc_add_series( data =  tmp_dtf_percent_line_im %>% filter( Type_gs == 'Goods' ) ,
                                                     mapping = hcaes(  x = Year, y = Value, group = SNZ_commodity ),
                                                     type = 'line',
                                                     marker = list(symbol = 'circle') #,
                                                     #visible = c(T,rep(F,length(tmp_top_g_ex)-1))
                                      ) %>%
                                      hc_add_series( data =  tmp_dtf_percent_line_im %>% filter( Type_gs == 'Services' ),
                                                     mapping = hcaes(  x = Year, y = Value, group = SNZ_commodity ),
                                                     type = 'line', dashStyle = 'DashDot', marker = list(symbol = 'circle') #,
                                                     #visible = c(T,rep(F,length(tmp_top_s_ex)-1))
                                      )
                                )
                          }
                          
                          ## !!!!! try UI insert ----------- 
                          insertUI(
                             selector = '#body_im',
                             ui =   div( id = 'body_im_line_value_percent',
                                         fluidRow( h1("Imports for selected commodities/services"),
                                                   p("Click on the commodity or service names in the legend area to show their trends"),
                                                   column(6, div(id = "body_value_im", h4("Import values"), highchartOutput('CIImportValueLine') ) ),
                                                   column(6, div(id = "body_percent_im", h4("As a percent of total imports"), highchartOutput('CIImportPercentLine') ) ))
                             )
                          )
                          ## end Try UI insert --------##
                          
                          ## 3.3 build import value change table ----------------
                          ## data frame to plot
                          tmp_dtf_key_tab_im <- 
                             dtf_shiny_commodity_service_im %>%
                             filter( SNZ_commodity %in% tmp_top_im) %>%
                             mutate( SNZ_commodity = factor(SNZ_commodity, levels = tmp_top_im) ) %>%
                             arrange( SNZ_commodity )
                          
                          tmp_tab <-
                             tmp_dtf_key_tab_im %>%
                             mutate( Name =  SNZ_commodity ) %>%
                             group_by( Name) %>%
                             mutate( CAGR1 = CAGR( Value[Year == max(Year)]/
                                                      Value[Year == (max(Year)-1)], 1)/100,
                                     CAGR5 = CAGR( Value[Year == max(Year)]/
                                                      Value[Year == (max(Year)-5)], 5)/100,
                                     CAGR10 = CAGR( Value[Year == max(Year)]/
                                                       Value[Year == (max(Year)-10)], 10)/100,
                                     ABS5 = Value[Year == max(Year)] - Value[Year == (max(Year)-5)],
                                     ABS10 = Value[Year == max(Year)] - Value[Year == (max(Year)-10)]
                             ) %>%
                             ungroup %>%
                             filter( Year == max(Year) ) %>%
                             left_join(tmp_dtf_percent_line_im %>% dplyr::select(-CAGR5, -Value) ) %>%
                             dplyr::select( Name, Value, Share, CAGR1, CAGR5, CAGR10, ABS5, ABS10) %>%
                             mutate( Value =Value/10^6,
                                     ABS5 = ABS5/10^6,
                                     ABS10 = ABS10/10^6 ) %>%
                             #dplyr::select( Name, CAGR1, CAGR5, CAGR10) %>%
                             mutate( Name = factor(Name, levels = tmp_top_im) ) %>%
                             arrange( Name )
                          
                          ### join back to hs code
                          hs_group_flat <- 
                             hs_group %>%
                             group_by( HS_group ) %>%
                             summarise( HS_code = paste0(HS_code, collapse = '; ') ) %>%
                             ungroup
                          
                          tmp_tab %<>%
                             left_join( hs_group_flat, by = c("Name"= 'HS_group') ) %>%
                             dplyr::select( HS_code, Name, Value, Share, CAGR1, CAGR5, CAGR10, ABS5, ABS10 )
                          
                          #build table
                          output$GrowthTabSelectedIm <- renderDataTable(
                             datatable( tmp_tab,
                                        rownames = F,
                                        filter = c("top"),
                                        extensions = 'Buttons',
                                        options = list(dom = 'Bfltp',# 'Bt', 
                                                       buttons = c('copy', 'csv', 'excel', 'pdf', 'print') #, pageLength = -1 
                                                       ,scrollX = TRUE
                                                       #,fixedColumns = list(leftColumns = 2) 
                                                       ,autoWidth = T
                                                       ,pageLength = 10
                                                       ,lengthMenu = list(c(10,  -1), list('10', 'All')) ,
                                                       searchHighlight = TRUE,
                                                       search = list(regex = TRUE, caseInsensitive = FALSE )
                                                       ),
                                        colnames=c("HS codes", "Classification", 'Value ($m)', 'Share of total imports','CAGR 1', 'CAGR 5', 'CAGR 10', 'ABS5', 'ABS10')
                             ) %>%
                                formatStyle(
                                   c('CAGR1', 'CAGR5', 'CAGR10'),
                                   background = styleColorBar( c(0, max(c(tmp_tab$CAGR1,tmp_tab$CAGR5, tmp_tab$CAGR10))*2) , 'lightblue'),
                                   backgroundSize = '100% 90%',
                                   backgroundRepeat = 'no-repeat',
                                   backgroundPosition = 'center'
                                ) %>%
                                formatStyle(c('CAGR1', 'CAGR5', 'CAGR10', 'ABS5', 'ABS10'),
                                            color = JS("value < 0 ? 'darkred' : value > 0 ? 'darkgreen' : 'black'")) %>%
                                formatPercentage( c('Share','CAGR1', 'CAGR5', 'CAGR10'),digit = 1 ) %>%
                                formatStyle( columns = c('Name', 'Value', 'Share', 'CAGR1', 'CAGR5', 'CAGR10', 'ABS5', 'ABS10'), `font-size`= '115%' ) %>%
                                formatCurrency( columns = c('Value', 'ABS5', 'ABS10'), mark = ' ', digits = 1)
                          )
                          ## !!!!! try UI insert ----------- 
                          insertUI(
                             selector = '#body_growth_im',
                             ui =   div( id = 'body_im_growth_tab',
                                         fluidRow( h1("Short, medium, and long term growth for selected commodities/services"),
                                                   p("Compound annual growth rate (CAGR) for the past 1, 5, and 10 years. Absolute value change (ABS) for the past 5 and 10 years."),
                                                   dataTableOutput('GrowthTabSelectedIm')
                                         )
                             )
                          )
                          ## end Try UI insert --------##
                          
                          ## 3.4 Build import by country output groups -------------------
                          ## create a selector for each selected commodity ----------------------
                          output$CIIMSelectorByMarkets <- renderUI({
                             selectizeInput("select_comodity_im_for_market_analysis",
                                            tags$p("Please select or search a commodity/service for its market analysis"), 
                                            choices =  tmp_tab$Name[input$GrowthTabSelectedIm_rows_all]  , # input$select_comodity_im, 
                                            selected = NULL,  width = "500px",
                                            multiple = F)
                          })
                          
                          ### build data for market analysis -- these has to be reactive values
                          ## The name of the selected commodity
                          tmp_selected_im <- 
                             reactive({
                                input$select_comodity_im_for_market_analysis
                             })
                          
                          ## The HS codes of the selected commodity
                          tmp_hs_im <- 
                             reactive({
                                hs_group$HS_code[hs_group$HS_group == tmp_selected_im()]
                             })
                          
                          ## The data from of the selected commodity by markets
                          tmp_dtf_market_im <- 
                             reactive({
                                dtf_shiny_full %>%
                                   filter( Commodity %in% tmp_hs_im(), 
                                           Year >= 2007,
                                           Type_ie == 'Imports') %>%
                                   left_join( concord_country_iso_latlon_raw, by = 'Country' ) %>%
                                   group_by( Year, Country, Type_ie, Type_gs, Note, ISO2, lat, lon ) %>%
                                   summarize( Value = sum(Value, na.rm=T) ) %>%
                                   ungroup %>%
                                   mutate( Commodity = as.character( tmp_selected_im() ) )
                             })
                          
                          ### selcted commodity and service outputs
                          output$SelectedIm <- 
                             renderText({
                                tmp_selected_im()
                             })
                          
                          ## !!!!! try UI insert ----------- 
                          insertUI(
                             selector = '#body_ci_markets_im',
                             ui =   div( id = 'body_ci_markets_im_selector',
                                         fluidRow(h1("Import markets analysis for selected commodity/service"),
                                                  uiOutput("CIIMSelectorByMarkets") ),
                                         fluidRow( shiny::span(h1( HTML(paste0(textOutput("SelectedIm"))), align = "center" ), style = "color:darkblue" ) )
                             )
                          )
                          ## end Try UI insert --------##
                          
                          
                          ### 3.4.1 plot map ---------------
                          ### highchart map 
                          tmp_dtf_market_im_map <- 
                             reactive({
                                tmp_dtf_market_im() %>%
                                   filter( Year == max(Year),
                                           !is.na(lat) ) %>%
                                   mutate( Value = Value/10^6,
                                           z= Value,
                                           name = Country)
                             })
                          
                          output$MapIMMarket <- 
                             renderHighchart({
                                hcmap( data = tmp_dtf_market_im_map() ,
                                       value = 'Value',
                                       joinBy = c('iso-a2','ISO2'), 
                                       name="Imports value",
                                       borderWidth = 1,
                                       borderColor = "#fafafa",
                                       nullColor = "lightgrey",
                                       tooltip = list( table = TRUE,
                                                       sort = TRUE,
                                                       headerFormat = '<span style="font-size:13px">{series.name}</span><br/>',
                                                       pointFormat = '{point.name}: <b>${point.value:,.1f} m</b>' )
                                ) %>%
                                   hc_add_series(data =  tmp_dtf_market_im_map(),
                                                 type = "mapbubble",
                                                 color  = hex_to_rgba("#f1c40f", 0.9),
                                                 minSize = 0,
                                                 name="Imports value",
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

                          ## !!!!! try UI insert ----------- 
                          insertUI(
                             selector = '#body_ci_markets_im',
                             ui =   div( id = 'body_ci_markets_im_map',
                                         fluidRow(h2( paste0("Map of import values")  ) ,
                                                  p("The size of bubble area and color both represent the value of imports."),
                                                  highchartOutput('MapIMMarket') )
                             )
                          )
                          ## end Try UI insert --------##
                          
                          ### 3.4.2 Top markets for selected commodity line chart ----------------
                          tmp_top_country_selected_im <- 
                             reactive({
                                tmp_dtf_market_im() %>%
                                   filter( Year == max(Year),
                                           Value > 0, 
                                           !Country %in% c("World", 
                                                           "Destination Unknown - EU")
                                   ) %>% ## 1 bn commodity
                                   arrange( -Value ) %>%
                                   dplyr::select( Country ) %>%
                                   as.matrix() %>%
                                   as.character
                             })
                          
                          ### only show top 10 countries 
                          # if( length(tmp_top_country_selected_im())<=10 ){
                          #    tmp_top10_country_selected_im <-
                          #       reactive({
                          #          tmp_top_country_selected_im()
                          #       })
                          # }
                          # if( length(tmp_top_country_selected_im())>10 ){
                          #    tmp_top10_country_selected_im <-
                          #       reactive({
                          #          tmp_top_country_selected_im()[1:10]
                          #       })
                          # }
                          tmp_top10_country_selected_im <-
                             reactive({
                                tmp_top_country_selected_im()[1:min(10,length(tmp_top_country_selected_im()))]
                             })
                          
                          ### derive datafrom for the line plot
                          tmp_dtf_market_im_line <- 
                             reactive({
                                tmp_dtf_market_im() %>%
                                   filter( Country %in%  as.character(tmp_top_country_selected_im()) ) %>%
                                   mutate( Value = Value/10^6 ,
                                           Country = factor(Country, levels = as.character(tmp_top_country_selected_im()) )
                                   ) %>%
                                   arrange(Country)
                             })
                          
                          ## line plot
                          output$SelectedImMarketLine <- renderHighchart(
                             highchart() %>%
                                hc_exporting(enabled = TRUE, formAttributes = list(target = "_blank")) %>%
                                hc_add_series( data =  tmp_dtf_market_im_line() %>%
                                                  filter( Country %in% as.character(tmp_top10_country_selected_im()) ),
                                               mapping = hcaes(  x = Year, y = Value, group = Country),
                                               type = 'line',
                                               marker = list(symbol = 'circle'), 
                                               visible = c( rep(T,5), rep(F,length( as.character(tmp_top10_country_selected_im()) )-5) )
                                ) %>%
                                hc_xAxis( categories = c( unique( tmp_dtf_market_im_line()$Year) ) ) %>%
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
                          )
                          
                          ### 
                          ### 3.4.3 Top markets for selected commodity percent line chart -------------------
                          tmp_dtf_market_im_line_percent <- 
                             reactive({
                                tmp_dtf_market_im_line() %>%
                                   group_by(Year, Type_ie, Type_gs, Note, Commodity) %>%
                                   mutate( Share = Value/sum(Value, na.rm=T)) %>%
                                   ungroup %>%
                                   mutate( Value = Share*100 ) 
                             })
                          
                          output$SelectedImMarketLinePercent <-
                             renderHighchart(
                                highchart() %>%
                                   hc_exporting(enabled = TRUE, formAttributes = list(target = "_blank")) %>%
                                   hc_add_series( data =  tmp_dtf_market_im_line_percent() %>%
                                                     filter( Country %in% as.character(tmp_top10_country_selected_im()) ),
                                                  mapping = hcaes(  x = Year, y = Value, group = Country),
                                                  type = 'line',
                                                  marker = list(symbol = 'circle'), 
                                                  visible = c( rep(T,5), rep(F,length( as.character(tmp_top10_country_selected_im()) )-5) )
                                   ) %>%
                                   hc_xAxis( categories = c( unique( tmp_dtf_market_im_line_percent()$Year) ) ) %>%
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
                             )
                          
                          ## !!!!! try UI insert ----------- 
                          insertUI(
                             selector = '#body_ci_markets_im',
                             ui =   div( id = 'body_ci_markets_im_top',
                                         fluidRow( h2(paste0("Top 10 import markets trends") ),
                                                   p("Click on the country names in the legend area to show their trends"),
                                                   column(6, 
                                                          h4("Import values"),
                                                          highchartOutput("SelectedImMarketLine") 
                                                   ),
                                                   column(6,
                                                          h4("As a percent of total imports of the selected"),
                                                          highchartOutput("SelectedImMarketLinePercent")
                                                   )
                                         )
                             )
                          )
                          ## end Try UI insert --------##
                          
                          ### 3.4.4 Growth prospective table ----------------------
                          tmp_tab_im_growth <-
                             reactive({
                                tmp_dtf_market_im_line() %>%
                                   #filter( Country %in% as.character(tmp_top10_country_selected_im()) ) %>%
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
                                           ABS10 = as.numeric(ABS10) 
                                           ) %>%
                                   #filter( Year == max(Year) ) %>%
                                   left_join( tmp_dtf_market_im_line() %>% rename(Name = Country) %>% filter( Year == max(Year) )  ) %>%
                                   left_join( tmp_dtf_market_im_line_percent() %>% dplyr::select( -Value ) %>% rename( Name = Country) %>% filter( Year == max(Year) )  ) %>%
                                   dplyr::select( Name, Value, Share, CAGR1, CAGR5, CAGR10, ABS5, ABS10) %>%
                                   #dplyr::select( Name, CAGR1, CAGR5, CAGR10) %>%
                                   mutate( Name = factor(Name, levels = as.character(tmp_top_country_selected_im()) ) ) %>%
                                   arrange( Name )
                             })
                          
                          output$SelectedImMarketGrowthTab <- renderDataTable(
                             datatable( tmp_tab_im_growth(),
                                        rownames = F,
                                        extensions = 'Buttons',
                                        options = list(dom = 'Bltp',#'Bt', 
                                                       buttons = c('copy', 'csv', 'excel', 'pdf', 'print') #, pageLength = -1 
                                                       ,scrollX = TRUE
                                                       ,pageLength = 10
                                                       ,lengthMenu = list(c(10,  -1), list('10', 'All'))
                                                       ) ,
                                        colnames=c("Markets",'Value ($m)', 'Share','CAGR 1', 'CAGR 5', 'CAGR 10', 'ABS5', 'ABS10')
                             ) %>%
                                formatStyle(
                                   c('CAGR1', 'CAGR5', 'CAGR10'),
                                   background = styleColorBar( c(0, max(c(tmp_tab_im_growth()$CAGR1,
                                                                          tmp_tab_im_growth()$CAGR5, 
                                                                          tmp_tab_im_growth()$CAGR10))*2, na.rm=T) , 'lightblue'),
                                   backgroundSize = '100% 90%',
                                   backgroundRepeat = 'no-repeat',
                                   backgroundPosition = 'center'
                                ) %>%
                                formatStyle(c('CAGR1', 'CAGR5', 'CAGR10', 'ABS5', 'ABS10'),
                                            color = JS("value < 0 ? 'darkred' : value > 0 ? 'darkgreen' : 'black'")) %>%
                                formatPercentage( c('Share','CAGR1', 'CAGR5', 'CAGR10'),digit = 1 ) %>%
                                formatStyle( columns = c('Name','Value','Share','CAGR1', 'CAGR5', 'CAGR10'), `font-size`= '115%' ) %>%
                                formatCurrency( columns = c("Value", 'ABS5', 'ABS10'), mark = ' ', digits = 1)
                          )
                          ## !!!!! try UI insert ----------- 
                          insertUI(
                             selector = '#body_ci_markets_im',
                             ui =   div( id = 'body_ci_markets_im_growth',
                                         fluidRow( h2("Top import markets growth prospective"),
                                                   p("Compound annual growth rate (CAGR) for the past 1, 5, and 10 years. Absolute value change (ABS) for the past 5 and 10 years."),
                                                   dataTableOutput("SelectedImMarketGrowthTab")
                                         )
                             )
                          )
                          ## end Try UI insert --------##
                          
                          
                          ## 3.5 show HS groupings -------------------------
                          output$HS_pre_im <- renderDataTable( hs_group,rownames = FALSE, 
                                                               extensions = 'Buttons',
                                                               options = list(dom = 'Bltp', 
                                                                              buttons = c('copy', 'csv', 'excel', 'pdf', 'print'),
                                                                              scrollX = TRUE,
                                                                              pageLength = 5,
                                                                              lengthMenu = list(c(5, -1), list('5', 'All'))  ) 
                          )
                          #shinyjs::show(selector = '#body_appendix_hs_im')
                          
                          ## !!!!! try UI insert ----------- 
                          insertUI(
                             selector = '#body_ci_markets_im',
                             ui =   div( id = 'body_appendix_hs_im',
                                         conditionalPanel("input.rbtn_prebuilt_diy_im == 'Pre-defined'",
                                                          fluidRow( tags$h1("Appendix -- HS grouping selected"),
                                                                    div(id = 'output_hs_pre_im', dataTableOutput( ("HS_pre_im") ) )
                                                          )
                                         ),
                                         
                                         conditionalPanel( "input.rbtn_prebuilt_diy_im == 'Self-defined'",
                                                           fluidRow( tags$h1("Appendix -- HS grouping uploaded"),
                                                                     div(id = 'output_hs_im', dataTableOutput( ("HS_im") ) )
                                                           )
                                                           
                                         )
                             )
                          )
                          ## end Try UI insert --------##
                          
                          
                          ## hide waite message ----
                          shinyjs::hide( id = 'wait_message_ci_im' )
                       }
                       
                       
                       ## 1.3 work on Self defined warning if no .csv HS code and grouping uploaded -------------
                       if(input$rbtn_prebuilt_diy_im=='Self-defined' & is.null(input$file_comodity_im)) {
                          showModal(modalDialog(
                             title = "Warning",
                             tags$b("Please upload an appropriate CSV file with HS codes and groupsings!"),
                             size = 's'
                          ))
                       }
                       
                       ## Now if a csv file is uploaded -- check HS groupings
                       if(input$rbtn_prebuilt_diy_im=='Self-defined' & !is.null(input$file_comodity_im)){
                          ## warning if not a CSV file
                          if( !grepl(".csv",input$file_comodity_im$datapath)){
                             showModal(modalDialog(
                                title = "Warning",
                                tags$b("Only CSV files are accepted!"),
                                size = 's'
                             ))
                             
                          }else{
                             ## read the grouping
                             hs_group <-  read.csv(input$file_comodity_im$datapath, row.names = NULL) 
                             
                             ## check if the first column is HS code
                             tmp_hs_c1 <- gsub("[`]", "", hs_group[,1])
                             if( ncol(hs_group) >2 ){
                                showModal(modalDialog(
                                   title = "Warning",
                                   tags$p("Please check your uploaded HS groupings and make sure", 
                                          tags$b("it contains TWO columns only!")),
                                   size = 's'
                                ))
                             }else if( any( is.na( as.numeric(tmp_hs_c1) )  ) ){
                                showModal(modalDialog(
                                   title = "Warning",
                                   tags$p("Please check your uploaded HS groupings and make sure", 
                                          tags$b("the first column is HS codes!")),
                                   size = 's'
                                ))
                             }else if( any( nchar(tmp_hs_c1) > 6 ) ){
                                showModal(modalDialog(
                                   title = "Warning",
                                   tags$p("Please check your uploaded HS groupings and make sure", 
                                          tags$b("all HS codes are within level 6!") ),
                                   size = 's'
                                ))
                             }else{
                                ## first both checked not passed
                                tmp_execution_self_define <- TRUE
                             }
                             
                             ## 1.3.1 Build graphs self-defined commodity --------------
                             if(tmp_execution_self_define){
                                ## --- hide howto -----
                                shinyjs::hide(id = 'ci_howto_im')
                                ## show waite message ----
                                shinyjs::show( id = 'wait_message_ci_im' )
                                ## disable the buttone ---
                                shinyjs::disable("btn_build_commodity_report_im")
                                ## disable the upload button ---
                                shinyjs::disable("file_comodity_im")
                                shinyjs::disable("rbtn_prebuilt_diy_im")
                                
                                
                                ## make sure the HS codes become characters and HS 1 has 01 format
                                ## standerdise column names
                                colnames(hs_group) <- c("HS_code", "HS_group")
                                ## make columns characters and make sure HS code has 01, and 0122 etc format
                                hs_group %<>%
                                   mutate_all( funs(as.character) ) %>%
                                   mutate( HS_code = gsub("[`]","",HS_code) ) %>%
                                   mutate( HS_code = if_else(nchar(HS_code)%in%c(1,3,5), paste0("0", HS_code), HS_code  )  )
                                
                                ## 3.0.1 Self-define Build the main data.frame -- all selected commodity by country ------
                                tmp_dtf_shiny_full <-
                                   dtf_shiny_full %>%
                                   filter( Type_ie == 'Imports', 
                                           Commodity %in% hs_group$HS_code ) %>%
                                   left_join( concord_country_iso_latlon_raw, by = 'Country' ) %>%
                                   left_join( hs_group, by = c('Commodity' = 'HS_code') ) %>%
                                   group_by( Year, Country, Type_ie, Type_gs, HS_group, ISO2, lat, lon, Note ) %>%
                                   summarise( Value = sum(Value, na.rm=T) ) %>%
                                   ungroup
                                
                                #output$test_full_shiny <- renderDataTable(tmp_dtf_shiny_full)
                                
                                ## commodity only -- sum all countires
                                tmp_dtf_shiny_full_commodity_only <-
                                   tmp_dtf_shiny_full %>%
                                   group_by( Year,  Type_ie, Type_gs, HS_group, Note ) %>%
                                   summarise( Value = sum(Value, na.rm=T) ) %>%
                                   ungroup %>%
                                   mutate( Country = 'World' )
                                
                                ## 3.1 Self-defined: Build import value line chart -------------------- 
                                tmp_top_g_im <-
                                   tmp_dtf_shiny_full_commodity_only %>%
                                   filter( Year == max(Year)) %>% 
                                   arrange( -Value ) %>%
                                   dplyr::select( HS_group ) %>%
                                   as.matrix() %>%
                                   as.character
                                
                                
                                ## top selected commodities and top 5services
                                tmp_top_im <- c( tmp_top_g_im) #, tmp_top_s_ex)
                                
                                ## data frame to plot
                                tmp_dtf_key_line_im <- 
                                   tmp_dtf_shiny_full_commodity_only%>%
                                   filter( HS_group %in% tmp_top_im,
                                           Year >=2007) %>%
                                   mutate( Value = round(Value/10^6),
                                           HS_group = factor(HS_group, levels = tmp_top_im)
                                   ) %>%
                                   arrange( HS_group )
                                
                                ### plot
                                output$CIImportValueLine <- 
                                   renderHighchart(
                                      highchart() %>%
                                         hc_exporting(enabled = TRUE, formAttributes = list(target = "_blank")) %>%
                                         hc_xAxis( categories = c( unique( tmp_dtf_key_line_im$Year) ) ) %>%
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
                                         hc_add_series( data =  tmp_dtf_key_line_im %>% filter( Type_gs == 'Goods' ) ,
                                                        mapping = hcaes(  x = Year, y = Value, group = HS_group ),
                                                        type = 'line',
                                                        marker = list(symbol = 'circle') #,
                                                        #visible = c(T,rep(F,length(tmp_top_g_ex)-1))
                                         )
                                   )
                                
                                ## 3.2 Self-defined: build import as a percent of total export line chart -----------------------
                                tmp_tot_im <-
                                   dtf_shiny_full %>%
                                   filter( Country == 'World',
                                           Type_ie == 'Imports',
                                           Year >= 2007 )  %>%
                                   mutate( Value = round(Value/10^6) ) %>%
                                   group_by( Year, Country, Type_ie ) %>%
                                   summarize( Value = sum(Value, na.rm=T) ) %>%
                                   ungroup %>%
                                   mutate( HS_group = 'Total imports' )
                                
                                tmp_dtf_percent_line_im <-
                                   tmp_dtf_key_line_im %>%
                                   bind_rows( tmp_tot_im ) %>%
                                   group_by( Year, Country, Type_ie ) %>%
                                   mutate( Share = Value/Value[HS_group=='Total imports'],
                                           Value = Share*100 ) %>%
                                   ungroup %>%
                                   filter( HS_group != 'Total imports' ) %>%
                                   mutate( HS_group = factor(HS_group, levels = tmp_top_im) ) %>%
                                   arrange( HS_group )
                                
                                # ### plot
                                output$CIImportPercentLine <-
                                   renderHighchart(
                                      highchart() %>%
                                         hc_exporting(enabled = TRUE, formAttributes = list(target = "_blank")) %>%
                                         hc_xAxis( categories = c( unique( tmp_dtf_percent_line_im$Year) ) ) %>%
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
                                         hc_add_series( data =  tmp_dtf_percent_line_im %>% filter( Type_gs == 'Goods' ) ,
                                                        mapping = hcaes(  x = Year, y = Value, group = HS_group ),
                                                        type = 'line',
                                                        marker = list(symbol = 'circle') #,
                                                        #visible = c(T,rep(F,length(tmp_top_g_ex)-1))
                                         )
                                   )
                                
                                ## !!!!! try UI insert ----------- 
                                if( length(unique(hs_group$HS_group)) < 70 ){
                                   insertUI(
                                      selector = '#body_im_self_defined',
                                      ui =   div( id = 'body_im_line_value_percent_self_defined',
                                                  fluidRow( h1("Imports for selected commodities/services"),
                                                            p("Click on the commodity or service names in the legend area to show their trends"),
                                                            column(6, div(id = "body_value_im", h4("Import values"), highchartOutput('CIImportValueLine') ) ),
                                                            column(6, div(id = "body_percent_im", h4("As a percent of total imports"), highchartOutput('CIImportPercentLine') ) ))
                                      )
                                   )
                                }
                                ## end Try UI insert --------##
                                ## 2.3 Self-defined: build import value change table ----------------
                                ## data frame to plot
                                tmp_dtf_key_tab_im <- 
                                   tmp_dtf_shiny_full_commodity_only %>%
                                   filter( HS_group %in% tmp_top_im) %>%
                                   mutate( HS_group = factor(HS_group, levels = tmp_top_im) ) %>%
                                   arrange( HS_group )
                                
                                
                                tmp_tab <-
                                   tmp_dtf_key_tab_im %>%
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
                                   left_join( tmp_dtf_key_tab_im , 
                                              by =c('Name'='HS_group') ) %>%
                                   left_join( tmp_dtf_percent_line_im %>% dplyr::select( -Value) %>% rename(Name = HS_group) ) %>%
                                   filter( Year == max(Year) ) %>%
                                   mutate( Value = Value/10^6, ABS5 = ABS5/10^6, ABS10 = ABS10/10^6 ) %>%
                                   dplyr::select( Name, Value, Share, CAGR1, CAGR5, CAGR10, ABS5, ABS10) %>%
                                   mutate( Name = factor(Name, levels = tmp_top_im),
                                           CAGR1 = ifelse(CAGR1 %in% c(Inf,-Inf), NA, CAGR1),
                                           CAGR5 = ifelse(CAGR5 %in% c(Inf,-Inf), NA, CAGR5),
                                           CAGR10 = ifelse(CAGR10 %in% c(Inf,-Inf), NA, CAGR10)
                                   ) %>%
                                   arrange( Name )
                                
                                ### join back to hs code
                                hs_group_flat <- 
                                   hs_group %>%
                                   group_by( HS_group ) %>%
                                   summarise( HS_code = paste0(HS_code, collapse = '; ') ) %>%
                                   ungroup
                                
                                tmp_tab %<>%
                                   left_join( hs_group_flat, by = c("Name"= 'HS_group') ) %>%
                                   dplyr::select( HS_code, Name, Value, Share, CAGR1, CAGR5, CAGR10, ABS5, ABS10 )
                                

                                output$GrowthTabSelectedIm <- renderDataTable(
                                   datatable( tmp_tab,
                                              rownames = F,
                                              filter = c("top"),
                                              extensions = 'Buttons',
                                              options = list(dom = 'Bfltp',# 'Bt', 
                                                             buttons = c('copy', 'csv', 'excel', 'pdf', 'print') #, pageLength = -1 
                                                             ,scrollX = TRUE
                                                             #,fixedColumns = list(leftColumns = 2) 
                                                             ,autoWidth = T
                                                             ,pageLength = 10
                                                             ,lengthMenu = list(c(10,  -1), list('10', 'All')) ,
                                                             searchHighlight = TRUE,
                                                             search = list(regex = TRUE, caseInsensitive = FALSE )
                                                             ) ,
                                              colnames=c("HS codes", "Classification" ,'Value ($m)', 'Share of total imports','CAGR 1', 'CAGR 5', 'CAGR 10', 'ABS5', 'ABS10')
                                   ) %>%
                                      formatStyle(
                                         c('CAGR1', 'CAGR5', 'CAGR10'),
                                         background = styleColorBar( c(0, max(c(tmp_tab$CAGR1,tmp_tab$CAGR5, tmp_tab$CAGR10))*2, na.rm=T) , 'lightblue'),
                                         backgroundSize = '100% 90%',
                                         backgroundRepeat = 'no-repeat',
                                         backgroundPosition = 'center'
                                      ) %>%
                                      formatStyle(c('CAGR1', 'CAGR5', 'CAGR10', 'ABS5', 'ABS10'),
                                                  color = JS("value < 0 ? 'darkred' : value > 0 ? 'darkgreen' : 'black'")) %>%
                                      formatPercentage( c('Share','CAGR1', 'CAGR5', 'CAGR10'),digit = 1 ) %>%
                                      formatStyle( columns = c('Name','Value', 'Share','CAGR1', 'CAGR5', 'CAGR10', 'ABS5', 'ABS10'), `font-size`= '115%' ) %>%
                                      formatCurrency( columns = c('Value', 'ABS5', 'ABS10'), mark = ' ', digits = 1)
                                )
                                
                                ## !!!!! try UI insert ----------- 
                                insertUI(
                                   selector = '#body_growth_im_self_defined',
                                   ui =   div( id = 'body_im_growth_tab_self_defined',
                                               fluidRow( h1("Short, medium, and long term growth for selected commodities/services"),
                                                         p("Compound annual growth rate (CAGR) for the past 1, 5, and 10 years. Absolute value change (ABS) for the past 5 and 10 years."),
                                                         dataTableOutput('GrowthTabSelectedIm')
                                               )
                                   )
                                )
                                ## end Try UI insert --------##
                                
                                ## 3.4 Self-defined: Build import by country output groups -------------------
                                ## create a selector for each selected commodity ----------------
                                output$CIIMSelectorByMarkets <- renderUI({
                                   selectizeInput("select_comodity_im_for_market_analysis",
                                                  tags$p("Please select or search a commodity/service for its market analysis"), 
                                                  choices =  tmp_tab$Name[input$GrowthTabSelectedIm_rows_all], #tmp_top_im, 
                                                  selected = tmp_top_im[1],  width = "500px",
                                                  multiple = F)
                                })
                                
                                ### build data for market analysis -- these has to be reactive values
                                ## The name of the selected commodity
                                tmp_selected_im <- 
                                   reactive({
                                      input$select_comodity_im_for_market_analysis
                                   })
                                
                                ## The HS codes of the selected commodity
                                tmp_hs_im <- 
                                   reactive({
                                      hs_group$HS_code[hs_group$HS_group == tmp_selected_im()]
                                   })
                                
                                ## The data from of the selected commodity by markets
                                tmp_dtf_market_im <- 
                                   reactive({
                                      dtf_shiny_full %>%
                                         filter( Commodity %in% tmp_hs_im(), 
                                                 Year >= 2007,
                                                 Type_ie == 'Imports') %>%
                                         left_join( concord_country_iso_latlon_raw, by = 'Country' ) %>%
                                         group_by( Year, Country, Type_ie, Type_gs, Note, ISO2, lat, lon ) %>%
                                         summarize( Value = sum(Value, na.rm=T) ) %>%
                                         ungroup %>%
                                         mutate( Commodity = as.character( tmp_selected_im() ) )
                                   })
                                
                                ### selcted commodity and service outputs
                                output$SelectedIm <- 
                                   renderText({
                                      tmp_selected_im()
                                   })
                                
                                ## !!!!! try UI insert ----------- 
                                insertUI(
                                   selector = '#body_ci_markets_im_self_defined',
                                   ui =   div( id = 'body_ci_markets_im_selector_self_defined',
                                               fluidRow(h1("Import markets analysis for selected commodity/service"),
                                                        uiOutput("CIIMSelectorByMarkets") ),
                                               fluidRow( shiny::span(h1( HTML(paste0(textOutput("SelectedIm"))), align = "center" ), style = "color:darkblue" ) )
                                   )
                                )
                                ## end Try UI insert --------##
                                
                                ### 3.4.0 Value Line and Percentage line for selected commodities ----------------
                                tmp_dtf_line_selected_im <-
                                   reactive({
                                      tmp_dtf_key_line_im %>%
                                         filter( HS_group %in% as.character( tmp_selected_im() ) )
                                   })
                                
                                ### plot
                                output$CISelectedImportValueLine <- 
                                   renderHighchart(
                                      highchart() %>%
                                         hc_exporting(enabled = TRUE, formAttributes = list(target = "_blank")) %>%
                                         hc_xAxis( categories = c( unique( tmp_dtf_line_selected_im()$Year) ) ) %>%
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
                                         hc_add_series( data =  tmp_dtf_line_selected_im() %>% filter( Type_gs == 'Goods' ) ,
                                                        mapping = hcaes(  x = Year, y = Value, group = HS_group ),
                                                        type = 'line',
                                                        marker = list(symbol = 'circle') #,
                                                        #visible = c(T,rep(F,length(tmp_top_g_ex)-1))
                                         )
                                   )
                                
                                ## percentage line
                                tmp_dtf_percent_selected_line_im <-
                                   reactive({
                                      tmp_dtf_percent_line_im %>%
                                         filter( HS_group %in% as.character( tmp_selected_im() ) )
                                   })
                                
                                # ### plot
                                output$CISelectedImportPercentLine <-
                                   renderHighchart(
                                      highchart() %>%
                                         hc_exporting(enabled = TRUE, formAttributes = list(target = "_blank")) %>%
                                         hc_xAxis( categories = c( unique( tmp_dtf_percent_selected_line_im()$Year) ) ) %>%
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
                                         hc_add_series( data =  tmp_dtf_percent_selected_line_im() %>% filter( Type_gs == 'Goods' ) ,
                                                        mapping = hcaes(  x = Year, y = Value, group = HS_group ),
                                                        type = 'line',
                                                        marker = list(symbol = 'circle') #,
                                                        #visible = c(T,rep(F,length(tmp_top_g_ex)-1))
                                         )
                                   )
                                
                                ## !!!!! try UI insert ----------- 
                                insertUI(
                                   selector = '#body_ci_markets_im_self_defined',
                                   ui =   div( id = 'body_selected_im_line_value_percent_self_defined',
                                               fluidRow( h1("Imports trend"),
                                                         p("Click on the commodity or service names in the legend area to show their trends"),
                                                         column(6, div(id = "body_value_selected_im", h4("Import values"), highchartOutput('CISelectedImportValueLine') ) ),
                                                         column(6, div(id = "body_percent_selected_im", h4("As a percent of total imports"), highchartOutput('CISelectedImportPercentLine') ) ))
                                   )
                                )
                                ## end Try UI insert --------##
                                
                                ### 3.4.1 Self-defined: build highchart map  ---------------------------
                                tmp_dtf_market_im_map <- 
                                   reactive({
                                      tmp_dtf_market_im() %>%
                                         filter( Year == max(Year),
                                                 !is.na(lat) ) %>%
                                         mutate( Value = Value/10^6,
                                                 z= Value,
                                                 name = Country)
                                   })
                                
                                ## plot map
                                output$MapIMMarket <- 
                                   renderHighchart({
                                      hcmap( data = tmp_dtf_market_im_map() ,
                                             value = 'Value',
                                             joinBy = c('iso-a2','ISO2'), 
                                             name="Imports value",
                                             borderWidth = 1,
                                             borderColor = "#fafafa",
                                             nullColor = "lightgrey",
                                             tooltip = list( table = TRUE,
                                                             sort = TRUE,
                                                             headerFormat = '<span style="font-size:13px">{series.name}</span><br/>',
                                                             pointFormat = '{point.name}: <b>${point.value:,.1f} m</b>' )
                                      ) %>%
                                         hc_add_series(data =  tmp_dtf_market_im_map(),
                                                       type = "mapbubble",
                                                       color  = hex_to_rgba("#f1c40f", 0.9),
                                                       minSize = 0,
                                                       name="Imports value",
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
                                
                                ## !!!!! try UI insert ----------- 
                                insertUI(
                                   selector = '#body_ci_markets_im_self_defined',
                                   ui =   div( id = 'body_ci_markets_im_map_self_defined',
                                               fluidRow(h2( paste0("Map of import values")  ) ,
                                                        p("The size of bubble area and color both represent the value of imports."),
                                                        highchartOutput('MapIMMarket') )
                                   )
                                )
                                ## end Try UI insert --------##
                                
                                ### 3.4.2 Self-defined: Top markets for selected commodity line chart ----------------
                                tmp_top_country_selected_im <- 
                                   reactive({
                                      tmp_dtf_market_im() %>%
                                         filter( Year == max(Year),
                                                 Value > 0 , 
                                                 !Country %in% c("World", 
                                                                 "Destination Unknown - EU")
                                         ) %>% ## 1 bn commodity
                                         arrange( -Value ) %>%
                                         dplyr::select( Country ) %>%
                                         as.matrix() %>%
                                         as.character
                                   })
                                

                                tmp_top10_country_selected_im <-
                                   reactive({
                                      tmp_top_country_selected_im()[1:min(10,length(tmp_top_country_selected_im()))]
                                   })
                                
                                ## test the see top countries
                                # output$test_top_country_ex <- 
                                #    renderText({
                                #       tmp_top_country_selected_ex()
                                #    })
                                
                                ### derive datafrom for the line plot
                                tmp_dtf_market_im_line <- 
                                   reactive({
                                      tmp_dtf_market_im() %>%
                                         filter( Country %in%  as.character(tmp_top_country_selected_im()) ) %>%
                                         mutate( Value = Value/10^6 ,
                                                 Country = factor(Country, levels = as.character(tmp_top_country_selected_im()) )
                                         ) %>%
                                         arrange(Country)
                                   })
                                
                                ## test the see top countries
                                # output$test_top_country_ex_dtf <- 
                                #    renderDataTable({
                                #       tmp_dtf_market_ex_line()
                                #    })
                                
                                ## line plot
                                output$SelectedImMarketLine <- renderHighchart(
                                   highchart() %>%
                                      hc_exporting(enabled = TRUE, formAttributes = list(target = "_blank")) %>%
                                      hc_add_series( data =  tmp_dtf_market_im_line() %>%
                                                        filter( Country %in% as.character(tmp_top10_country_selected_im()) ),
                                                     mapping = hcaes(  x = Year, y = Value, group = Country),
                                                     type = 'line',
                                                     marker = list(symbol = 'circle'), 
                                                     visible = c( rep(T,5), rep(F,length( as.character(tmp_top10_country_selected_im()) )-5) )
                                      ) %>%
                                      hc_xAxis( categories = c( unique( tmp_dtf_market_im_line()$Year) ) ) %>%
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
                                )
                                
                                
                                ### 3.4.3 Self-defined: Top markets for selected commodity percent line chart -------------------
                                tmp_dtf_market_im_line_percent <- 
                                   reactive({
                                      tmp_dtf_market_im_line() %>%
                                         group_by(Year, Type_ie, Type_gs, Note, Commodity) %>%
                                         mutate( Share = Value/sum(Value, na.rm=T)) %>%
                                         ungroup %>%
                                         mutate( Value = Share*100 ) 
                                   })
                                
                                output$SelectedImMarketLinePercent <-
                                   renderHighchart(
                                      highchart() %>%
                                         hc_exporting(enabled = TRUE, formAttributes = list(target = "_blank")) %>%
                                         hc_add_series( data =  tmp_dtf_market_im_line_percent() %>%
                                                           filter( Country %in% as.character(tmp_top10_country_selected_im()) ),
                                                        mapping = hcaes(  x = Year, y = Value, group = Country),
                                                        type = 'line',
                                                        marker = list(symbol = 'circle'), 
                                                        visible = c( rep(T,5), rep(F,length( as.character(tmp_top10_country_selected_im()) )-5) )
                                         ) %>%
                                         hc_xAxis( categories = c( unique( tmp_dtf_market_im_line_percent()$Year) ) ) %>%
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
                                   )
                                
                                ## !!!!! try UI insert ----------- 
                                insertUI(
                                   selector = '#body_ci_markets_im_self_defined',
                                   ui =   div( id = 'body_ci_markets_im_top_self_defined',
                                               fluidRow( h2(paste0("Top 10 import markets trends") ),
                                                         p("Click on the country names in the legend area to show their trends"),
                                                         column(6, 
                                                                h4("Import values"),
                                                                highchartOutput("SelectedImMarketLine") 
                                                         ),
                                                         column(6,
                                                                h4("As a percent of total imports of the selected"),
                                                                highchartOutput("SelectedImMarketLinePercent")
                                                         )
                                               )
                                   )
                                )
                                ## end Try UI insert --------##
                                
                                ### 3.4.4 Self-defined: Growth prospective tab ----------------------
                                tmp_tab_im_growth <-
                                   reactive({
                                      tmp_dtf_market_im_line() %>%
                                         #filter( Country %in% as.character(tmp_top10_country_selected_im()) ) %>%
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
                                         left_join( tmp_dtf_market_im_line() %>% rename(Name = Country) %>% filter( Year == max(Year) )  ) %>%
                                         left_join( tmp_dtf_market_im_line_percent() %>% dplyr::select( -Value ) %>% rename( Name = Country) %>% filter( Year == max(Year) )  ) %>%
                                         dplyr::select( Name, Value, Share, CAGR1, CAGR5, CAGR10, ABS5, ABS10) %>%
                                         #dplyr::select( Name, CAGR1, CAGR5, CAGR10) %>%
                                         mutate( Name = factor(Name, levels = as.character(tmp_top_country_selected_im()) ) ) %>%
                                         arrange( Name )
                                   })
                                
                                output$SelectedImMarketGrowthTab <- renderDataTable(
                                   datatable( tmp_tab_im_growth(),
                                              rownames = F,
                                              extensions = 'Buttons',
                                              options = list(dom = 'Bltp',#'Bt', 
                                                             buttons = c('copy', 'csv', 'excel', 'pdf', 'print') #, pageLength = -1 
                                                             ,scrollX = TRUE
                                                             ,pageLength = 10
                                                             ,lengthMenu = list(c(10,  -1), list('10', 'All'))
                                                             ) ,
                                              colnames=c("Markets",'Value ($m)', 'Share of world market','CAGR 1', 'CAGR 5', 'CAGR 10', 'ABS5', 'ABS10')
                                   ) %>%
                                      formatStyle(
                                         c('CAGR1', 'CAGR5', 'CAGR10'),
                                         background = styleColorBar( c(0, max(c(tmp_tab_im_growth()$CAGR1,
                                                                                tmp_tab_im_growth()$CAGR5, 
                                                                                tmp_tab_im_growth()$CAGR10))*2, na.rm=T) , 'lightblue'),
                                         backgroundSize = '100% 90%',
                                         backgroundRepeat = 'no-repeat',
                                         backgroundPosition = 'center'
                                      ) %>%
                                      formatStyle(c('CAGR1', 'CAGR5', 'CAGR10', 'ABS5', 'ABS10'),
                                                  color = JS("value < 0 ? 'darkred' : value > 0 ? 'darkgreen' : 'black'")) %>%
                                      formatPercentage( c('Share','CAGR1', 'CAGR5', 'CAGR10'),digit = 1 ) %>%
                                      formatStyle( columns = c('Name', 'Value','Share' ,'CAGR1', 'CAGR5', 'CAGR10'), `font-size`= '115%' ) %>%
                                      formatCurrency( columns = c('Value', 'ABS5', 'ABS10'), mark = ' ', digits = 1)
                                )
                                
                                ## !!!!! try UI insert ----------- 
                                insertUI(
                                   selector = '#body_ci_markets_im_self_defined',
                                   ui =   div( id = 'body_ci_markets_im_growth_self_defined',
                                               fluidRow( h2("Top import markets growth prospective"),
                                                         p("Compound annual growth rate (CAGR) for the past 1, 5, and 10 years. Absolute value change (ABS) for the past 5 and 10 years."),
                                                         dataTableOutput("SelectedImMarketGrowthTab")
                                               )
                                   )
                                )
                                ## end Try UI insert --------##
                                
                                
                                ## 3.5 Self-defined: show HS groupings in appendix -------------------
                                output$HS_im <- renderDataTable( hs_group,rownames = FALSE, 
                                                                 extensions = 'Buttons',
                                                                 options = list(dom = 'Bltp', buttons = c('copy', 'csv', 'excel', 'pdf', 'print'),
                                                                                pageLength = 5,
                                                                                lengthMenu = list(c(5,  -1), list('5', 'All')) 
                                                                 ) 
                                )
                                
                                ## !!!!! try UI insert ----------- 
                                insertUI(
                                   selector = '#body_ci_markets_im_self_defined',
                                   ui =   div( id = 'body_appendix_hs_im_self_defined',
                                               conditionalPanel("input.rbtn_prebuilt_diy_im == 'Pre-defined'",
                                                                fluidRow( tags$h1("Appendix -- HS grouping selected"),
                                                                          div(id = 'output_hs_pre_im', dataTableOutput( ("HS_pre_im") ) )
                                                                )
                                               ),
                                               
                                               conditionalPanel( "input.rbtn_prebuilt_diy_im == 'Self-defined'",
                                                                 fluidRow( tags$h1("Appendix -- HS grouping uploaded"),
                                                                           div(id = 'output_hs_im', dataTableOutput( ("HS_im") ) )
                                                                 )
                                                                 
                                               )
                                   )
                                )
                                ## end Try UI insert --------##
                                ## show waite message ----
                                shinyjs::hide( id = 'wait_message_ci_im' )
                             }
                          }
                       }
                    }
           )
      
      
      ## III. Country intelligence ------------------------
      observeEvent(input$btn_build_country_report,
                   {
                      tmp_execution <- FALSE
                      
                      if(is.null(input$select_country)) {
                         showModal(modalDialog(
                            title = "Warning",
                            tags$b("Please select one or multiple countries or a country group!"),
                            size = 's'
                         ))
                      }
                      
                      ## III.0 select country or country group prerequisit ------------------------
                      ## One can either select one or multiple countries, OR only one country group
                      if( any(input$select_country %in% list_country[['Country groups']]) &
                          length(input$select_country)>1 ){
                         showModal(modalDialog(
                            title = "Warning",
                            tags$b("You can select only ONE of the country groups!"),
                            size = 's'
                         ))
                      }
                      
                      ##  a country group selected
                      if( any(input$select_country %in% list_country[['Country groups']]) & length(input$select_country)==1 ){
                         tmp_selected_countries <- concord_country_member$Country[concord_country_member$Group==input$select_country]
                         tmp_execution <- TRUE
                         tmp_single_country <- FALSE
                         output$SelectedMarketMultiple <-
                            renderText({paste0("Market group selected: ",input$select_country)})
                      }
                      
                      # multiple countries selected
                      if( !any(input$select_country %in% list_country[['Country groups']]) & length(input$select_country)>1 ){
                         tmp_selected_countries <- input$select_country
                         tmp_execution <- TRUE
                         tmp_single_country <- FALSE
                         output$SelectedMarketMultiple <-
                            renderText({paste0( length(which(tmp_selected_countries!='New Zealand')) , " markets selected")})
                      }
                      
                      ## only one country selected!
                      if(!any(input$select_country %in% list_country[['Country groups']]) &  length(input$select_country)==1 ) {
                         tmp_selected_countries <- input$select_country
                         tmp_execution <- TRUE
                         tmp_single_country <- TRUE
                         output$SelectedMarketSingle <-
                            renderText({paste0('Single market selected: ',tmp_selected_countries)})
                      }

                      ### work on next only when the inputs are correct!!!
                      if( tmp_execution ){
                         ## hide howto ----
                         shinyjs::hide(id = 'country_howto')
                         ## show wait message ----
                         shinyjs::show( id = 'wait_message_country_intel' )
                         ## disable a button -----
                         shinyjs::disable("btn_build_country_report")
                         ## disable a country selection button -----
                         shinyjs::disable("select_country")
                         
                         ## !!!!!!!!!!!!!!!! insert UI country name --------------------
                         insertUI(
                            selector = "#country_name",
                            ui = div(
                               id = 'country_name_single_or_multiple',
                               conditionalPanel( "input.select_country.length == 1 &&
                                                 input.select_country.valueOf() != 'APEC' &&
                                                 input.select_country.valueOf() != 'EU28' &&
                                                 input.select_country.valueOf() != 'CPTPP' &&
                                                 input.select_country.valueOf() != 'GCC' &&
                                                 input.select_country.valueOf() != 'Pacific Islands Forum' &&
                                                 input.select_country.valueOf() != 'ASEAN' &&
                                                 input.select_country.valueOf() != 'OECD' &&
                                                 input.select_country.valueOf() != 'Five Eyes' &&
                                                 input.select_country.valueOf() != 'Latin America' &&
                                                 input.select_country.valueOf() != 'OPEC' &&
                                                 input.select_country.valueOf() != 'FTA in force' &&
                                                 input.select_country.valueOf() != 'Middle East' && 
                                                 input.select_country.valueOf() != 'Northern Africa' && 
                                                 input.select_country.valueOf() != 'Eastern Africa' && 
                                                 input.select_country.valueOf() != 'Central Africa' && 
                                                 input.select_country.valueOf() != 'Southern Africa' && 
                                                 input.select_country.valueOf() != 'Western Africa' && 
                                                 input.select_country.valueOf() != 'Africa' && 
                                                 input.select_country.valueOf() != 'Arab Maghreb Union' &&
                                                 input.select_country.valueOf() != 'Eastern African Community' &&
                                                 input.select_country.valueOf() != 'Economic Community of West African States' &&
                                                 input.select_country.valueOf() != 'Southern African Development Community' &&
                                                 input.select_country.valueOf() != 'G7' &&
                                                 input.select_country.valueOf() != 'BRI countries' " ,
                                                 fluidRow( shiny::span(h1( HTML(paste0(textOutput("SelectedMarketSingle"))), align = "center" ), style = "color:darkblue" ) )
                               ),
                               
                               conditionalPanel( "input.select_country.length > 1 || 
                                              input.select_country.valueOf() == 'APEC' || 
                                              input.select_country.valueOf() == 'EU28'||
                                              input.select_country.valueOf() == 'CPTPP' ||
                                              input.select_country.valueOf() == 'GCC' ||
                                                 input.select_country.valueOf() == 'Pacific Islands Forum' ||
                                                 input.select_country.valueOf() == 'ASEAN' ||
                                                 input.select_country.valueOf() == 'OECD' ||
                                                 input.select_country.valueOf() == 'Five Eyes' ||
                                                 input.select_country.valueOf() == 'Latin America' ||
                                                 input.select_country.valueOf() == 'OPEC' ||
                                                 input.select_country.valueOf() == 'FTA in force' ||
                                                 input.select_country.valueOf() == 'Middle East' || 
                                                 input.select_country.valueOf() == 'Northern Africa' || 
                                                 input.select_country.valueOf() == 'Eastern Africa' || 
                                                 input.select_country.valueOf() == 'Central Africa' || 
                                                 input.select_country.valueOf() == 'Southern Africa' || 
                                                 input.select_country.valueOf() == 'Western Africa' || 
                                                 input.select_country.valueOf() == 'Africa' || 
                                                 input.select_country.valueOf() == 'Arab Maghreb Union' ||
                                                 input.select_country.valueOf() == 'Eastern African Community' ||
                                                 input.select_country.valueOf() == 'Economic Community of West African States' ||
                                                 input.select_country.valueOf() == 'Southern African Development Community' ||
                                                 input.select_country.valueOf() == 'G7' ||
                                                 input.select_country.valueOf() == 'BRI countries' ",
                                               fluidRow( shiny::span(h1( HTML(paste0(textOutput("SelectedMarketMultiple"))), align = "center" ), style = "color:darkblue" ) )

                               )
                            )
                         )
                         #shinyjs::hide( id = 'wait_message' )
                         ## III.1 Basic country info table ---------------------------------
                         # ### define the select country
                         print("------------------  Basic country tables -------------------------")
                         dtf_select_country <- 
                            data.frame(Country = tmp_selected_countries) %>% 
                            mutate( Country = as.character(tmp_selected_countries) )

                         dtf_select_country <-
                            dtf_country_group %>%
                            right_join( dtf_select_country, by = 'Country' ) %>%
                            dplyr::select( -Region ) %>%
                            left_join( flag_table ) %>%
                            mutate( Flag_img = paste0( "<img src='",
                                                       Flag_link,
                                                       "' height = '21' width = '42'>", "</img>") ) %>%
                            dplyr::select( Country, Flag = Flag_img, ISO2 )

                         #dtf_selected_country_map <- dtf_select_country[,c('Country','ISO2')]

                         ### get population -- if the country does not have any data
                         print("------------------  Basic country tables - get population -------------------------")
                         pop_download_fail <- try(
                             tmp_population <-
                                WDI(indicator='SP.POP.TOTL',
                                    country = dtf_select_country$ISO2,
                                    start=2014, end = max(dtf_shiny_full$Year))
                         )

                         if( class(pop_download_fail)=='try-error' ){
                            tmp_population <-
                               data.frame( iso2c = dtf_select_country$ISO2,
                                           country = dtf_select_country$Country,
                                           `SP.POP.TOTL` = NA,
                                           year = max(dtf_shiny_full$Year) ) %>%
                               dplyr::select( ISO2 = iso2c, `Population` =  'SP.POP.TOTL')
                         }else{
                            tmp_population <-
                               #WDI(indicator='SP.POP.TOTL',
                               #    country = dtf_select_country$ISO2,
                               #    start=2014, end = max(dtf_shiny_full$Year)) %>%
                               tmp_population %>%
                               filter(!is.na(`SP.POP.TOTL`) ) %>%
                               filter( year == max(year)) %>%
                               dplyr::select( ISO2 = iso2c, `Population` =  'SP.POP.TOTL') %>%
                               mutate( Population = Population/10^3 )
                         }


                         dtf_select_country %<>%
                            left_join(  tmp_population, by = 'ISO2' )

                         ### get gdp per capita and population data from Worldbank
                         print("------------------  Basic country tables - get GDP -------------------------")
                         gdp_download_fail <- try(
                             tmp_gdp_per_cap <-
                                WDI(indicator='NY.GDP.PCAP.CD',
                                    country = dtf_select_country$ISO2,
                                    start=2014, end = max(dtf_shiny_full$Year))
                         )

                         if( class(gdp_download_fail)=='try-error' ){
                            tmp_gdp_per_cap <-
                               data.frame( iso2c = dtf_select_country$ISO2,
                                           country = dtf_select_country$Country,
                                           `NY.GDP.PCAP.CD` = NA,
                                           year = max(dtf_shiny_full$Year) ) %>%
                               dplyr::select( ISO2 = iso2c, `GDP per capita` =  'NY.GDP.PCAP.CD')
                         }else{
                            tmp_gdp_per_cap <-
                               #WDI(indicator='SP.POP.TOTL',
                               #    country = dtf_select_country$ISO2,
                               #    start=2014, end = max(dtf_shiny_full$Year)) %>%
                               tmp_gdp_per_cap %>%
                               filter(!is.na(`NY.GDP.PCAP.CD`) ) %>%
                               filter( year == max(year)) %>%
                               dplyr::select( ISO2 = iso2c, `GDP per capita` =  'NY.GDP.PCAP.CD')
                         }


                         dtf_select_country %<>%
                            left_join(  tmp_gdp_per_cap, by = 'ISO2' )

                         ### get nearest distance to NZ
                         print("------------------  Basic country tables - get distance -------------------------")
                         dtf_select_country$`Distance to NZ` <- NA

                         for( i_country in 1:nrow(dtf_select_country) ){
                            print(dtf_select_country$Country[i_country])
                            if( dtf_select_country$Country[i_country] != "Destination Unknown - EU" ){
                               tmp_distance <- distm( concord_country[ concord_country$ISO2=='NZ' ,c('lon','lat')],
                                                      concord_country[ concord_country$ISO2 %in% dtf_select_country$ISO2[i_country]  ,c('lon','lat')])
                               tmp_distance <- round( as.numeric(tmp_distance)/1000, -2)
                               dtf_select_country$`Distance to NZ`[i_country] <- tmp_distance
                            }else{
                               dtf_select_country$`Distance to NZ`[i_country] <- 17880 #https://www.distancefromto.net/distance-from/New+Zealand/to/Europe
                            }
                         }

                         ## sort by population
                         dtf_select_country %<>%
                            ## World bank does not provide Taiwan data. we get data from 'https://eng.stat.gov.tw/ct.asp?xItem=41871&ctNode=2265&mp=5'
                            mutate( Population = ifelse(Country=='Taiwan',23540, Population ),
                                    `GDP per capita` = ifelse( Country=='Taiwan', 25119, `GDP per capita`) ) %>%
                            arrange( -Population )

                         ## generate map data
                         print("------------------  Basic country tables - build data for map -------------------------")
                         dtf_select_country_map <-
                            left_join(dtf_select_country,
                                      concord_country %>% dplyr::select(-Country),
                                      by = 'ISO2') %>%
                            mutate( z = 1, name = Country )

                         ### data for table
                         if( tmp_single_country ){
                            dtf_select_country %<>%
                               dplyr::select( -ISO2 )
                         }else{
                            dtf_select_country %<>%
                               dplyr::select( -ISO2 ) %>%
                               bind_rows( data.frame( Country='Total selected markets',
                                                      Flag = '',
                                                      Population = sum(dtf_select_country$Population, na.rm=T) ,
                                                      `GDP per capita` = sum( (dtf_select_country$Population/
                                                                                  sum(dtf_select_country$Population, na.rm=T)
                                                                               ) * dtf_select_country$`GDP per capita`, na.rm=T ),
                                                      `Distance to NZ` = mean( dtf_select_country$`Distance to NZ` , na.rm=T),
                                                      check.names = FALSE
                                                      )
                               )
                         }

                         ## render a country table
                         output$CountryTable <-
                            renderDataTable({
                               datatable( dtf_select_country,
                                          escape=FALSE,
                                          rownames = F,
                                          colnames=c("","", "Population<br>('000)" ,
                                                     "GDP per capita<br>(current US$)",
                                                     "Distance to NZ<br>(KM)"
                                          ),
                                          options = list(dom = 'ltp',
                                                         scrollX = TRUE, 
                                                         pageLength = 5,
                                                         lengthMenu = list(c(5,  -1), list('5', 'All'))  )
                               ) %>%
                                  formatCurrency( c("GDP per capita"), digits = 0, mark = ' ' ) %>%
                                  formatCurrency( c('Population'), digits = 0, mark = ' ', currency = '' ) %>%
                                  formatCurrency( c('Distance to NZ'), digits = 0, mark = ' ', currency = '' )
                            })


                         ## III.2 Map of selected countries -----------------
                         print("------------------  Country maps -------------------------")
                         output$MapSelectedCountry <-
                            renderHighchart({
                               base_selected_country_map <-
                                  hcmap( data = dtf_select_country_map ,#%>% mutate( Selected = 1 ),
                                         #value = 'Selected',
                                         value = 'z',
                                         joinBy = c('iso-a2','ISO2'),
                                         name="Selected market",
                                         borderWidth = 1,
                                         borderColor = "#fafafa",
                                         nullColor = "lightgrey" #,
                                         # tooltip = list( table = TRUE,
                                         #                 sort = TRUE,
                                         #                 headerFormat = '',
                                         #                 pointFormat = '{point.name}' ),
                                         # dataLabels = list(enabled=F)
                                  )%>%
                                  hc_add_series( data = dtf_select_country_map %>% dplyr::select(-name),
                                                 type = "mappoint",
                                                 color  = hex_to_rgba("#00ff00", 0.9),
                                                 marker = list( radius = 2 ),
                                                 dataLables = list(enabled=F),
                                                 #minSize = 0,
                                                 name="" #,#,
                                                 #maxSize = 4 #,
                                                 # tooltip = list(table = TRUE,
                                                 #               sort = TRUE,
                                                 #               headerFormat = '',
                                                 #               pointFormat = '{point.name}')
                                                 # dataLabels = list(enabled=T,
                                                 #                   format="{point.name}",
                                                 #                   style = list(fontSize = '10px', fontWeight = 'normal', color = 'white')
                                                 #)
                                  ) %>%
                                  hc_legend( enabled=FALSE ) %>%
                                  hc_tooltip( enabled = F) %>%
                                  hc_exporting(enabled = TRUE, formAttributes = list(target = "_blank")) %>%
                                  hc_mapNavigation(enabled = TRUE)

                               if( length(input$CountryTable_rows_selected)==0 ){
                                  base_selected_country_map
                               }else{
                                  update_selected_country_map <-
                                     base_selected_country_map %>%
                                     hc_add_series( data = dtf_select_country_map[input$CountryTable_rows_selected,],
                                                    type = "mappoint",
                                                    dataLabels = list(enabled=T,
                                                                      format="{point.name}",
                                                                      style = list(fontSize = '10px', fontWeight = 'normal', color = 'white')
                                                    ),
                                                    color  = hex_to_rgba("#FF0000", 0.9),
                                                    marker = list( radius = 3 ),
                                                    #minSize = 0,
                                                    name=""#,
                                                    #maxSize = 9
                                     )

                                  update_selected_country_map
                               }

                            })

                         ## !!!!!!!!!!!!! insert UI --------------------
                         insertUI(
                            selector = "#country_info",
                            ui = div(id = 'country_info_table_map',
                                     fluidRow( h1("Background market information"),
                                               column(6, dataTableOutput('CountryTable') %>% withSpinner(type=4),
                                                      HTML("<footer>Note: Population and GDP per capital are the latest data from the World Bank. Distance to New Zealand is the nearest distance between two territories' centre points. </footer>")
                                               ),
                                               column(6, highchartOutput("MapSelectedCountry") )
                                     )
                            )
                         )
                         ## III.3 Trade summary table for selected markets ALL CountryTradeTableTotal -----------------------
                         print("------------------  Trade summary tables -------------------------")
                         tmp_tab_all_country <- sum_selected_country( tmp_selected_countries )
                         
                         ## for some countries, there are only 5 years of service data 
                         if( is.na(tmp_tab_all_country$CAGR10[tmp_tab_all_country$Name == 'Services exports']) ){
                            tmp_tab_all_country$CAGR10[tmp_tab_all_country$Name == 'Total exports'] <- NA
                            tmp_tab_all_country$CAGR10[tmp_tab_all_country$Name == 'Two-way trade'] <- NA
                         }
                         if( is.na(tmp_tab_all_country$CAGR10[tmp_tab_all_country$Name == 'Services imports']) ){
                            tmp_tab_all_country$CAGR10[tmp_tab_all_country$Name == 'Total imports'] <- NA
                         }
                         
                         ## plot
                         output$CountryTradeTableTotal <- renderDataTable({
                            datatable( tmp_tab_all_country,
                                       rownames = F,
                                       extensions = 'Buttons',
                                       options = list(dom = 'Bt', 
                                                      scrollX = TRUE ,
                                                      buttons = c('copy', 'csv', 'excel', 'pdf', 'print') ) ,
                                       colnames=c("","Value ($m)", 'Share of world market','CAGR 1', 'CAGR 5', 'CAGR 10')
                            ) %>%
                               formatStyle(columns = 'Name',
                                           target = 'row',
                                           fontWeight = styleEqual(c('Total imports','Total exports','Two-way trade', 'Trade balance'),
                                                                   c('bold','bold','bold', 'bold')),
                                           backgroundColor = styleEqual(c('Total imports','Total exports'),
                                                                        c('lightgrey','lightgrey'))
                               ) %>%
                               formatStyle(
                                  c('CAGR1', 'CAGR5', 'CAGR10'),
                                  background = styleColorBar( c(0,max(tmp_tab_all_country[,c('CAGR1','CAGR5','CAGR10')],na.rm=T)*2) , 'lightblue'),
                                  backgroundSize = '100% 90%',
                                  backgroundRepeat = 'no-repeat',
                                  backgroundPosition = 'center',
                                  color = JS("value < 0 ? 'darkred' : value > 0 ? 'darkgreen' : 'black'")
                               ) %>%
                               formatPercentage( c('CAGR1', 'CAGR5', 'CAGR10', 'Share'),digit = 1 ) %>%
                               formatStyle( columns = c('Name','CAGR1', 'CAGR5', 'CAGR10', 'Share', 'Value'), `font-size`= '115%' ) %>%
                               formatCurrency( columns = c('Value'), digits = 0 )
                         })
                         
                         ## III 3.1 Investment position summary table --------------------------------------
                         print("------------------  Investment summary tables -------------------------")
                         tmp_tab_all_country_investment <-
                            sum_selected_country_investment( tmp_selected_countries )
                         
                         output$CountryInvestmentTableTotal <- renderDataTable({
                            datatable( tmp_tab_all_country_investment,
                                       rownames = F,
                                       extensions = 'Buttons',
                                       options = list(dom = 'Bt', 
                                                      scrollX = TRUE,
                                                      buttons = c('copy', 'csv', 'excel', 'pdf', 'print') ) ,
                                       colnames=c("","Value ($m)", 'Share of world market','CAGR 1', 'CAGR 5', 'CAGR 10')
                            ) %>%
                               formatStyle(columns = 'Name',
                                           #target = 'row',
                                           fontWeight = styleEqual(c('Foreign direct investment',
                                                                     'Overseas direct investment',
                                                                     'Two-way direct investment'),
                                                                   c('bold','bold','bold')
                                                                   ) #,
                                           #backgroundColor = styleEqual(c('Foreign direct investment',
                                           #                               'Overseas direct investment'),
                                           #                             c('lightgrey','lightgrey'))
                               ) %>%
                               formatStyle(
                                  c('CAGR1', 'CAGR5', 'CAGR10'),
                                  background = styleColorBar( c(0,max(tmp_tab_all_country[,c('CAGR1','CAGR5','CAGR10')],na.rm=T)*2) , 'lightblue'),
                                  backgroundSize = '100% 90%',
                                  backgroundRepeat = 'no-repeat',
                                  backgroundPosition = 'center',
                                  color = JS("value < 0 ? 'darkred' : value > 0 ? 'darkgreen' : 'black'")
                               ) %>%
                               formatPercentage( c('CAGR1', 'CAGR5', 'CAGR10', 'Share'),digit = 1 ) %>%
                               formatStyle( columns = c('Name','CAGR1', 'CAGR5', 'CAGR10', 'Share', 'Value'), `font-size`= '115%' ) %>%
                               formatCurrency( columns = c('Value'), digits = 0 )
                         })

                         ## III 3.2 People movement summary table ---------------------------
                         print("------------------  People movement summary tables -------------------------")
                         tmp_tab_all_country_pplmove <-
                            sum_selected_country_pplmove( tmp_selected_countries )
                         
                         output$CountryPplMovementTableTotal <- renderDataTable({
                            datatable( tmp_tab_all_country_pplmove,
                                       rownames = F,
                                       extensions = 'Buttons',
                                       options = list(dom = 'Bt', 
                                                      scrollX = TRUE,
                                                      buttons = c('copy', 'csv', 'excel', 'pdf', 'print') ) ,
                                       colnames=c("","Value ('000)", 'Share of world market','CAGR 1', 'CAGR 5', 'CAGR 10')
                            ) %>%
                               formatStyle(columns = 'Name',
                                           #target = 'row',
                                           fontWeight = styleEqual(c('Foreign visitors travelling in',
                                                                     'NZ visitors travelling out',
                                                                     'Two-way visitor movement'),
                                                                   c('bold','bold','bold')
                                           ) #,
                                           #backgroundColor = styleEqual(c('Foreign direct investment',
                                           #                               'Overseas direct investment'),
                                           #                             c('lightgrey','lightgrey'))
                               ) %>%
                               formatStyle(
                                  c('CAGR1', 'CAGR5', 'CAGR10'),
                                  background = styleColorBar( c(0,max(tmp_tab_all_country_pplmove[,c('CAGR1','CAGR5','CAGR10')],na.rm=T)*2) , 'lightblue'),
                                  backgroundSize = '100% 90%',
                                  backgroundRepeat = 'no-repeat',
                                  backgroundPosition = 'center',
                                  color = JS("value < 0 ? 'darkred' : value > 0 ? 'darkgreen' : 'black'")
                               ) %>%
                               formatPercentage( c('CAGR1', 'CAGR5', 'CAGR10', 'Share'),digit = 1 ) %>%
                               formatStyle( columns = c('Name','CAGR1', 'CAGR5', 'CAGR10', 'Share', 'Value'), `font-size`= '115%' ) %>%
                               formatCurrency( columns = c('Value'), digits = 0, currency = '' )
                         })
                         
                         ## III.4 Line graph two-way trade CountryTwowayTradeGraphTotal ----------------------
                         print("------------------  Line graphs -------------------------")
                         tmp_dtf_twoway_line <-
                            dtf_shiny_country_gs %>%
                            filter( Year >=2007, Country %in% tmp_selected_countries ) %>%
                            group_by( Year ) %>%
                            do( Value = sum( .$Value, na.rm=T ) ) %>%
                            ungroup %>%
                            mutate( Value = as.numeric(Value)/10^6 ) %>%
                            mutate( Name = 'Two-way trade' ) %>%
                            mutate( Country = 'The selected markets' )

                         output$CountryTwowayTradeGraphTotal <- renderHighchart({
                            highchart() %>%
                               hc_exporting(enabled = TRUE, formAttributes = list(target = "_blank")) %>%
                               hc_xAxis( categories = c( unique( tmp_dtf_twoway_line$Year) ) ) %>%
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
                                                                " {series.name}: ${point.y:,.0f} m"),
                                          headerFormat = '<span style="font-size: 13px">Year {point.key}</span>'
                               ) %>%
                               hc_legend( layout = 'vertical', align = 'left', verticalAlign = 'top', floating = T, x = 100, y = -15 ) %>%
                               hc_add_series( data =  tmp_dtf_twoway_line  ,
                                              mapping = hcaes(  x = Year, y = Value, group = Name ),
                                              type = 'line',
                                              marker = list(symbol = 'circle') #,
                                              #visible = c(T,rep(F,length(tmp_top_g_ex)-1))
                               )
                         })
                         ## III.5 Line graph trade balance CountryTradeBalanceGraphTotal ----------------------
                         tmp_dtf_balance_line <-
                            dtf_shiny_country_gs %>%
                            filter( Year >=2007, Country %in% tmp_selected_countries ) %>%
                            group_by( Year, Type_ie ) %>%
                            do( Value = sum( .$Value, na.rm=T ) ) %>%
                            ungroup %>%
                            mutate( Value = as.numeric(Value)/10^6 ) %>%
                            group_by( Year ) %>%
                            do( Value = .$Value[.$Type_ie=='Exports'] - .$Value[.$Type_ie=='Imports']) %>%
                            ungroup %>%
                            mutate( Value = round(as.numeric(Value)) ) %>%
                            mutate( Name = 'Trade balance' ) %>%
                            bind_rows(dtf_shiny_country_gs %>%
                                         filter( Type_gs == 'Goods') %>%
                                         filter( Year >=2007, Country %in% tmp_selected_countries ) %>%
                                         group_by( Year, Type_ie ) %>%
                                         do( Value = sum( .$Value, na.rm=T ) ) %>%
                                         ungroup %>%
                                         mutate( Value = as.numeric(Value)/10^6 ) %>%
                                         group_by( Year ) %>%
                                         do( Value = .$Value[.$Type_ie=='Exports'] - .$Value[.$Type_ie=='Imports']) %>%
                                         ungroup %>%
                                         mutate( Value = round(as.numeric(Value)) ) %>%
                                         mutate( Name = 'Goods balance' )
                                      ) %>%
                            bind_rows(dtf_shiny_country_gs %>%
                                         filter( Type_gs == 'Services') %>%
                                         filter( Year >=2007, Country %in% tmp_selected_countries ) %>%
                                         group_by( Year, Type_ie ) %>%
                                         do( Value = sum( .$Value, na.rm=T ) ) %>%
                                         ungroup %>%
                                         mutate( Value = as.numeric(Value)/10^6 ) %>%
                                         group_by( Year ) %>%
                                         do( Value = .$Value[.$Type_ie=='Exports'] - .$Value[.$Type_ie=='Imports']) %>%
                                         ungroup %>%
                                         mutate( Value = round(as.numeric(Value)) ) %>%
                                         mutate( Name = 'Services balance' )
                                      ) %>%
                            mutate( Country = 'The selected markets' )
                         
                         
                         ## check if service data full, if not make it full
                         tmp_year_balance_line_g <- tmp_dtf_balance_line$Year[tmp_dtf_balance_line$Name == 'Goods balance']
                         tmp_year_balance_line_s <- tmp_dtf_balance_line$Year[tmp_dtf_balance_line$Name == 'Services balance']
                         
                         if( any(tmp_year_balance_line_s != 
                                 tmp_year_balance_line_g) ){
                            ## year missing
                            tmp_year_balance_line_missing <- setdiff( tmp_year_balance_line_g, tmp_year_balance_line_s )
                            
                            ## reconstruct the dataset
                            tmp_dtf_balance_line %<>%
                               bind_rows( data.frame( Year = tmp_year_balance_line_missing,
                                                      Value = NA,
                                                      Name = "Services balance", 
                                                      Country = "The selected markets") ) %>%
                               group_by( Name, Country ) %>%
                               arrange( Year ) %>%
                               ungroup
                         }

                         # output$CountryTradeBalanceGraphTotal <-
                         #    renderHighchart({
                         #       highchart() %>%
                         #          hc_exporting(enabled = TRUE, formAttributes = list(target = "_blank")) %>%
                         #          hc_xAxis( categories = c( unique( tmp_dtf_balance_line$Year) ) ) %>%
                         #          hc_yAxis( title = list(text = "$ million, NZD"),
                         #                    labels = list( format = "${value:,.0f} m")
                         #          ) %>%
                         #          hc_plotOptions(line = list(
                         #             dataLabels = list(enabled = F),
                         #             #stacking = "normal",
                         #             enableMouseTracking = T #,
                         #             #series = list(events = list(legendItemClick = sharelegend)) ,
                         #             #showInLegend = T
                         #          ) )%>%
                         #          hc_tooltip(table = TRUE,
                         #                     sort = TRUE,
                         #                     pointFormat = paste0( '<br> <span style="color:{point.color}">\u25CF</span>',
                         #                                           " {series.name}: ${point.y:,.0f} m"),
                         #                     headerFormat = '<span style="font-size: 13px">Year {point.key}</span>'
                         #          ) %>%
                         #          hc_legend( layout = 'vertical', align = 'left', verticalAlign = 'top', floating = T, x = 100, y = -15 ) %>%
                         #          hc_add_series( data =  tmp_dtf_balance_line  ,
                         #                         mapping = hcaes(  x = Year, y = Value, group = Name ),
                         #                         type = 'line',
                         #                         marker = list(symbol = 'circle') #,
                         #                         #visible = c(T,rep(F,length(tmp_top_g_ex)-1))
                         #          )
                         #    })
                         
                         output$CountryTradeBalanceGraphTotal <-
                            renderHighchart({
                               highchart() %>%
                                  hc_exporting(enabled = TRUE, formAttributes = list(target = "_blank")) %>%
                                  hc_chart(type = 'line') %>%
                                  hc_series( list(name = 'Trade balance', data =tmp_dtf_balance_line$Value[tmp_dtf_balance_line$Name =='Trade balance'], color='brown' , marker = list(enabled = F), lineWidth = 3 ),
                                             list(name = 'Goods balance', data =tmp_dtf_balance_line$Value[tmp_dtf_balance_line$Name =='Goods balance'], color = 'darkgreen', dashStyle = 'shortDot', marker = list(symbol = 'circle') ),
                                             list(name = 'Services balance', data =tmp_dtf_balance_line$Value[tmp_dtf_balance_line$Name =='Services balance'], color = 'darkblue', dashStyle = 'shortDot',  marker = list(symbol = 'triangle') )
                                  )%>%
                                  hc_xAxis( categories = unique(tmp_dtf_balance_line$Year) ) %>%
                                  hc_yAxis( title = list(text = "$ million, NZD"),
                                            labels = list( format = "${value:,.0f} m"),
                                            plotLines = list(
                                               list(#label = list(text = "This is a plotLine"),
                                                  color = "#ff0000",
                                                  #dashStyle = 'shortDot',
                                                  width = 2,
                                                  value = 0 ) )
                                  ) %>%
                                  hc_plotOptions(column = list(
                                     dataLabels = list(enabled = F),
                                     #stacking = "normal",
                                     enableMouseTracking = T ) 
                                  )%>%
                                  hc_tooltip(table = TRUE,
                                             sort = TRUE,
                                             pointFormat = paste0( '<br> <span style="color:{point.color}">\u25CF</span>',
                                                                   " {series.name}: ${point.y} m"),
                                             headerFormat = '<span style="font-size: 13px">Year {point.key}</span>'
                                  ) %>%
                                  hc_legend( layout = 'vertical', align = 'left', verticalAlign = 'top', floating = T, x = 100, y = 000 )
                            })
                         
                         
                         ## III.6 Line graph Exports CountryExportsGraphTotal ----------------------
                         tmp_dtf_ex_line <-
                            dtf_shiny_country_gs %>%
                            filter( Year >=2007, Country %in% tmp_selected_countries,
                                    Type_ie == 'Exports') %>%
                            group_by( Year, Type_gs ) %>%
                            do( Value = sum( .$Value, na.rm=T ) ) %>%
                            ungroup %>%
                            mutate( Value = as.numeric(Value)/10^6 ) %>%
                            mutate( Name = paste0(Type_gs,' exports') ) %>%
                            dplyr::select( -Type_gs )

                         tmp_dtf_ex_line %<>%
                            bind_rows( tmp_dtf_ex_line %>%
                                          group_by( Year ) %>%
                                          do( Value = sum(.$Value, na.rm=T) ) %>%
                                          ungroup %>%
                                          mutate( Value = as.numeric(Value) ) %>%
                                          mutate(Name = 'Total exports')
                            ) %>%
                            mutate( Country = 'The selected markets' ) %>%
                            mutate( Name = factor(Name, levels = c('Total exports','Goods exports','Services exports')) )

                         output$CountryExportsGraphTotal <-
                            renderHighchart({
                               highchart() %>%
                                  hc_exporting(enabled = TRUE, formAttributes = list(target = "_blank")) %>%
                                  hc_xAxis( categories = c( unique( tmp_dtf_ex_line$Year) ) ) %>%
                                  hc_yAxis( title = list(text = "$ million, NZD"),
                                            labels = list( format = "${value:,.0f} m")
                                  ) %>%
                                  hc_plotOptions(line = list(
                                     dataLabels = list(enabled = F),
                                     #stacking = "normal",
                                     enableMouseTracking = T #,
                                     #series = list(events = list(legendItemClick = sharelegend)) ,
                                     #showInLegend = T
                                  ) )%>%
                                  hc_tooltip(table = TRUE,
                                             sort = TRUE,
                                             pointFormat = paste0( '<br> <span style="color:{point.color}">\u25CF</span>',
                                                                   " {series.name}: ${point.y:,.0f} m"),
                                             headerFormat = '<span style="font-size: 13px">Year {point.key}</span>'
                                  ) %>%
                                  hc_legend( layout = 'vertical', align = 'left', verticalAlign = 'top', floating = T, x = 100, y = -15 ) %>%
                                  hc_add_series( data =  tmp_dtf_ex_line  ,
                                                 mapping = hcaes(  x = Year, y = Value, group = Name ),
                                                 type = 'line',
                                                 marker = list(symbol = 'circle') #,
                                                 #visible = c(T,rep(F,length(tmp_top_g_ex)-1))
                                  )
                            })
                         ## III.7 Line graph Exports CountryExportsGraphTotalPercent ----------------------
                         tmp_dtf_ex_line_world <-
                            dtf_shiny_country_gs %>%
                            filter( Year >=2007, Country %in% 'World',
                                    Type_ie == 'Exports') %>%
                            group_by( Year, Type_gs ) %>%
                            do( Value = sum( .$Value, na.rm=T ) ) %>%
                            ungroup %>%
                            mutate( Value = as.numeric(Value)/10^6 ) %>%
                            mutate( Name = paste0(Type_gs,' exports') ) %>%
                            dplyr::select( -Type_gs )

                         tmp_dtf_ex_line_world %<>%
                            bind_rows( tmp_dtf_ex_line_world %>%
                                          group_by( Year ) %>%
                                          do( Value = sum(.$Value, na.rm=T) ) %>%
                                          ungroup %>%
                                          mutate( Value = as.numeric(Value) ) %>%
                                          mutate(Name = 'Total exports')
                            ) %>%
                            mutate( Country = 'World' ) %>%
                            mutate( Name = factor(Name, levels = c('Total exports','Goods exports','Services exports')) )

                         tmp_dtf_ex_line_percent <-
                            tmp_dtf_ex_line %>%
                            bind_rows( tmp_dtf_ex_line_world ) %>%
                            group_by( Year, Name ) %>%
                            do( Share = .$Value/.$Value[.$Country=='World'] ) %>%
                            ungroup %>%
                            rowwise %>%
                            mutate( Value = ifelse( length(unlist(Share))==2, unlist(Share)[1]*100, NA) )

                         output$CountryExportsGraphTotalPercent <-
                            renderHighchart({
                               highchart() %>%
                                  hc_exporting(enabled = TRUE, formAttributes = list(target = "_blank")) %>%
                                  hc_xAxis( categories = c( unique( tmp_dtf_ex_line_percent$Year) ) ) %>%
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
                                  hc_add_series( data =  tmp_dtf_ex_line_percent  ,
                                                 mapping = hcaes(  x = Year, y = Value, group = Name ),
                                                 type = 'line',
                                                 marker = list(symbol = 'circle') #,
                                                 #visible = c(T,rep(F,length(tmp_top_g_ex)-1))
                                  )
                            })

                         ## III.8 Line graph Imports CountryImportsGraphTotal ----------------------
                         tmp_dtf_im_line <-
                            dtf_shiny_country_gs %>%
                            filter( Year >=2007, Country %in% tmp_selected_countries,
                                    Type_ie == 'Imports') %>%
                            group_by( Year, Type_gs ) %>%
                            do( Value = sum( .$Value, na.rm=T ) ) %>%
                            ungroup %>%
                            mutate( Value = as.numeric(Value)/10^6 ) %>%
                            mutate( Name = paste0(Type_gs,' imports') ) %>%
                            dplyr::select( -Type_gs )

                         tmp_dtf_im_line %<>%
                            bind_rows( tmp_dtf_im_line %>%
                                          group_by( Year ) %>%
                                          do( Value = sum(.$Value, na.rm=T) ) %>%
                                          ungroup %>%
                                          mutate( Value = as.numeric(Value) ) %>%
                                          mutate(Name = 'Total imports')
                            ) %>%
                            mutate( Country = 'The selected markets' ) %>%
                            mutate( Name = factor(Name, levels = c('Total imports','Goods imports','Services imports')) )

                         output$CountryImportsGraphTotal <-
                            renderHighchart({
                               highchart() %>%
                                  hc_exporting(enabled = TRUE, formAttributes = list(target = "_blank")) %>%
                                  hc_xAxis( categories = c( unique( tmp_dtf_im_line$Year) ) ) %>%
                                  hc_yAxis( title = list(text = "$ million, NZD"),
                                            labels = list( format = "${value:,.0f} m")
                                  ) %>%
                                  hc_plotOptions(line = list(
                                     dataLabels = list(enabled = F),
                                     #stacking = "normal",
                                     enableMouseTracking = T #,
                                     #series = list(events = list(legendItemClick = sharelegend)) ,
                                     #showInLegend = T
                                  ) )%>%
                                  hc_tooltip(table = TRUE,
                                             sort = TRUE,
                                             pointFormat = paste0( '<br> <span style="color:{point.color}">\u25CF</span>',
                                                                   " {series.name}: ${point.y:,.0f} m"),
                                             headerFormat = '<span style="font-size: 13px">Year {point.key}</span>'
                                  ) %>%
                                  hc_legend( layout = 'vertical', align = 'left', verticalAlign = 'top', floating = T, x = 100, y = -15 ) %>%
                                  hc_add_series( data =  tmp_dtf_im_line  ,
                                                 mapping = hcaes(  x = Year, y = Value, group = Name ),
                                                 type = 'line',
                                                 marker = list(symbol = 'circle') #,
                                                 #visible = c(T,rep(F,length(tmp_top_g_ex)-1))
                                  )
                            })
                         ## III.9 Line graph Imports CountryImportsGraphTotalPercent ----------------------
                         tmp_dtf_im_line_world <-
                            dtf_shiny_country_gs %>%
                            filter( Year >=2007, Country %in% 'World',
                                    Type_ie == 'Imports') %>%
                            group_by( Year, Type_gs ) %>%
                            do( Value = sum( .$Value, na.rm=T ) ) %>%
                            ungroup %>%
                            mutate( Value = as.numeric(Value)/10^6 ) %>%
                            mutate( Name = paste0(Type_gs,' imports') ) %>%
                            dplyr::select( -Type_gs )

                         tmp_dtf_im_line_world %<>%
                            bind_rows( tmp_dtf_im_line_world %>%
                                          group_by( Year ) %>%
                                          do( Value = sum(.$Value, na.rm=T) ) %>%
                                          ungroup %>%
                                          mutate( Value = as.numeric(Value) ) %>%
                                          mutate(Name = 'Total imports')
                            ) %>%
                            mutate( Country = 'World' ) %>%
                            mutate( Name = factor(Name, levels = c('Total imports','Goods imports','Services imports')) )

                         tmp_dtf_im_line_percent <-
                            tmp_dtf_im_line %>%
                            bind_rows( tmp_dtf_im_line_world ) %>%
                            group_by( Year, Name ) %>%
                            do( Share = .$Value/.$Value[.$Country=='World'] ) %>%
                            ungroup %>%
                            rowwise %>%
                            mutate( Value = ifelse( length(unlist(Share))==2, unlist(Share)[1]*100, NA) )

                         output$CountryImportsGraphTotalPercent <-
                            renderHighchart({
                               highchart() %>%
                                  hc_exporting(enabled = TRUE, formAttributes = list(target = "_blank")) %>%
                                  hc_xAxis( categories = c( unique( tmp_dtf_im_line_percent$Year) ) ) %>%
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
                                  hc_add_series( data =  tmp_dtf_im_line_percent  ,
                                                 mapping = hcaes(  x = Year, y = Value, group = Name ),
                                                 type = 'line',
                                                 marker = list(symbol = 'circle') #,
                                                 #visible = c(T,rep(F,length(tmp_top_g_ex)-1))
                                  )
                            })
                         ## III.9.1 Line graph Investment position CountryInvestmentGraphTotal ----------------------
                         tmp_dtf_invest_line <-
                            dtf_fdi_odi %>%
                            filter( Year >=2007, 
                                    Country %in% tmp_selected_countries 
                                    ) %>%
                            group_by( Year, Type ) %>%
                            do( Value = sum( .$Value, na.rm=T ) ) %>%
                            ungroup %>%
                            mutate( Value = as.numeric(Value) ) %>%
                            mutate( Name =  ifelse(Type=="FDI", 
                                                   'Foreign direct investment',
                                                   "Overseas direct investment") ) %>%
                            dplyr::select( -Type )
                         
                         tmp_dtf_invest_line %<>%
                            bind_rows( tmp_dtf_invest_line %>%
                                          group_by( Year ) %>%
                                          do( Value = sum(.$Value, na.rm=T) ) %>%
                                          ungroup %>%
                                          mutate( Value = as.numeric(Value) ) %>%
                                          mutate(Name = 'Two-way direct investment')
                            ) %>%
                            mutate( Country = 'The selected markets' ) %>%
                            mutate( Name = factor(Name, levels = c('Two-way direct investment',
                                                                   'Foreign direct investment',
                                                                   'Overseas direct investment')) 
                                    )
                         
                         output$CountryInvestmentGraphTotal <-
                            renderHighchart({
                               highchart() %>%
                                  hc_exporting(enabled = TRUE, formAttributes = list(target = "_blank")) %>%
                                  hc_xAxis( categories = c( unique( tmp_dtf_invest_line$Year) ) ) %>%
                                  hc_yAxis( title = list(text = "$ million, NZD"),
                                            labels = list( format = "${value:,.0f} m")
                                  ) %>%
                                  hc_plotOptions(line = list(
                                     dataLabels = list(enabled = F),
                                     #stacking = "normal",
                                     enableMouseTracking = T #,
                                     #series = list(events = list(legendItemClick = sharelegend)) ,
                                     #showInLegend = T
                                  ) )%>%
                                  hc_tooltip(table = TRUE,
                                             sort = TRUE,
                                             pointFormat = paste0( '<br> <span style="color:{point.color}">\u25CF</span>',
                                                                   " {series.name}: ${point.y:,.0f} m"),
                                             headerFormat = '<span style="font-size: 13px">Year {point.key}</span>'
                                  ) %>%
                                  hc_legend( layout = 'vertical', align = 'left', verticalAlign = 'top', floating = T, x = 100, y = -15 ) %>%
                                  hc_add_series( data =  tmp_dtf_invest_line  ,
                                                 mapping = hcaes(  x = Year, y = Value, group = Name ),
                                                 type = 'line',
                                                 marker = list(symbol = 'circle') #,
                                                 #visible = c(T,rep(F,length(tmp_top_g_ex)-1))
                                  )
                            })
                         
                         ## III.9.2 Line graph Investment CountryInvestmentGraphTotalPercent ----------------------
                         tmp_dtf_invest_line_world <-
                            dtf_fdi_odi %>%
                            filter( Year >=2007, 
                                    Country %in% 'World' #,
                                    #Type_ie == 'Imports' 
                                    ) %>%
                            group_by( Year, Type ) %>%
                            do( Value = sum( .$Value, na.rm=T ) ) %>%
                            ungroup %>%
                            mutate( Value = as.numeric(Value) ) %>%
                            mutate( Name =  ifelse(Type=="FDI", 
                                                   'Foreign direct investment',
                                                   "Overseas direct investment") ) %>%
                            dplyr::select( -Type )
                         
                         tmp_dtf_invest_line_world %<>%
                            bind_rows( tmp_dtf_invest_line_world %>%
                                          group_by( Year ) %>%
                                          do( Value = sum(.$Value, na.rm=T) ) %>%
                                          ungroup %>%
                                          mutate( Value = as.numeric(Value) ) %>%
                                          mutate(Name = 'Two-way direct investment')
                            ) %>%
                            mutate( Country = 'World' ) %>%
                            mutate( Name = factor(Name, levels = c('Two-way direct investment',
                                                                   'Foreign direct investment',
                                                                   'Overseas direct investment') ) )
                         
                         tmp_dtf_invest_line_percent <-
                            tmp_dtf_invest_line %>%
                            bind_rows( tmp_dtf_invest_line_world ) %>%
                            group_by( Year, Name ) %>%
                            do( Share = .$Value/.$Value[.$Country=='World'] ) %>%
                            ungroup %>%
                            rowwise %>%
                            mutate( Value = ifelse( length(unlist(Share))==2, unlist(Share)[1]*100, NA) )
                         
                         output$CountryInvestmentGraphTotalPercent <-
                            renderHighchart({
                               highchart() %>%
                                  hc_exporting(enabled = TRUE, formAttributes = list(target = "_blank")) %>%
                                  hc_xAxis( categories = c( unique( tmp_dtf_invest_line_percent$Year) ) ) %>%
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
                                  hc_add_series( data =  tmp_dtf_invest_line_percent  ,
                                                 mapping = hcaes(  x = Year, y = Value, group = Name ),
                                                 type = 'line',
                                                 marker = list(symbol = 'circle') #,
                                                 #visible = c(T,rep(F,length(tmp_top_g_ex)-1))
                                  )
                            })
                         
                         ## III.9.3 Line graph Ppl Movement CountryPplMovementGraphTotal ----------------------
                         tmp_dtf_pplmove_line <-
                            dtf_in_out %>%
                            filter( Year >=2007, 
                                    Country %in% tmp_selected_countries 
                            ) %>%
                            group_by( Year, Type ) %>%
                            do( Value = sum( .$Value, na.rm=T ) ) %>%
                            ungroup %>%
                            mutate( Value = as.numeric(Value)/10^3 ) %>%
                            mutate( Name =  Type ) %>%
                            dplyr::select( -Type )
                         
                         tmp_dtf_pplmove_line %<>%
                            bind_rows( tmp_dtf_pplmove_line %>%
                                          group_by( Year ) %>%
                                          do( Value = sum(.$Value, na.rm=T) ) %>%
                                          ungroup %>%
                                          mutate( Value = as.numeric(Value) ) %>%
                                          mutate(Name = 'Two-way visitor movement')
                            ) %>%
                            mutate( Country = 'The selected markets' ) %>%
                            mutate( Name = factor(Name, levels = c('Two-way visitor movement',
                                                                   'Foreign visitors travelling in',
                                                                   'NZ visitors travelling out')) 
                            )
                         
                         output$CountryPplMovementGraphTotal <-
                            renderHighchart({
                               highchart() %>%
                                  hc_exporting(enabled = TRUE, formAttributes = list(target = "_blank")) %>%
                                  hc_xAxis( categories = c( unique( tmp_dtf_pplmove_line$Year) ) ) %>%
                                  hc_yAxis( title = list(text = "Number of visitors, '000"),
                                            labels = list( format = "{value:,.0f}")
                                  ) %>%
                                  hc_plotOptions(line = list(
                                     dataLabels = list(enabled = F),
                                     #stacking = "normal",
                                     enableMouseTracking = T #,
                                     #series = list(events = list(legendItemClick = sharelegend)) ,
                                     #showInLegend = T
                                  ) )%>%
                                  hc_tooltip(table = TRUE,
                                             sort = TRUE,
                                             pointFormat = paste0( '<br> <span style="color:{point.color}">\u25CF</span>',
                                                                   " {series.name}: {point.y:,.0f} 000"),
                                             headerFormat = '<span style="font-size: 13px">Year {point.key}</span>'
                                  ) %>%
                                  hc_legend( layout = 'vertical', align = 'left', verticalAlign = 'top', floating = T, x = 100, y = -15 ) %>%
                                  hc_add_series( data =  tmp_dtf_pplmove_line ,
                                                 mapping = hcaes(  x = Year, y = Value, group = Name ),
                                                 type = 'line',
                                                 marker = list(symbol = 'circle') #,
                                                 #visible = c(T,rep(F,length(tmp_top_g_ex)-1))
                                  )
                            })
                         
                         ## III.9.4 Line graph ppl movement CountryPplMovementGraphTotalPercent ----------------------
                         tmp_dtf_pplmove_line_world <-
                            dtf_in_out %>%
                            filter( Year >=2007, 
                                    Country %in% 'World'
                            ) %>%
                            group_by( Year, Type ) %>%
                            do( Value = sum( .$Value, na.rm=T ) ) %>%
                            ungroup %>%
                            mutate( Value = as.numeric(Value)/10^3 ) %>%
                            mutate( Name =  Type ) %>%
                            dplyr::select( -Type )
                         
                         tmp_dtf_pplmove_line_world %<>%
                            bind_rows( tmp_dtf_pplmove_line_world %>%
                                          group_by( Year ) %>%
                                          do( Value = sum(.$Value, na.rm=T) ) %>%
                                          ungroup %>%
                                          mutate( Value = as.numeric(Value) ) %>%
                                          mutate(Name = 'Two-way visitor movement')
                            ) %>%
                            mutate( Country = 'World' ) %>%
                            mutate( Name = factor(Name, levels = c('Two-way visitor movement',
                                                                   'Foreign visitors travelling in',
                                                                   'NZ visitors travelling out') ) )
                         
                         tmp_dtf_pplmove_line_percent <-
                            tmp_dtf_pplmove_line %>%
                            bind_rows( tmp_dtf_pplmove_line_world ) %>%
                            group_by( Year, Name ) %>%
                            do( Share = .$Value/.$Value[.$Country=='World'] ) %>%
                            ungroup %>%
                            rowwise %>%
                            mutate( Value = ifelse( length(unlist(Share))==2, unlist(Share)[1]*100, NA) )
                         
                         output$CountryPplMovementGraphTotalPercent <-
                            renderHighchart({
                               highchart() %>%
                                  hc_exporting(enabled = TRUE, formAttributes = list(target = "_blank")) %>%
                                  hc_xAxis( categories = c( unique( tmp_dtf_pplmove_line_percent$Year) ) ) %>%
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
                                  hc_add_series( data =  tmp_dtf_pplmove_line_percent  ,
                                                 mapping = hcaes(  x = Year, y = Value, group = Name ),
                                                 type = 'line',
                                                 marker = list(symbol = 'circle') #,
                                                 #visible = c(T,rep(F,length(tmp_top_g_ex)-1))
                                  )
                            })
                         
                         ## III.10 Treemap Key export commodities KeyExCountryTotalTreeMap -------------------
                         print("------------------  Tree maps -------------------------")
                         tmp_dtf_ex_country <-
                            get_snz_gs_country("Exports",tmp_selected_countries) %>%
                            mutate( Value = Value/10^6)
                         
                         fail_tm_ex_country <- try(
                            tmp_tm_ex_country <-
                               treemap( tmp_dtf_ex_country  %>%
                                           filter(Year==max(Year)),
                                        index = c("Type_gs", "SNZ_commodity"),
                                        vSize = "Value",
                                        vColor = "CAGR5",
                                        type = 'value',
                                        #aspRatio = 1.618,
                                        overlap.labels = 1,
                                        fun.aggregate = "weighted.mean",
                                        #palette = "RdYlGn",
                                        draw = FALSE)
                         )
                         
                         if( class(fail_tm_ex_country)=='try-error' ){
                            output$KeyExCountryTotalTreeMap <-
                               renderHighchart({
                                  highchart %>%
                                     hc_title(text = "key commodities and services EXPORTS")
                               })
                         }else{
                            output$KeyExCountryTotalTreeMap <-
                               renderHighchart({
                                  highchart() %>%
                                     hc_add_series_treemap2(
                                  #hctreemap( 
                                     tmp_tm_ex_country ,
                                            allowDrillToNode = TRUE,
                                            layoutAlgorithm = "squarified",
                                            levelIsConstant = FALSE,
                                            levels = list(list(level = 1,
                                                               dataLabels = list(enabled = TRUE,
                                                                                 style = list(fontSize = '20px', color = 'white',
                                                                                              fontWeight = 'normal'),
                                                                                 backgroundColor = 'lightgrey',
                                                                                 align = 'left', verticalAlign = 'top'),
                                                               borderColor = "#555",
                                                               borderWidth = 2 ),
                                                          list(level = 2,
                                                               dataLabels = list(enabled = TRUE,
                                                                                 style = list(fontSize = '9px',
                                                                                              fontWeight = 'normal')
                                                               )
                                                          )
                                            )
                                  ) %>%
                                     hc_chart(backgroundColor = NULL, plotBorderColor = "#555", plotBorderWidth = 2) %>%
                                     hc_title(text = "key commodities and services EXPORTS") %>%
                                     hc_subtitle(text = "Coloured by compound annual growth rate (CAGR) for the past 5 years (%)") %>%
                                     hc_exporting(enabled = TRUE, formAttributes = list(target = "_blank")) %>%
                                     hc_tooltip(pointFormat = "<b>{point.name}</b>:<br>
                                             Export value: ${point.value:,.0f} m <br>
                                             CAGR 5: {point.colorValue:,.1f}%") %>% 
                                     hc_colorAxis(minColor = tmp_tm_ex_country$tm$color[which.min(tmp_tm_ex_country$tm$vColorValue)],
                                                  maxColor = tmp_tm_ex_country$tm$color[which.max(tmp_tm_ex_country$tm$vColorValue)] ,
                                                  labels = list(format = "{value}%", useHTML = TRUE), reversed = FALSE
                                     ) %>%
                                     hc_legend(align = "right", layout = "vertical", verticalAlign = "top",
                                               reversed = TRUE , y = 70, symbolHeight = 250, itemMarginTop = 10)
                               })
                         }

                         
                         ## III.11 Treemap Key import commodities KeyImCountryTotalTreeMap -------------------
                         tmp_dtf_im_country <-
                            get_snz_gs_country("Imports",tmp_selected_countries)%>%
                            mutate( Value = Value/10^6)

                         fail_tm_im_country <- 
                            try( tmp_tm_im_country <-
                                    treemap( tmp_dtf_im_country  %>%
                                                filter(Year==max(Year)) ,
                                             index = c("Type_gs", "SNZ_commodity"),
                                             vSize = "Value",
                                             vColor = "CAGR5",
                                             type = 'value',
                                             #aspRatio = 1.618,
                                             overlap.labels = 1,
                                             fun.aggregate = "weighted.mean",
                                             #palette = "RdYlGn",
                                             draw = FALSE)
                                 )
                         
                         if( class(fail_tm_im_country) == 'try-error' ){
                            output$KeyImCountryTotalTreeMap <-
                               renderHighchart({ 
                                  highchart() %>%
                                     hc_title(text = "key commodities and services IMPORTS") 
                            })
                         }else{
                            output$KeyImCountryTotalTreeMap <-
                               renderHighchart({
                                  highchart() %>%
                                     hc_add_series_treemap2(
                                  #hctreemap(
                                     tmp_tm_im_country ,
                                            allowDrillToNode = TRUE,
                                            layoutAlgorithm = "squarified",
                                            levelIsConstant = FALSE,
                                            levels = list(list(level = 1,
                                                               dataLabels = list(enabled = TRUE,
                                                                                 style = list(fontSize = '20px', color = 'white',
                                                                                              fontWeight = 'normal'),
                                                                                 backgroundColor = 'lightgrey',
                                                                                 align = 'left', verticalAlign = 'top'),
                                                               borderColor = "#555",
                                                               borderWidth = 2 ),
                                                          list(level = 2,
                                                               dataLabels = list(enabled = TRUE,
                                                                                 style = list(fontSize = '9px',
                                                                                              fontWeight = 'normal')
                                                               )
                                                          )
                                            )
                                  ) %>%
                                     hc_chart(backgroundColor = NULL, plotBorderColor = "#555", plotBorderWidth = 2) %>%
                                     hc_title(text = "key commodities and services IMPORTS") %>%
                                     hc_subtitle(text = "Coloured by compound annual growth rate (CAGR) for the past 5 years (%)") %>%
                                     hc_exporting(enabled = TRUE, formAttributes = list(target = "_blank")) %>%
                                     hc_tooltip(pointFormat = "<b>{point.name}</b>:<br>
                                                Import value: ${point.value:,.0f} m <br>
                                                CAGR 5: {point.colorValue:,.1f}%") %>% 
                                     hc_colorAxis(minColor = tmp_tm_im_country$tm$color[which.min(tmp_tm_im_country$tm$vColorValue)],
                                                  maxColor = tmp_tm_im_country$tm$color[which.max(tmp_tm_im_country$tm$vColorValue)] ,
                                                  labels = list(format = "{value}%", useHTML = TRUE), reversed = FALSE
                                     ) %>%
                                     hc_legend(align = "right", layout = "vertical", verticalAlign = "top",
                                               reversed = TRUE , y = 70, symbolHeight = 250, itemMarginTop = 10)
                               })
                         }
                         
                         ## III.12 Line graph Key commodity country key Exports KeyExCountryTotalLine ---------------------
                         print("------------------  Line graph for commodities -------------------------")
                         tmp_top_g_country_ex <-
                            tmp_dtf_ex_country %>%
                            filter( Year == max(Year),
                                    Type_gs == 'Goods',
                                    !SNZ_commodity %in% c('Confidential data', 'Other goods')
                            ) %>%
                            arrange( -Value ) %>%
                            dplyr::select( SNZ_commodity ) %>%
                            as.matrix() %>%
                            as.character

                         ## top 10 commodities
                         tmp_top_g_country_ex <-  tmp_top_g_country_ex[1:(min(10, length(tmp_top_g_country_ex)))]

                         tmp_top_s_country_ex <-
                            tmp_dtf_ex_country %>%
                            filter( Year == max(Year),
                                    Type_gs == 'Services',
                                    !SNZ_commodity %in% c('Other business services', 'Other services')
                            ) %>%
                            arrange( -Value ) %>%
                            dplyr::select( SNZ_commodity ) %>%
                            as.matrix() %>%
                            as.character

                         ## top 5 services
                         tmp_top_s_country_ex <-  na.omit(tmp_top_s_country_ex[1:(min(5, length(tmp_top_s_country_ex)))])

                         ## top 10 commodities and top 5services
                         tmp_top_country_ex <- c( tmp_top_g_country_ex, tmp_top_s_country_ex)

                         tmp_dtf_key_line_country_ex <-
                            tmp_dtf_ex_country %>%
                            filter( SNZ_commodity %in% tmp_top_country_ex,
                                    Year >=2007) %>%
                            mutate( SNZ_commodity = factor(SNZ_commodity, levels = tmp_top_country_ex)
                            ) %>%
                            arrange( SNZ_commodity )

                         ### plot
                         tmp_hc_ex_country <-
                            highchart() %>%
                            hc_exporting(enabled = TRUE, formAttributes = list(target = "_blank")) %>%
                            hc_xAxis( categories = c( unique( tmp_dtf_key_line_country_ex$Year) ) ) %>%
                            hc_yAxis( title = list(text = "$ million, NZD"), #"Commodities and services exports over $1 bn"
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

                         ### if any services are selected?
                         if( length(tmp_top_g_country_ex)>=1&length(tmp_top_s_country_ex)==0 ) {
                            output$KeyExCountryTotalLine <-
                               renderHighchart(
                                  tmp_hc_ex_country %>%
                                     hc_add_series( data =  tmp_dtf_key_line_country_ex %>% filter( Type_gs == 'Goods' ) ,
                                                    mapping = hcaes(  x = Year, y = Value, group = SNZ_commodity ),
                                                    type = 'line',
                                                    marker = list(symbol = 'circle') ,
                                                    visible = c(T,rep(F,length(tmp_top_g_country_ex)-1))
                                     )
                               )
                         }
                         if( length(tmp_top_g_country_ex)==0 & length(tmp_top_s_country_ex)>=1 ){
                            output$KeyExCountryTotalLine <-
                               renderHighchart(
                                  tmp_hc_ex_country %>%
                                     hc_add_series( data =  tmp_dtf_key_line_country_ex %>% filter( Type_gs == 'Services' ),
                                                    mapping = hcaes(  x = Year, y = Value, group = SNZ_commodity ),
                                                    type = 'line', dashStyle = 'DashDot', marker = list(symbol = 'circle') ,
                                                    visible = c(T,rep(F,length(tmp_top_s_country_ex)-1))
                                     )
                               )
                         }
                         if( length(tmp_top_g_country_ex)>=1 & length(tmp_top_s_country_ex)>=1 ){
                            output$KeyExCountryTotalLine <-
                               renderHighchart(
                                  tmp_hc_ex_country %>%
                                     hc_add_series( data =  tmp_dtf_key_line_country_ex %>% filter( Type_gs == 'Goods' ) ,
                                                    mapping = hcaes(  x = Year, y = Value, group = SNZ_commodity ),
                                                    type = 'line',
                                                    marker = list(symbol = 'circle') ,
                                                    visible = c(T,rep(F,length(tmp_top_g_country_ex)-1))
                                     ) %>%
                                     hc_add_series( data =  tmp_dtf_key_line_country_ex %>% filter( Type_gs == 'Services' ),
                                                    mapping = hcaes(  x = Year, y = Value, group = SNZ_commodity ),
                                                    type = 'line', dashStyle = 'DashDot', marker = list(symbol = 'circle') ,
                                                    visible = c(T,rep(F,length(tmp_top_s_country_ex)-1))
                                     )
                               )
                         }


                         ## III.13 Line graph Key commodity country key Exports Percent KeyExCountryTotalLinePercent ---------------------
                         tmp_ex_country_percent_hc <-
                            highchart() %>%
                            hc_exporting(enabled = TRUE, formAttributes = list(target = "_blank")) %>%
                            hc_xAxis( categories = c( unique( tmp_dtf_key_line_country_ex$Year) ) ) %>%
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
                         #hc_legend( enabled = FALSE )

                         ### if any services are selected?
                         if( length(tmp_top_g_country_ex)>=1&length(tmp_top_s_country_ex)==0 ) {
                            output$KeyExCountryTotalLinePercent <-
                               renderHighchart(
                                  tmp_ex_country_percent_hc %>%
                                     hc_add_series( data =  tmp_dtf_key_line_country_ex %>% filter( Type_gs == 'Goods' ) ,
                                                    mapping = hcaes(  x = Year, y = Share, group = SNZ_commodity ),
                                                    type = 'line',
                                                    marker = list(symbol = 'circle') ,
                                                    visible = c(T,rep(F,length(tmp_top_g_country_ex)-1))
                                     )
                               )
                         }
                         if( length(tmp_top_g_country_ex)==0 & length(tmp_top_s_country_ex)>=1 ){
                            output$KeyExCountryTotalLinePercent <-
                               renderHighchart(
                                  tmp_ex_country_percent_hc %>%
                                     hc_add_series( data =  tmp_dtf_key_line_country_ex %>% filter( Type_gs == 'Services' ),
                                                    mapping = hcaes(  x = Year, y = Share, group = SNZ_commodity ),
                                                    type = 'line', dashStyle = 'DashDot', marker = list(symbol = 'circle') ,
                                                    visible = c(T,rep(F,length(tmp_top_s_country_ex)-1))
                                     )
                               )
                         }
                         if( length(tmp_top_g_country_ex)>=1 & length(tmp_top_s_country_ex)>=1 ){
                            output$KeyExCountryTotalLinePercent <-
                               renderHighchart(
                                  tmp_ex_country_percent_hc %>%
                                     hc_add_series( data =  tmp_dtf_key_line_country_ex %>% filter( Type_gs == 'Goods' ) ,
                                                    mapping = hcaes(  x = Year, y = Share, group = SNZ_commodity ),
                                                    type = 'line',
                                                    marker = list(symbol = 'circle') ,
                                                    visible = c(T,rep(F,length(tmp_top_g_country_ex)-1))
                                     ) %>%
                                     hc_add_series( data =  tmp_dtf_key_line_country_ex %>% filter( Type_gs == 'Services' ),
                                                    mapping = hcaes(  x = Year, y = Share, group = SNZ_commodity ),
                                                    type = 'line', dashStyle = 'DashDot', marker = list(symbol = 'circle') ,
                                                    visible = c(T,rep(F,length(tmp_top_s_country_ex)-1))
                                     )
                               )
                         }

                         ## III.14 Line graph Key commodity country key Imports KeyImCountryTotalLine ---------------------
                         tmp_top_g_country_im <-
                            tmp_dtf_im_country %>%
                            filter( Year == max(Year),
                                    Type_gs == 'Goods',
                                    !SNZ_commodity %in% c('Confidential data', 'Other goods')
                            ) %>%
                            arrange( -Value ) %>%
                            dplyr::select( SNZ_commodity ) %>%
                            as.matrix() %>%
                            as.character

                         ## top 10 commodities
                         tmp_top_g_country_im <-  tmp_top_g_country_im[1:(min(10, length(tmp_top_g_country_im)))]

                         tmp_top_s_country_im <-
                            tmp_dtf_im_country %>%
                            filter( Year == max(Year),
                                    Type_gs == 'Services',
                                    !SNZ_commodity %in% c('Other business services', 'Other services')
                            ) %>%
                            arrange( -Value ) %>%
                            dplyr::select( SNZ_commodity ) %>%
                            as.matrix() %>%
                            as.character

                         ## top 5 services
                         tmp_top_s_country_im <-  na.omit(tmp_top_s_country_im[1:(min(5, length(tmp_top_s_country_im)))])

                         ## top 10 commodities and top 5services
                         tmp_top_country_im <- c( tmp_top_g_country_im, tmp_top_s_country_im)

                         tmp_dtf_key_line_country_im <-
                            tmp_dtf_im_country %>%
                            filter( SNZ_commodity %in% tmp_top_country_im,
                                    Year >=2007) %>%
                            mutate( SNZ_commodity = factor(SNZ_commodity, levels = tmp_top_country_im)
                            ) %>%
                            arrange( SNZ_commodity )

                         ### plot
                         tmp_hc_im_country <-
                            highchart() %>%
                            hc_exporting(enabled = TRUE, formAttributes = list(target = "_blank")) %>%
                            hc_xAxis( categories = c( unique( tmp_dtf_key_line_country_im$Year) ) ) %>%
                            hc_yAxis( title = list(text = "$ million, NZD"), #"Commodities and services exports over $1 bn"
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

                         ### if any services are selected?
                         if( length(tmp_top_g_country_im)>=1&length(tmp_top_s_country_im)==0 ) {
                            output$KeyImCountryTotalLine <-
                               renderHighchart(
                                  tmp_hc_im_country %>%
                                     hc_add_series( data =  tmp_dtf_key_line_country_im %>% filter( Type_gs == 'Goods' ) ,
                                                    mapping = hcaes(  x = Year, y = Value, group = SNZ_commodity ),
                                                    type = 'line',
                                                    marker = list(symbol = 'circle') ,
                                                    visible = c(T,rep(F,length(tmp_top_g_country_im)-1))
                                     )
                               )
                         }
                         if( length(tmp_top_g_country_im)==0 & length(tmp_top_s_country_im)>=1 ){
                            output$KeyImCountryTotalLine <-
                               renderHighchart(
                                  tmp_hc_im_country %>%
                                     hc_add_series( data =  tmp_dtf_key_line_country_im %>% filter( Type_gs == 'Services' ),
                                                    mapping = hcaes(  x = Year, y = Value, group = SNZ_commodity ),
                                                    type = 'line', dashStyle = 'DashDot', marker = list(symbol = 'circle') ,
                                                    visible = c(T,rep(F,length(tmp_top_s_country_im)-1))
                                     )
                               )
                         }
                         if( length(tmp_top_g_country_im)>=1 & length(tmp_top_s_country_im)>=1 ){
                            output$KeyImCountryTotalLine <-
                               renderHighchart(
                                  tmp_hc_im_country %>%
                                     hc_add_series( data =  tmp_dtf_key_line_country_im %>% filter( Type_gs == 'Goods' ) ,
                                                    mapping = hcaes(  x = Year, y = Value, group = SNZ_commodity ),
                                                    type = 'line',
                                                    marker = list(symbol = 'circle') ,
                                                    visible = c(T,rep(F,length(tmp_top_g_country_im)-1))
                                     ) %>%
                                     hc_add_series( data =  tmp_dtf_key_line_country_im %>% filter( Type_gs == 'Services' ),
                                                    mapping = hcaes(  x = Year, y = Value, group = SNZ_commodity ),
                                                    type = 'line', dashStyle = 'DashDot', marker = list(symbol = 'circle') ,
                                                    visible = c(T,rep(F,length(tmp_top_s_country_im)-1))
                                     )
                               )
                         }

                         ## III.15 Line graph Key commodity country key Imports Percent KeyImCountryTotalLinePercent ---------------------
                         tmp_im_country_percent_hc <-
                            highchart() %>%
                            hc_exporting(enabled = TRUE, formAttributes = list(target = "_blank")) %>%
                            hc_xAxis( categories = c( unique( tmp_dtf_key_line_country_im$Year) ) ) %>%
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
                         #hc_legend( enabled = FALSE )

                         ### if any services are selected?
                         if( length(tmp_top_g_country_im)>=1&length(tmp_top_s_country_im)==0 ) {
                            output$KeyImCountryTotalLinePercent <-
                               renderHighchart(
                                  tmp_im_country_percent_hc %>%
                                     hc_add_series( data =  tmp_dtf_key_line_country_im %>% filter( Type_gs == 'Goods' ) ,
                                                    mapping = hcaes(  x = Year, y = Share, group = SNZ_commodity ),
                                                    type = 'line',
                                                    marker = list(symbol = 'circle') ,
                                                    visible = c(T,rep(F,length(tmp_top_g_country_im)-1))
                                     )
                               )
                         }
                         if( length(tmp_top_g_country_im)==0 & length(tmp_top_s_country_im)>=1 ){
                            output$KeyImCountryTotalLinePercent <-
                               renderHighchart(
                                  tmp_im_country_percent_hc %>%
                                     hc_add_series( data =  tmp_dtf_key_line_country_im %>% filter( Type_gs == 'Services' ),
                                                    mapping = hcaes(  x = Year, y = Share, group = SNZ_commodity ),
                                                    type = 'line', dashStyle = 'DashDot', marker = list(symbol = 'circle') ,
                                                    visible = c(T,rep(F,length(tmp_top_s_country_im)-1))
                                     )
                               )
                         }
                         if( length(tmp_top_g_country_im)>=1 & length(tmp_top_s_country_im)>=1 ){
                            output$KeyImCountryTotalLinePercent <-
                               renderHighchart(
                                  tmp_im_country_percent_hc %>%
                                     hc_add_series( data =  tmp_dtf_key_line_country_im %>% filter( Type_gs == 'Goods' ) ,
                                                    mapping = hcaes(  x = Year, y = Share, group = SNZ_commodity ),
                                                    type = 'line',
                                                    marker = list(symbol = 'circle') ,
                                                    visible = c(T,rep(F,length(tmp_top_g_country_im)-1))
                                     ) %>%
                                     hc_add_series( data =  tmp_dtf_key_line_country_im %>% filter( Type_gs == 'Services' ),
                                                    mapping = hcaes(  x = Year, y = Share, group = SNZ_commodity ),
                                                    type = 'line', dashStyle = 'DashDot', marker = list(symbol = 'circle') ,
                                                    visible = c(T,rep(F,length(tmp_top_s_country_im)-1))
                                     )
                               )
                         }
                         ## !!!!!!!!!!!!! insert UI ---------------------
                         insertUI(
                            selector = "#country_trade_summary",
                            ui = 
                               div( id = 'country_trade_summary_all_items',
                                    ## 3.3.1.1 Sumary trade table for total selected country ---------------
                                    fluidRow( h1("Trade summary"),
                                              p("Compound annual growth rate (CAGR) for the past 1, 5, 10 years"),
                                              dataTableOutput("CountryTradeTableTotal") ) ,
                                    
                                    ## 3.3.1.1.0 Sumary investment table for total selected country ---------------
                                    fluidRow( h1("Investment position summary"),
                                              p(paste0("Directional basis stock of direct investment for the year ended March ", max(dtf_fdi_odi$Year) , " is used. Compound annual growth rate (CAGR) for the past 1, 5, 10 years")),
                                              dataTableOutput("CountryInvestmentTableTotal") ) ,
                                    
                                    ## 3.3.1.1.1 Sumary Ppl movement table for total selected country ---------------
                                    fluidRow( h1("Visitor movement summary"),
                                              p("Only short-term visitors are included. Compound annual growth rate (CAGR) for the past 1, 5, 10 years"),
                                              dataTableOutput("CountryPplMovementTableTotal") ) ,
                                    
                                    ## 3.3.1.2 Trade Trends graph for total selected country -----------------
                                    fluidRow( h1("Trade trends"),
                                              p("click the legend to hide/show the corresponding series") ),
                                    
                                    ### two way trade and trade deficit graph
                                    fluidRow( h3("Two-way trade and trade balance", align = 'center'),
                                              column(width = 6, h4("Two-way trade"), highchartOutput("CountryTwowayTradeGraphTotal")),
                                              column(width = 6, h4("Trade balance"), highchartOutput("CountryTradeBalanceGraphTotal")) ) ,
                                    
                                    ## total Exports
                                    fluidRow( h3("Total exports, goods exports and services exports" ,align = 'center'),
                                              column( width = 6, h4("Export values"), highchartOutput("CountryExportsGraphTotal") ),
                                              column( width = 6, h4("As a percentage of world exports"), highchartOutput("CountryExportsGraphTotalPercent") ) ) ,
                                    
                                    ## total Impports
                                    fluidRow( h3("Total imports, goods imports and services imports" ,align = 'center'),
                                              column( width = 6, h4("Import values"), highchartOutput("CountryImportsGraphTotal") ),
                                              column( width = 6, h4("As a percentage of world imports"), highchartOutput("CountryImportsGraphTotalPercent") ) ),
                                    
                                    
                                    ## 3.3.1.2.0 Investment position Trends graph for total selected country -----------------
                                    fluidRow( h1("Investment position trends"),
                                              p("Click the legend to hide/show the corresponding series. The series are annual data of March ended.") ),
                                    
                                    ## FDI and ODI ---
                                    fluidRow( h3("Two-way, foreign and overseas direct investment" ,align = 'center'),
                                              column( width = 6, h4("Investment stock values"), highchartOutput("CountryInvestmentGraphTotal") ),
                                              column( width = 6, h4("As a percentage of world"), highchartOutput("CountryInvestmentGraphTotalPercent") ) ),
                                    
                                    ## 3.3.1.2.1 Ppl movement Trends graph for total selected country -----------------
                                    fluidRow( h1("Visitor movement trends"),
                                              p("click the legend to hide/show the corresponding series") ),
                                    
                                    ## FDI and ODI ---
                                    fluidRow( h3("Two-way travel, foreign visitors travelling in and NZ visitors travelling out" ,align = 'center'),
                                              column( width = 6, h4("Number of visitors"), highchartOutput("CountryPplMovementGraphTotal") ),
                                              column( width = 6, h4("As a percentage of world"), highchartOutput("CountryPplMovementGraphTotalPercent") ) ),
                                    
                                    ## 3.3.1.1.2 exports and imports commodity - TREE MAP ----------------
                                    fluidRow( h2(paste0('Key commodities and services')), 
                                              tags$a(href = 'http://archive.stats.govt.nz/browse_for_stats/industry_sectors/imports_and_exports.aspx', "Key commodities and services are defined by Stats NZ", target = "_blank") ),
                                    fluidRow( highchartOutput('KeyExCountryTotalTreeMap') ),
                                    fluidRow( highchartOutput('KeyImCountryTotalTreeMap') ),
                                    
                                    ## 3.3.1.1.3 trends of key exports and imports commodity ---------------
                                    fluidRow( h2(paste0('Trends of key commodities and services')), 
                                              p("Click on the commodity or service names in the legend area to show their trends") ),
                                    fluidRow( h3("key commodities and services EXPORTS", align = 'center'),
                                              column( width = 6, h4("Export values"), highchartOutput('KeyExCountryTotalLine') ),
                                              column( width = 6, h4("As a percentage of world exports"), highchartOutput('KeyExCountryTotalLinePercent') ) ),
                                    fluidRow( h3("key commodities and services IMPORTS", align = 'center'),
                                              column( width = 6, h4("Import values"), highchartOutput('KeyImCountryTotalLine') ),
                                              column( width = 6, h4("As a percentage of world imports"), highchartOutput('KeyImCountryTotalLinePercent') ) )
                                    
                               )
                         )
                         
                         
                         ## IF multiple countries selected -------------------------------
                         if( !tmp_single_country ){
                            ## III.16 Appendix -- Export table CountrySummaryAllExports --------------------------
                            print("------------------  Appendix tables -------------------------")
                            tmp_ex_im_tb_country <- sum_selected_country_individual(tmp_selected_countries)
                            tmp_ex_country_tab <- tmp_ex_im_tb_country$Ex

                            # container of the table
                            sketch_ex <-  htmltools::withTags(table(
                               class = 'display',
                               thead(
                                  tr(
                                     th(rowspan = 2, 'Market'),
                                     th(colspan = 3, 'Total exports'),
                                     th(colspan = 3, 'Goods exports'),
                                     th(colspan = 3, 'Services exports')
                                  ),
                                  tr( #th('Country'),
                                     lapply(rep(c('Value ($m)', 'Share of world market', 'CAGR5'), 3), th, align = 'center')
                                  )
                               )
                            ))

                            output$CountrySummaryAllExports <-
                               renderDataTable({
                                  datatable(tmp_ex_country_tab,
                                            container = sketch_ex,
                                            rownames = FALSE,
                                            extensions = 'Buttons',
                                            options = list(dom = 'Bltp', 
                                                           scrollX = TRUE,
                                                           buttons = c('copy', 'csv', 'excel', 'pdf', 'print'),
                                                           pageLength = 5,
                                                           lengthMenu = list(c(5,  -1), list('5', 'All')),
                                                           columnDefs = list(list(className = 'dt-center', targets = 0:(ncol(tmp_ex_country_tab)-1) ) )
                                                           )
                                             ) %>%
                                     formatPercentage( c('TotExShare', 'TotExCAGR5', 'GExShare', 'GExCAGR5', 'SExShare', 'SExCAGR5'),digit = 1 ) %>%
                                     formatCurrency( columns = c('TotExValue','GExValue', 'SExValue'), digits = 0 ) %>%
                                     formatStyle(
                                        c('TotExCAGR5', 'GExCAGR5', 'SExCAGR5'),
                                        background = styleColorBar( c(0,max(tmp_ex_country_tab[,c('TotExCAGR5','GExCAGR5', 'SExCAGR5')],na.rm=T)*2) ,
                                                                    'lightblue'),
                                        backgroundSize = '100% 90%',
                                        backgroundRepeat = 'no-repeat',
                                        backgroundPosition = 'center',
                                        color = JS("value < 0 ? 'darkred' : value > 0 ? 'darkgreen' : 'black'")
                                     ) %>%
                                     formatStyle( 1:ncol(tmp_ex_country_tab), 'vertical-align'='center', 'text-align' = 'center' )
                               })

                            ## III.17 Appendix -- Import table CountrySummaryAllImports--------------------------
                            tmp_im_country_tab <- tmp_ex_im_tb_country$Im

                            # container of the table
                            sketch_im <-  htmltools::withTags(table(
                               class = 'display',
                               thead(
                                  tr(
                                     th(rowspan = 2, 'Market'),
                                     th(colspan = 3, 'Total imports'),
                                     th(colspan = 3, 'Goods imports'),
                                     th(colspan = 3, 'Services imports')
                                  ),
                                  tr( #th('Country'),
                                     lapply(rep(c('Value ($m)', 'Share of world market', 'CAGR5'), 3), th)
                                  )
                               )
                            ))

                            output$CountrySummaryAllImports <-
                               renderDataTable({
                                  datatable(tmp_im_country_tab,
                                            container = sketch_im,
                                            rownames = FALSE,
                                            extensions = 'Buttons',
                                            options = list(dom = 'Bltp', 
                                                           scrollX = TRUE , 
                                                           buttons = c('copy', 'csv', 'excel', 'pdf', 'print'),
                                                           pageLength = 5,
                                                           lengthMenu = list(c(5,  -1), list('5', 'All')),
                                                           columnDefs = list(list(className = 'dt-center', targets = 0:(ncol(tmp_im_country_tab)-1) ) )
                                                           )
                                  ) %>%
                                     formatPercentage( c('TotImShare', 'TotImCAGR5', 'GImShare', 'GImCAGR5', 'SImShare', 'SImCAGR5'),digit = 1 ) %>%
                                     formatCurrency( columns = c('TotImValue','GImValue', 'SImValue'), digits = 0 ) %>%
                                     formatStyle(
                                        c('TotImCAGR5', 'GImCAGR5', 'SImCAGR5'),
                                        background = styleColorBar( c(0,max(tmp_im_country_tab[,c('TotImCAGR5','GImCAGR5', 'SImCAGR5')],na.rm=T)*2) ,
                                                                    'lightblue'),
                                        backgroundSize = '100% 90%',
                                        backgroundRepeat = 'no-repeat',
                                        backgroundPosition = 'center',
                                        color = JS("value < 0 ? 'darkred' : value > 0 ? 'darkgreen' : 'black'")
                                     ) %>%
                                     formatStyle( 1:ncol(tmp_im_country_tab), 'vertical-align'='center', 'text-align' = 'center' )
                               })

                            ## III.18 Appendxi -- Twoway trade and balance talbe CountrySummaryAllTwowayBalance------------------
                            tmp_tb_country_tab <-tmp_ex_im_tb_country$TB

                            # container of the table
                            sketch_tb <-  htmltools::withTags(table(
                               class = 'display',
                               thead(
                                  tr(
                                     th(rowspan = 2, 'Market'),
                                     th(colspan = 3, 'Two-way trade'),
                                     th(colspan = 1, 'Trade balance'),
                                     th(colspan = 1, 'Goods balance'),
                                     th(colspan = 1, 'Services balance')
                                     
                                  ),
                                  tr( #th('Country'),
                                     lapply(rep(c('Value ($m)', 'Share of world market', 'CAGR5'), 1), th),
                                     th('Value ($m)'),
                                     th('Value ($m)'),
                                     th('Value ($m)')
                                     
                                  )
                               )
                            ))

                            output$CountrySummaryAllTwowayBalance <-
                               renderDataTable({
                                  datatable(tmp_tb_country_tab,
                                            container = sketch_tb,
                                            rownames = FALSE,
                                            extensions = 'Buttons',
                                            options = list(dom = 'Bltp', 
                                                           scrollX = TRUE,
                                                           buttons = c('copy', 'csv', 'excel', 'pdf', 'print'),
                                                           pageLength = 5,
                                                           lengthMenu = list(c(5,  -1), list('5', 'All')) ,
                                                           columnDefs = list(list(className = 'dt-center', targets = 0:(ncol(tmp_tb_country_tab)-1) ) )
                                            )
                                  ) %>%
                                     formatPercentage( c('TwowayShare', 'TwowayCAGR5') , digit = 1 ) %>%
                                     formatCurrency( columns = c('TwowayValue','BalanceValue','BalanceValue_g','BalanceValue_s'), digits = 0 ) %>%
                                     formatStyle(
                                        c('TwowayCAGR5'),
                                        background = styleColorBar( c(0,max(tmp_tb_country_tab[,c('TwowayCAGR5')],na.rm=T)*2) ,
                                                                    'lightblue'),
                                        backgroundSize = '100% 90%',
                                        backgroundRepeat = 'no-repeat',
                                        backgroundPosition = 'center',
                                        color = JS("value < 0 ? 'darkred' : value > 0 ? 'darkgreen' : 'black'")
                                     ) %>%
                                     formatStyle( c('BalanceValue','BalanceValue_g','BalanceValue_s'),
                                                   color = JS("value < 0 ? 'darkred' : value > 0 ? 'darkgreen' : 'black'")
                                                   ) %>%
                                     formatStyle( 1:ncol(tmp_tb_country_tab), 'vertical-align'='center', 'text-align' = 'center' )
                               })
                            
                            ## III.19 Appendix -- FDI ODI and Two-wayDI table CountrySummaryAllInvestment --------------------------
                            print("------------------  Appendix Investment tables -------------------------")
                            tmp_invest_tb_country <- sum_selected_country_individual_investment(tmp_selected_countries)
                            
                            # container of the table
                            sketch_invest <-  htmltools::withTags(table(
                               class = 'display',
                               thead(
                                  tr(
                                     th(rowspan = 2, 'Market'),
                                     th(colspan = 3, 'Foreign direct investment'),
                                     th(colspan = 3, 'Overseas direct investment'),
                                     th(colspan = 3, 'Two-way direct investment')
                                  ),
                                  tr( #th('Country'),
                                     lapply(rep(c('Value ($m)', 'Share of world market', 'CAGR5'), 3), th, align = 'center')
                                  )
                               )
                            ))
                            
                            output$CountrySummaryAllInvestment <-
                               renderDataTable({
                                  datatable(tmp_invest_tb_country,
                                            container = sketch_invest,
                                            rownames = FALSE,
                                            extensions = 'Buttons',
                                            options = list(dom = 'Bltp', 
                                                           scrollX = TRUE,
                                                           buttons = c('copy', 'csv', 'excel', 'pdf', 'print'),
                                                           pageLength = 5,
                                                           lengthMenu = list(c(5,  -1), list('5', 'All')),
                                                           columnDefs = list(list(className = 'dt-center', targets = 0:(ncol(tmp_invest_tb_country)-1) ) )
                                            )
                                  ) %>%
                                     formatPercentage( c('FDIShare', 'FDICAGR5', 'ODIShare', 'ODICAGR5', 'TwowayDIShare', 'TwowayDICAGR5'),digit = 1 ) %>%
                                     formatCurrency( columns = c('FDIValue','ODIValue', 'TwowayDIValue'), digits = 0 ) %>%
                                     formatStyle(
                                        c('FDICAGR5', 'ODICAGR5', 'TwowayDICAGR5'),
                                        background = styleColorBar( c(0,max(tmp_invest_tb_country[,c('FDICAGR5','ODICAGR5', 'TwowayDICAGR5')],na.rm=T)*2) ,
                                                                    'lightblue'),
                                        backgroundSize = '100% 90%',
                                        backgroundRepeat = 'no-repeat',
                                        backgroundPosition = 'center',
                                        color = JS("value < 0 ? 'darkred' : value > 0 ? 'darkgreen' : 'black'")
                                     ) %>%
                                     formatStyle( 1:ncol(tmp_invest_tb_country), 'vertical-align'='center', 'text-align' = 'center' )
                               })


                            ## III.19 Appendix -- Visitor movement table CountrySummaryAllPplMovement --------------------------
                            print("------------------  Appendix Visitor movement tables -------------------------")
                            tmp_pplmove_tb_country <- sum_selected_country_individual_pplmove(tmp_selected_countries)
                            
                            # container of the table
                            sketch_pplmove <-  htmltools::withTags(table(
                               class = 'display',
                               thead(
                                  tr(
                                     th(rowspan = 2, 'Market'),
                                     th(colspan = 3, 'Foreign visitors travelling in'),
                                     th(colspan = 3, 'NZ visitors travelling out'),
                                     th(colspan = 3, 'Two-way visitor movement')
                                  ),
                                  tr( #th('Country'),
                                     lapply(rep(c("Value ('000)", 'Share of world market', 'CAGR5'), 3), th, align = 'center')
                                  )
                               )
                            ))
                            
                            output$CountrySummaryAllPplMovement <-
                               renderDataTable({
                                  datatable(tmp_pplmove_tb_country,
                                            container = sketch_pplmove,
                                            rownames = FALSE,
                                            extensions = 'Buttons',
                                            options = list(dom = 'Bltp',
                                                           scrollX = TRUE,
                                                           buttons = c('copy', 'csv', 'excel', 'pdf', 'print'),
                                                           pageLength = 5,
                                                           lengthMenu = list(c(5,  -1), list('5', 'All')),
                                                           columnDefs = list(list(className = 'dt-center', targets = 0:(ncol(tmp_invest_tb_country)-1) ) )
                                            )
                                  ) %>%
                                     formatPercentage( c('InShare', 'InCAGR5', 'OutShare', 'OutCAGR5', 'TwowayMoveShare', 'TwowayMoveCAGR5'),digit = 1 ) %>%
                                     formatCurrency( columns = c('InValue','OutValue', 'TwowayMoveValue'), digits = 0, currency = '' ) %>%
                                     formatStyle(
                                        c('InCAGR5', 'OutCAGR5', 'TwowayMoveCAGR5'),
                                        background = styleColorBar( c(0,max(tmp_pplmove_tb_country[,c('InCAGR5','OutCAGR5', 'TwowayMoveCAGR5')],na.rm=T)*2) ,
                                                                    'lightblue'),
                                        backgroundSize = '100% 90%',
                                        backgroundRepeat = 'no-repeat',
                                        backgroundPosition = 'center',
                                        color = JS("value < 0 ? 'darkred' : value > 0 ? 'darkgreen' : 'black'")
                                     ) %>%
                                     formatStyle( 1:ncol(tmp_pplmove_tb_country), 'vertical-align'='center', 'text-align' = 'center' )
                               })
                            
                            
                            ## !!!!!!!!!!!!!!!! insert UI ------------------------
                            insertUI(
                               selector = "#country_appendix",
                               ui = div( id = "country_trade_summary_appendix",
                                         conditionalPanel( "input.select_country.length > 1 || 
                                                            input.select_country.valueOf() == 'APEC' || 
                                                           input.select_country.valueOf() == 'EU28'||
                                                           input.select_country.valueOf() == 'CPTPP' ||
                                                           input.select_country.valueOf() == 'GCC' ||
                                                           input.select_country.valueOf() == 'Pacific Islands Forum' ||
                                                           input.select_country.valueOf() == 'ASEAN' ||
                                                           input.select_country.valueOf() == 'OECD' ||
                                                           input.select_country.valueOf() == 'Five Eyes' ||
                                                           input.select_country.valueOf() == 'Latin America'||
                                                           input.select_country.valueOf() == 'OPEC' ||
                                                           input.select_country.valueOf() == 'FTA in force' ||
                                                           input.select_country.valueOf() == 'Middle East' || 
                                                           input.select_country.valueOf() == 'Northern Africa' || 
                                                           input.select_country.valueOf() == 'Eastern Africa' || 
                                                           input.select_country.valueOf() == 'Central Africa' || 
                                                           input.select_country.valueOf() == 'Southern Africa' || 
                                                           input.select_country.valueOf() == 'Western Africa' || 
                                                           input.select_country.valueOf() == 'Africa' || 
                                                           input.select_country.valueOf() == 'Arab Maghreb Union' ||
                                                           input.select_country.valueOf() == 'Eastern African Community' ||
                                                           input.select_country.valueOf() == 'Economic Community of West African States' ||
                                                           input.select_country.valueOf() == 'Southern African Development Community' ||
                                                           input.select_country.valueOf() == 'G7' ||
                                                           input.select_country.valueOf() == 'BRI countries' ",
                                                           
                                                           ### trade appendix 
                                                           fluidRow( h1("Appendix -- trade, investment, visitor movement statistics for all selected markets") ),
                                                           fluidRow( h2("Exports"),
                                                                     dataTableOutput("CountrySummaryAllExports") ),
                                                           fluidRow( h2("Imports"),
                                                                     dataTableOutput("CountrySummaryAllImports") ),
                                                           fluidRow( h2("Two-way trade and trade balance"),
                                                                     dataTableOutput("CountrySummaryAllTwowayBalance") ),
                                                           
                                                           ### investment appendix
                                                           #fluidRow( h1("Appendix -- investment position statistics for all selected markets") ),
                                                           fluidRow( h2("Directional investment stocks"),
                                                                     p(paste0("Directional basis stock of direct investment for the year ended March ", max(dtf_fdi_odi$Year) , " is used.")),
                                                                     dataTableOutput("CountrySummaryAllInvestment") ) ,
                                                           
                                                           ### ppl movement appendix
                                                           #fluidRow( h1("Appendix -- visitor movement statistics for all selected markets") ),
                                                           fluidRow( h2("Visitor movement"),
                                                                     dataTableOutput("CountrySummaryAllPplMovement") ) 
                                                           )
                                         )
                            )
                         }
                         ## hide wait message ----
                         shinyjs::hide( id = 'wait_message_country_intel' )

                     }
                   }
                  )
      
      ## IV. HS code finder/ Quick intelligence by HS code -----------------------------------
      ## 4.0.0 setup HS code table values ---------------
      output$HSCodeTable <- 
         renderDataTable({
            datatable( concord_hs24,
                       rownames = FALSE,
                       filter = c("top"),
                       #sDom = "top",
                       extensions = 'Buttons',
                       options = list(dom = 'Bfltp', 
                                      scrollX = TRUE,
                                      buttons = c('copy', 'csv', 'excel', 'pdf', 'print'),
                                      pageLength = 10,
                                      lengthMenu = list(c(10,  -1), list('10', 'All')),
                                      searchHighlight = TRUE,
                                      search = list(regex = TRUE, caseInsensitive = FALSE )
                       ),
                       colnames = c("HS level", "HS code", 'Classification')
                       )
         })
      
      HSCodeTable_proxy = dataTableProxy("HSCodeTable")
      
      ## Insert a button to clear all selections --
      observeEvent( input$action_bnt_ClearTable,
                    ({
                       HSCodeTable_proxy %>% selectRows(NULL)
                    })
      )
      
      ## 4.0 setup this reative values ---------------
      rv_intelHS <- reactiveValues()
      
      ## reactive values ---------------------
      observe({
         if (is.null(input$rbtn_intel_by_hs) || is.null(input$HSCodeTable_rows_selected) || length(input$HSCodeTable_rows_selected) == 0) {
            return()
         }
         ## --- show loading message when click on HS codes -------
         try(
            if( !is.null(input$HSCodeTable_rows_selected) #& 
                #is.null(rv_intelHS$tmp_tab) 
                ){
               shinyjs::show( id = "ci_intel_hs_loading_message" )
            }
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
      values <- reactiveValues(
         raw_data = NULL,      # 업로드된 원본
         clean_data = NULL,    # 전처리된 데이터
         diagnosis_res = NULL, # 진단 결과 (BCG 좌표 등)
         forecast_res = NULL,  # 예측 결과
         action_plan = NULL    # 시뮬레이션 결과
      )
      rv_steps <- reactiveValues(done = c(FALSE, FALSE, FALSE, FALSE))
      target_values <- reactiveValues(turn = 3, growth = 0.10)
      action_notes_val <- reactiveVal("")

      update_step_status <- function(step, done = TRUE) {
         cur <- rv_steps$done
         cur[step] <- done
         rv_steps$done <- cur
      }
      step_labels <- c("데이터 준비", "현황 진단", "수요 예측", "액션/공유")

      fin_validate <- shiny::validate
      fin_need <- shiny::need
      output$step_timeline <- renderUI({
         done <- rv_steps$done
         active <- switch(input$sidebar,
                          tab_data = 1L,
                          tab_diagnosis = 2L,
                          tab_prediction = 3L,
                          tab_action = 4L,
                          usage_guide = 4L,
                          NULL)
         tags$div(
            class = "step-timeline",
            lapply(seq_along(step_labels), function(i) {
               classes <- c("step-pill")
               if (!is.null(active) && !is.na(active) && active == i) classes <- c(classes, "active")
               if (length(done) >= i && isTRUE(done[[i]])) classes <- c(classes, "done")
               actionLink(
                  paste0("step_nav_", i),
                  label = HTML(sprintf(
                     "<span class='step-index'>%d</span><span class='step-title'>%s</span>%s",
                     i, step_labels[[i]],
                     if (length(done) >= i && isTRUE(done[[i]])) "<span class='step-check'>&#10003;</span>" else ""
                  )),
                  class = paste(classes, collapse = " ")
               )
            })
         )
      })
      observeEvent(input$step_nav_1, {
         updateTabItems(session, "sidebar", "tab_data")
         shinyjs::runjs("window.scrollTo({top:0,behavior:'smooth'});")
      })
      observeEvent(input$step_nav_2, {
         updateTabItems(session, "sidebar", "tab_diagnosis")
         shinyjs::runjs("window.scrollTo({top:0,behavior:'smooth'});")
      })
      observeEvent(input$step_nav_3, {
         updateTabItems(session, "sidebar", "tab_prediction")
         shinyjs::runjs("setTimeout(function(){var el=document.getElementById('segmentation_tabs'); if(el){el.scrollIntoView({behavior:'smooth',block:'start'});}},300);")
      })
      observeEvent(input$step_nav_4, {
         updateTabItems(session, "sidebar", "tab_action")
         shinyjs::runjs("setTimeout(function(){var el=document.getElementById('share_panel'); if(el){el.scrollIntoView({behavior:'smooth',block:'start'});}},400);")
      })
      observeEvent(input$step1_card, {
         updateTabItems(session, "sidebar", "tab_data")
      })
      observeEvent(input$go_step2_after_upload, {
         update_step_status(1, TRUE)
         update_step_status(2, TRUE)
         updateTabItems(session, "sidebar", "tab_diagnosis")
      })
      observeEvent(input$to_step3_panel, {
         updateTabItems(session, "sidebar", "tab_prediction")
         shinyjs::runjs("setTimeout(function(){var el=document.getElementById('segmentation_tabs'); if(el){el.scrollIntoView({behavior:'smooth',block:'start'});}},250);")
      })
      observeEvent(input$back_to_step2, {
         updateTabItems(session, "sidebar", "tab_diagnosis")
         shinyjs::runjs("window.scrollTo({top:0,behavior:'smooth'});")
      })
      observeEvent(input$go_detail, {
         updateTabItems(session, "sidebar", "tab_action")
         update_step_status(4, TRUE)
      })
      observeEvent(input$go_to_action, {
         updateTabItems(session, "sidebar", "tab_action")
         update_step_status(4, TRUE)
         shinyjs::runjs("setTimeout(function(){window.scrollTo({top:0,behavior:'smooth'});},150);")
      })

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
            values$forecast_res <- NULL
         }
      }, once = TRUE)

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
      fin_validate_upload_df <- function(df) {
         if (is.null(df) || !nrow(df)) stop("업로드한 파일이 비어 있습니다.")
         if (ncol(df) < 3) stop("컬럼 수가 충분하지 않습니다. 샘플 템플릿을 참고하세요.")
         TRUE
      }
      growth_target <- reactive({
         gt <- if (!is.null(input$target_growth)) input$target_growth / 100 else target_values$growth
         if (is.null(gt) || !is.finite(gt)) 0.1 else gt
      })
      turn_target <- reactive({
         tt <- if (!is.null(input$target_turn)) input$target_turn else target_values$turn
         if (is.null(tt) || !is.finite(tt)) 3 else tt
      })
      output$fin_template <- downloadHandler(
         filename = function() "fin_template.csv",
         content = function(file) {
            template <- tibble(
               year = 2020:2022,
               sales = c(120000000, 130000000, 150000000),
               inventory = c(30000000, 32000000, 35000000),
               net_income = c(8000000, 9000000, 11000000),
               total_assets = c(60000000, 65000000, 70000000),
               cogs = c(70000000, 75000000, 82000000),
               sku = c("SKU-A", "SKU-B", "SKU-C"),
               channel = c("온라인", "오프라인", "온라인")
            )
            readr::write_csv(template, file)
         }
      )

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
            values$forecast_res <- NULL
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
               values$forecast_res <- NULL
               update_step_status(1, TRUE)
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
               values$forecast_res <- NULL
               update_step_status(1, TRUE)
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
         values$raw_data <- fin_values$df_my_norm
         values$clean_data <- fin_values$df_my_norm
         fin_values$fc_result <- NULL
         values$forecast_res <- NULL
         update_step_status(1, TRUE)
         showNotification("데모 데이터가 로드되었습니다.", type = "message", duration = 4)
      })

      observeEvent(input$fin_upload, {
         req(input$fin_upload$datapath)
         df <- fin_read_upload_df(input$fin_upload$datapath)
         ok <- tryCatch(
            {
               fin_validate_upload_df(df)
               TRUE
            },
            error = function(e) {
               showNotification(conditionMessage(e), type = "error", duration = 6)
               FALSE
            }
         )
         if (!ok) return()
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
         values$raw_data <- df
         values$clean_data <- NULL
         fin_values$fc_result <- NULL
         values$forecast_res <- NULL
         update_step_status(1, TRUE)
         showNotification("업로드 완료! Step2 버튼을 눌러 다음 단계로 이동하세요.", type = "message", duration = 4)
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
         values$clean_data <- res
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
         values$clean_data <- df_all
         if (is.null(df_all) || nrow(df_all) == 0) {
            values$diagnosis_res <- NULL
            return()
         }
         latest_year <- max(df_all$year, na.rm = TRUE)
         latest <- df_all %>% filter(.data$year == latest_year)
         user_turnover <- mean(latest$inventory_turnover, na.rm = TRUE)
         user_margin <- mean(latest$roa, na.rm = TRUE)
         yr_pick <- if (!is.null(input$global_year)) input$global_year else max(dtf_shiny_commodity_service_ex$Year, na.rm = TRUE)
         industry <- dtf_shiny_commodity_service_ex %>%
            filter(.data$Year == yr_pick) %>%
            summarize(
               avg_turnover = mean(.data$CAGR5, na.rm = TRUE),
               avg_margin = mean(.data$CAGR5, na.rm = TRUE) / 100,
               .groups = "drop"
            )
         avg_turnover <- if (nrow(industry) && is.finite(industry$avg_turnover)) industry$avg_turnover[[1]] else 3
         avg_margin <- if (nrow(industry) && is.finite(industry$avg_margin)) industry$avg_margin[[1]] else 0.08
         quadrant <- if (!is.na(user_turnover) && !is.na(user_margin)) {
            if (user_turnover >= avg_turnover && user_margin >= avg_margin) "Cash Cow" else "Dog"
         } else "데이터 부족"
         msg <- if (is.na(user_turnover) || is.na(user_margin)) {
            "데이터가 부족해 정확한 위치를 계산할 수 없습니다."
         } else if (user_turnover < avg_turnover) {
            "현재 귀사는 악성 재고 구간에 있습니다. 긴급 처방이 필요합니다."
         } else {
            "재고 속도는 업종 평균 이상입니다. 수익성을 함께 끌어올리세요."
         }
         values$diagnosis_res <- list(
            latest_year = latest_year,
            user_turnover = user_turnover,
            user_margin = user_margin,
            avg_turnover = avg_turnover,
            avg_margin = avg_margin,
            quadrant = quadrant,
            message = msg
         )
         update_step_status(2, TRUE)
      })

      data_health <- reactive({
         df <- values$clean_data
         raw <- values$raw_data
         years <- if (!is.null(df)) unique(df$year) else integer()
         nulls <- if (!is.null(df)) {
            cols <- intersect(c("year", "sales", "inventory"), names(df))
            sum(!complete.cases(df[, cols, drop = FALSE]))
         } else NA_integer_
         list(
            has_raw = !is.null(raw) && nrow(raw) > 0,
            has_clean = !is.null(df) && nrow(df) > 0,
            nulls = nulls,
            year_count = length(years),
            latest_year = if (length(years)) max(years) else NA_integer_
         )
      })

      observe({
         h <- data_health()
         diag_ready <- isTRUE(h$has_clean) && (is.na(h$nulls) || h$nulls == 0)
         pred_ready <- diag_ready && h$year_count >= 3
         action_ready <- pred_ready && !is.null(values$forecast_res)
         diag_sel <- "a[data-value='tab_diagnosis']"
         pred_sel <- "a[data-value='tab_prediction']"
         act_sel <- "a[data-value='tab_action']"
         if (!diag_ready) {
            shinyjs::disable(selector = diag_sel)
            shinyjs::disable(selector = pred_sel)
            shinyjs::disable(selector = act_sel)
         } else {
            shinyjs::enable(selector = diag_sel)
            if (pred_ready) shinyjs::enable(selector = pred_sel) else shinyjs::disable(selector = pred_sel)
            if (action_ready) shinyjs::enable(selector = act_sel) else shinyjs::disable(selector = act_sel)
         }
      })

      output$data_health_signals <- renderUI({
         h <- data_health()
         mk_card <- function(icon, title, desc, status) {
            div(
               class = paste("precheck-card", status),
               div(class = "precheck-icon", icon),
               h4(title),
               p(desc)
            )
         }
         null_status <- if (is.na(h$nulls)) "pending" else if (h$nulls > 0) "alert" else "ok"
         null_desc <- if (is.na(h$nulls)) {
            "업로드하면 자동으로 결측을 점검해요."
         } else if (h$nulls > 0) {
            "필수 컬럼에 결측이 있어 보완이 필요합니다."
         } else {
            "필수 컬럼에서 결측이 발견되지 않았습니다."
         }
         year_status <- if (h$year_count >= 3) "ok" else if (h$year_count > 0) "warn" else "pending"
         year_desc <- if (h$year_count == 0) {
            "최소 1개 파일을 업로드하면 연도 범위를 계산합니다."
         } else if (h$year_count < 3) {
            "예측을 위해 3개 이상 연도를 확보해주세요."
         } else {
            paste0(h$year_count, "개 연도로 예측을 준비할 수 있습니다.")
         }
         forecast_ready <- !is.null(values$forecast_res)
         forecast_status <- if (forecast_ready) "ok" else if (h$year_count >= 3) "warn" else "alert"
         forecast_desc <- if (forecast_ready) {
            "이제 ‘현황 진단’ 또는 ‘예측 실행’을 눌러 결과를 확인하세요."
         } else if (h$year_count >= 3) {
            "예측 실행 버튼을 눌러 리본 포함 전망을 생성하세요."
         } else {
            "연도 3개 이상 확보 후 예측 실행이 가능합니다."
         }
         div(
            class = "precheck-card-grid",
            mk_card(if (null_status == "ok") "🟢" else if (null_status == "alert") "⚠️" else "⌛", "데이터 이상 여부", null_desc, null_status),
            mk_card("📅", if (h$year_count == 0) "연도 데이터 없음" else paste0(h$year_count, "개 연도 확보"), year_desc, year_status),
            mk_card("▶️", if (forecast_ready) "예측 실행 가능" else "예측 준비 중", forecast_desc, forecast_status)
         )
      })

      output$tab_lock_notice <- renderUI({
         h <- data_health()
         ready_diag <- isTRUE(h$has_clean) && (is.na(h$nulls) || h$nulls == 0)
         ready_pred <- ready_diag && h$year_count >= 3
         ready_action <- ready_pred && !is.null(values$forecast_res)
         msg <- if (!ready_diag) {
            "🟥 업로드/매핑을 먼저 완료하면 진단 탭이 열립니다."
         } else if (!ready_pred) {
            "🟡 연도 3개 이상이 확보되면 예측 탭이 활성화됩니다."
         } else if (!ready_action) {
            "🟡 예측을 실행하면 액션 플랜 탭이 활성화됩니다."
         } else {
            "🟢 진단/예측/액션 탭이 모두 활성화되었습니다."
         }
         div(class = "assist-text", strong(msg))
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
         values$forecast_res <- fin_values$fc_result
      })

      fin_forecast_result <- reactive({
         res <- fin_values$fc_result
         fin_validate(fin_need(!is.null(res) && !is.null(res$forecast) && nrow(res$forecast) > 0,
                               "예측 결과가 없습니다. '예측 실행'을 눌러주세요."))
         res
      })
      observeEvent(fin_values$fc_result, {
         values$forecast_res <- fin_values$fc_result
      })

      pred_focus_source <- reactive({
         res <- fin_forecast_result()
         if (!is.null(res$source)) res$source else "선택된 소스"
      })

      output$fin_kpi_row <- renderUI({
         df <- fin_combined_df()
         fin_validate(fin_need(nrow(df) > 0, "데이터를 불러오세요"))
         latest_year <- max(df$year, na.rm = TRUE)
         latest <- df %>% filter(.data$year == latest_year)
         mk_box <- function(title, value, color = "primary") {
            formatted <- scales::label_number(
               scale_cut = scales::cut_short_scale()
            )(value)
            valueBox(
               value = HTML(sprintf(
                  "<span class='fin-kpi-title'>%s</span><span class='fin-kpi-value'>%s</span>",
                  title,
                  formatted
               )),
               subtitle = NULL,
               color = color
            )
         }
         fluidRow(
            mk_box("최근 연도 매출", sum(latest$sales, na.rm = TRUE), "blue"),
            mk_box("재고자산회전율", mean(latest$inventory_turnover, na.rm = TRUE), "green"),
            mk_box("ROA", mean(latest$roa, na.rm = TRUE), "yellow")
         )
      })

      fin_summary_text <- reactive({
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

      output$fin_summary <- renderText(fin_summary_text())
      output$fin_summary_plus <- renderText(fin_summary_text())

      make_fin_ts_plot <- function(df) {
         fin_validate(fin_need(nrow(df) > 0, "데이터를 불러오세요"))
         plot_ly(df, x = ~year, y = ~sales, color = ~source, type = "scatter", mode = "lines+markers") %>%
            layout(yaxis = list(title = "매출액"))
      }
      output$fin_ts_plot <- renderPlotly(make_fin_ts_plot(fin_combined_df()))
      output$fin_ts_plot_plus <- renderPlotly(make_fin_ts_plot(fin_combined_df()))

      make_fin_quad_plot <- function(df) {
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
               yaxis = list(title = "ROA")
            )
      }
      output$fin_quad_plot <- renderPlotly(make_fin_quad_plot(fin_combined_df()))
      output$fin_quad_plot_plus <- renderPlotly(make_fin_quad_plot(fin_combined_df()))

      make_fin_fc_plot <- function(res, df_all) {
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
      }

      output$fin_fc_table <- renderTable({
         res <- fin_forecast_result()
         res$forecast
      })
      output$fin_fc_table_plus <- renderTable({
         res <- fin_forecast_result()
         res$forecast
      })

      output$fin_fc_plot <- renderPlotly({
         res <- fin_forecast_result()
         make_fin_fc_plot(res, fin_combined_df())
      })
      output$fin_fc_plot_plus <- renderPlotly({
         res <- fin_forecast_result()
         make_fin_fc_plot(res, fin_combined_df())
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
         update_step_status(3, TRUE)
         showNotification("예측이 업데이트되었습니다.", type = "message", duration = 4)
      })

      ## Additional KPI boxes for teammate tabs
      output$fin_kpi_sales <- renderUI({
         df <- fin_combined_df()
         fin_validate(fin_need(nrow(df) > 0, "데이터를 불러오세요"))
         latest_year <- max(df$year, na.rm = TRUE)
         latest <- df %>% filter(.data$year == latest_year)
         prev_year <- suppressWarnings(max(df$year[df$year < latest_year], na.rm = TRUE))
         prev <- if (is.finite(prev_year)) df %>% filter(.data$year == prev_year) else NULL
         sales <- sum(latest$sales, na.rm = TRUE)
         prev_sales <- if (!is.null(prev) && nrow(prev)) sum(prev$sales, na.rm = TRUE) else NA_real_
         delta <- if (!is.na(prev_sales) && prev_sales != 0) (sales - prev_sales) / prev_sales else NA_real_
         formatted <- scales::label_number(scale_cut = scales::cut_short_scale())(sales)
         delta_txt <- if (is.na(delta)) "전년 데이터 부족" else paste0("전년 대비 ", scales::percent(delta, accuracy = 0.1))
         tagList(
            div(class = "diag-kpi-card",
                div(class = "kpi-label", paste0("최근 연도 매출 (", latest_year, ")")),
                div(class = "kpi-value", formatted),
                div(class = "kpi-sub", delta_txt)
            )
         )
      })

      output$fin_kpi_it <- renderUI({
         df <- fin_combined_df()
         fin_validate(fin_need(nrow(df) > 0, "데이터를 불러오세요"))
         latest_year <- max(df$year, na.rm = TRUE)
         latest <- df %>% filter(.data$year == latest_year)
         it <- mean(latest$inventory_turnover, na.rm = TRUE)
         it_txt <- if (is.na(it)) "자료 부족" else sprintf("%.1f배", it)
         diag <- values$diagnosis_res
         avg_txt <- if (!is.null(diag) && !is.na(diag$avg_turnover)) sprintf("업종 평균 %.1f배", diag$avg_turnover) else "업종 평균 정보 부족"
         div(
            class = "diag-kpi-card",
            div(class = "kpi-label", "재고자산회전율"),
            div(class = "kpi-value", it_txt),
            div(class = "kpi-sub", avg_txt)
         )
      })

      output$fin_kpi_roa <- renderUI({
         df <- fin_combined_df()
         fin_validate(fin_need(nrow(df) > 0, "데이터를 불러오세요"))
         latest_year <- max(df$year, na.rm = TRUE)
         latest <- df %>% filter(.data$year == latest_year)
         roa <- mean(latest$roa, na.rm = TRUE)
         roa_txt <- if (is.na(roa)) "자료 부족" else scales::percent(roa, accuracy = 0.1)
         diag <- values$diagnosis_res
         avg_txt <- if (!is.null(diag) && !is.na(diag$avg_margin)) {
            paste0("업종 평균 ", scales::percent(diag$avg_margin, accuracy = 0.1))
         } else "업종 평균 정보 부족"
         div(
            class = "diag-kpi-card",
            div(class = "kpi-label", "ROA"),
            div(class = "kpi-value", roa_txt),
            div(class = "kpi-sub", avg_txt)
         )
      })

      output$diag_status_copy <- renderUI({
         res <- values$diagnosis_res
         if (is.null(res)) {
            return(div(class = "diag-summary-card", div(class = "diag-summary-text", tags$p("데이터를 업로드하면 진단 결과를 요약합니다."))))
         }
         status_class <- if (identical(res$quadrant, "Dog")) "danger" else if (identical(res$quadrant, "Cash Cow")) "success" else "neutral"
         status_label <- if (identical(res$quadrant, "Dog")) "과재고 위험" else if (identical(res$quadrant, "Cash Cow")) "안정 구간" else "진단 중"
         meta <- paste0(
            "기준 연도 ", res$latest_year,
            " · 업종 평균 턴 ", sprintf("%.1f배", res$avg_turnover),
            " · 평균 이익률 ", scales::percent(res$avg_margin, accuracy = 0.1)
         )
         div(
            class = "diag-summary-card",
            tags$span(class = paste("diag-status-pill", status_class), status_label),
            div(
               class = "diag-summary-text",
               tags$h4(res$message),
               tags$p("재고 속도와 이익률을 동시에 점검해 최적 상태를 유지하세요."),
               div(class = "diag-summary-meta", meta)
            )
         )
      })

      output$diag_quadrant_label <- renderUI({
         res <- values$diagnosis_res
         fin_validate(fin_need(!is.null(res), "데이터를 불러오세요"))
         cls <- if (identical(res$quadrant, "Dog")) "diag-status-pill danger" else if (identical(res$quadrant, "Cash Cow")) "diag-status-pill success" else "diag-status-pill neutral"
         label_txt <- if (identical(res$quadrant, "Dog")) "과재고 위험 구간" else if (identical(res$quadrant, "Cash Cow")) "건강 구간" else "진단 정보 부족"
         tags$span(class = cls, label_txt)
      })

      output$diag_metrics <- renderUI({
         res <- values$diagnosis_res
         fin_validate(fin_need(!is.null(res), "데이터를 불러오세요"))
         turn_txt <- if (is.na(res$user_turnover)) "N/A" else sprintf("%.1f", res$user_turnover)
         turn_avg_txt <- if (is.na(res$avg_turnover)) "N/A" else sprintf("%.1f", res$avg_turnover)
         margin_txt <- ifelse(is.na(res$user_margin), "N/A", scales::percent(res$user_margin, accuracy = 0.1))
         margin_avg_txt <- scales::percent(res$avg_margin, accuracy = 0.1)
         msg_turn <- if (is.na(res$user_turnover)) {
            "재고 회전율 계산 불가"
         } else if (res$user_turnover < res$avg_turnover) {
            "재고 회전이 느립니다. 할인 판매를 고려하세요."
         } else {
            "재고 회전이 업종 평균 이상입니다."
         }
         msg_margin <- if (is.na(res$user_margin)) {
            "이익률 계산 불가"
         } else if (res$user_margin < res$avg_margin) {
            "이익률이 업종 평균보다 낮습니다. 마진 회복 액션이 필요합니다."
         } else {
            "이익률이 업종 평균 이상입니다."
         }
         tagList(
            div(
               class = "diag-metric-row",
               div(class = "metric-label", "재고 턴"),
               div(class = "metric-value", paste0(turn_txt, "회")),
               div(class = "metric-note", paste0("업종 평균 ", turn_avg_txt, "회 · ", msg_turn))
            ),
            div(
               class = "diag-metric-row",
               div(class = "metric-label", "이익률"),
               div(class = "metric-value", margin_txt),
               div(class = "metric-note", paste0("업종 평균 ", margin_avg_txt, " · ", msg_margin))
            )
         )
      })

      output$diag_status_copy <- renderUI({
         res <- values$diagnosis_res
         if (is.null(res)) {
            return(HTML("<b>데이터를 업로드하면 진단 결과를 요약합니다.</b>"))
         }
         detail <- paste0(
            "기준 연도: ", res$latest_year,
            " · 업종 평균 턴 ", sprintf("%.1f", res$avg_turnover),
            " / 이익률 ", scales::percent(res$avg_margin, accuracy = 0.1)
         )
         HTML(paste0(
            "<h3 style='text-align:center; font-weight:700;'>", res$message, "</h3>",
            "<div style='text-align:center; color:#666; margin-top:6px;'>", detail, "</div>"
         ))
      })

      output$diag_quadrant_label <- renderUI({
         res <- values$diagnosis_res
         fin_validate(fin_need(!is.null(res), "데이터를 불러오세요"))
         cls <- if (identical(res$quadrant, "Dog")) "diag-badge red" else "diag-badge green"
         label_txt <- if (identical(res$quadrant, "Dog")) "위험 구간 (과재고)" else "건강 구간 (효율)"
         sub_txt <- if (identical(res$quadrant, "Dog")) "재고가 돈을 잠식" else "재고·이익 양호"
         HTML(sprintf(
            "<div class='%s'><div class='diag-badge-title'>%s</div><div class='diag-badge-sub'>%s</div></div>",
            cls, label_txt, sub_txt
         ))
      })

      output$diag_metrics <- renderUI({
         res <- values$diagnosis_res
         fin_validate(fin_need(!is.null(res), "데이터를 불러오세요"))
         turn_txt <- if (is.na(res$user_turnover)) "N/A" else sprintf("%.1f", res$user_turnover)
         turn_avg_txt <- if (is.na(res$avg_turnover)) "N/A" else sprintf("%.1f", res$avg_turnover)
         margin_txt <- ifelse(is.na(res$user_margin), "N/A", scales::percent(res$user_margin, accuracy = 0.1))
         margin_avg_txt <- scales::percent(res$avg_margin, accuracy = 0.1)
         msg_turn <- if (is.na(res$user_turnover)) {
            "재고 회전율 계산 불가"
         } else if (res$user_turnover < res$avg_turnover) {
            "재고 회전이 느립니다. 할인 판매를 고려하세요."
         } else {
            "재고 회전이 업종 평균 이상입니다."
         }
         msg_margin <- if (is.na(res$user_margin)) {
            "이익률 계산 불가"
         } else if (res$user_margin < res$avg_margin) {
            "이익률이 업종 평균보다 낮습니다. 마진 회복 액션이 필요합니다."
         } else {
            "이익률이 업종 평균 이상입니다."
         }
         HTML(paste0(
            "<div class='diag-metrics'>",
            "<div class='diag-metric'><div class='label'>재고 턴</div><div class='value'>", turn_txt, "회</div><div class='hint'>(업종 평균 ", turn_avg_txt, ")</div><div class='note'>", msg_turn, "</div></div>",
            "<div class='diag-metric'><div class='label'>이익률</div><div class='value'>", margin_txt, "</div><div class='hint'>(업종 평균 ", margin_avg_txt, ")</div><div class='note'>", msg_margin, "</div></div>",
            "</div>"
         ))
      })

      ## Detail tab outputs (teammate work)
      output$detail_plot_1 <- renderPlotly({
         df <- fin_combined_df()
         fin_validate(fin_need(nrow(df) > 0, "데이터를 불러오세요"))

         my_df <- df %>% filter(.data$source == "My Company")
         if (nrow(my_df) == 0 && nrow(df) > 0) {
            src <- df$source[[1]]
            my_df <- df %>% filter(.data$source == src)
         }
         fin_validate(fin_need(nrow(my_df) > 0, "내 기업 또는 비교 대상을 불러오세요"))

         plot_ly() %>%
            add_lines(
               data = my_df,
               x = ~year, y = ~sales,
               name = "매출",
               line = list(color = "#1f77b4")
            ) %>%
            add_lines(
               data = my_df,
               x = ~year, y = ~inventory,
               name = "재고",
               line = list(color = "#ff7f0e", dash = "dash")
            ) %>%
            layout(
               xaxis = list(title = "연도", dtick = 1),
               yaxis = list(title = "금액", tickformat = "~s"),
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
         if (nrow(my_df) == 0) {
            src <- fin_pick_source(df)
            my_df <- df %>%
               filter(.data$source == src) %>%
               arrange(.data$year) %>%
               mutate(
                  sales_growth = (sales / dplyr::lag(sales)) - 1,
                  inv_ratio = if_else(sales > 0, inventory / sales, NA_real_)
               )
         }
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
            my_df <- df %>% filter(.data$source == fin_pick_source(df))
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

      output$detail_plot_4 <- renderPlotly({
         res <- fin_forecast_result()
         df <- res$history %>% transmute(year, value = sales)
         fc <- res$forecast %>% transmute(year, value = yhat)
         fin_validate(fin_need(nrow(df) > 0, "데이터를 불러오세요"))
         fin_validate(fin_need(nrow(fc) > 0, "왼쪽 사이드바에서 '예측 실행'을 눌러 주세요."))
         combined <- bind_rows(
            mutate(df, type = "실제"),
            mutate(fc, type = "예측")
         ) %>% arrange(.data$year)

         plot_ly(
            data = combined,
            x = ~year, y = ~value, color = ~type,
            type = "scatter", mode = "lines+markers"
         ) %>%
            layout(
               yaxis = list(title = "매출액", tickformat = "~s"),
               xaxis = list(dtick = 1, title = "연도"),
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

      action_plan_calc <- reactive({
         df <- fin_combined_df()
         if (is.null(df) || nrow(df) == 0) return(NULL)
         latest_year <- max(df$year, na.rm = TRUE)
         latest <- df %>% filter(.data$year == latest_year)
         current_inventory <- sum(latest$inventory, na.rm = TRUE)
         current_sales <- sum(latest$sales, na.rm = TRUE)
         cogs <- sum(latest$cogs, na.rm = TRUE)
         cost_rate <- if (!is.na(current_sales) && current_sales != 0) cogs / current_sales else NA_real_
         fc_first <- tryCatch({
            res <- fin_forecast_result()
            res$forecast %>% arrange(.data$year) %>% slice_head(n = 1)
         }, error = function(e) NULL)
         expected_sales <- if (!is.null(fc_first)) fc_first$yhat[[1]] else if (is.finite(current_sales)) current_sales * (1 + growth_target()) else NA_real_
         target_turn <- turn_target()
         if (!is.finite(target_turn) || target_turn <= 0) target_turn <- 1
         target_inventory <- if (is.finite(expected_sales)) expected_sales / target_turn else NA_real_
         reduction <- if (is.finite(current_inventory) && is.finite(target_inventory)) current_inventory - target_inventory else NA_real_
         cash_gain <- if (is.finite(reduction) && is.finite(cost_rate)) reduction * cost_rate else NA_real_
         list(
            target_inventory = target_inventory,
            reduction = reduction,
            cash_gain = cash_gain,
            cost_rate = cost_rate,
            expected_sales = expected_sales,
            current_inventory = current_inventory,
            latest_year = latest_year
         )
      })
      observeEvent(action_plan_calc(), {
         values$action_plan <- action_plan_calc()
      })

      output$action_summary_card <- renderUI({
         plan <- action_plan_calc()
         fin_validate(fin_need(!is.null(plan), "데이터를 불러오세요"))
         reduction <- plan$reduction
         status <- if (is.null(reduction) || !is.finite(reduction)) {
            "neutral"
         } else if (reduction > 0) {
            "danger"
         } else if (reduction < 0) {
            "success"
         } else {
            "neutral"
         }
         status_label <- switch(
            status,
            danger = "과재고 정리 필요",
            success = "재고 추가 확보",
            "재고 정상 범위"
         )
         desc <- "목표 재고회전율과 매출 성장률에 따른 발주/현금 영향을 요약합니다."
         meta <- paste0(
            "목표 턴 ", if (!is.null(input$target_turn)) input$target_turn else "N/A",
            "회 · 성장 목표 ", if (!is.null(input$target_growth)) input$target_growth else 0, "%",
            if (!is.null(plan$latest_year)) paste0(" · 기준 연도 ", plan$latest_year) else ""
         )
         div(
            class = "action-summary-card",
            tags$span(class = paste("action-status-pill", status), status_label),
            div(
               class = "action-summary-text",
               tags$h4("현재 시나리오 요약"),
               tags$p(desc),
               div(class = "action-summary-meta", meta)
            )
         )
      })

      output$action_kpi_target <- renderUI({
         plan <- action_plan_calc()
         fin_validate(fin_need(!is.null(plan), "데이터를 불러오세요"))
         val_txt <- if (is.null(plan$target_inventory) || is.na(plan$target_inventory)) {
            "데이터 필요"
         } else {
            scales::label_number(scale_cut = scales::cut_short_scale())(plan$target_inventory)
         }
         div(
            class = "action-kpi-card",
            div(class = "kpi-label", "목표 재고"),
            div(class = "kpi-value", val_txt),
            div(class = "kpi-sub", "예상 매출 ÷ 목표 재고회전율")
         )
      })

      output$action_kpi_gap <- renderUI({
         plan <- action_plan_calc()
         fin_validate(fin_need(!is.null(plan), "데이터를 불러오세요"))
         reduction <- plan$reduction
         if (!is.null(reduction) && is.finite(reduction)) {
            label <- if (reduction > 0) "감축 필요 재고" else "추가 필요 재고"
            val_txt <- scales::label_number(scale_cut = scales::cut_short_scale())(abs(reduction))
         } else {
            label <- "재고 조정"
            val_txt <- "데이터 필요"
         }
         div(
            class = "action-kpi-card",
            div(class = "kpi-label", label),
            div(class = "kpi-value", val_txt),
            div(class = "kpi-sub", "현재 재고와 목표 재고의 차이")
         )
      })

      output$action_kpi_cash <- renderUI({
         plan <- action_plan_calc()
         fin_validate(fin_need(!is.null(plan), "데이터를 불러오세요"))
         gain <- plan$cash_gain
         val_txt <- if (is.null(gain) || is.na(gain)) {
            "데이터 필요"
         } else {
            paste0(ifelse(gain > 0, "+", ""), scales::label_number(scale_cut = scales::cut_short_scale())(gain))
         }
         div(
            class = "action-kpi-card",
            div(class = "kpi-label", "예상 현금 흐름 변화"),
            div(class = "kpi-value", val_txt),
            div(class = "kpi-sub", "재고 조정 후 확보/필요 자금")
         )
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

      make_pred_ts_plot <- function() {
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
      }
      output$pred_ts_plot <- renderPlotly(make_pred_ts_plot())
      output$pred_ts_plot_plus <- renderPlotly(make_pred_ts_plot())

      make_pred_comp_plot <- function() {
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
      }

      output$pred_comp_plot <- renderPlotly(make_pred_comp_plot())
      output$pred_comp_plot_plus <- renderPlotly(make_pred_comp_plot())

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
      output$pred_fc_error_plot_plus <- renderPlotly({
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

      output$pred_error_box <- renderPlotly({
         res <- fin_forecast_result()
         fitted <- res$fitted
         fin_validate(fin_need(nrow(fitted) > 0, "잔차를 계산할 데이터가 부족합니다. 예측을 다시 실행하세요."))
         plot_ly(fitted, y = ~resid, type = "box", name = "예측 오차", boxpoints = "all", jitter = 0.3) %>%
            layout(
               yaxis = list(title = "예측 - 실제", tickformat = "~s"),
               margin = list(t = 20)
            )
      })
      output$pred_error_box_note <- renderText({
         res <- fin_forecast_result()
         fitted <- res$fitted
         fin_validate(fin_need(nrow(fitted) > 0, "잔차를 계산할 데이터가 부족합니다. 예측을 다시 실행하세요."))
         resid_vals <- fitted$resid
         valid <- resid_vals[is.finite(resid_vals)]
         fin_validate(fin_need(length(valid) > 1, "잔차 표본이 부족합니다. 예측을 다시 실행하세요."))
         med <- stats::median(valid)
         iqr_val <- stats::IQR(valid)
         fmt <- scales::label_number(scale_cut = scales::cut_short_scale())
         paste0(
            "잔차 중앙값 ", fmt(med),
            ", 중앙 50% 폭 ", fmt(iqr_val),
            " 수준입니다. 0 근처면 모델 편향이 적습니다."
         )
      })

      output$pred_cum_plot <- renderPlotly({
         res <- fin_forecast_result()
         hist_df <- res$history %>% arrange(.data$year) %>% mutate(value = cumsum(.data$sales), type = "실제 누적")
         fc_df <- res$forecast %>% arrange(.data$year) %>% mutate(value = cumsum(.data$yhat), type = "예측 누적")
         fin_validate(fin_need(nrow(hist_df) > 0, "데이터를 불러오세요"))
         fin_validate(fin_need(nrow(fc_df) > 0, "예측을 실행하면 누적값을 볼 수 있습니다."))
         combined <- bind_rows(
            hist_df %>% select(.data$year, .data$value, .data$type),
            fc_df %>% select(.data$year, .data$value, .data$type)
         )
         plot_ly(combined, x = ~year, y = ~value, color = ~type, type = "scatter", mode = "lines+markers") %>%
            layout(
               xaxis = list(title = "연도", dtick = 1),
               yaxis = list(title = "누적 매출", tickformat = "~s"),
               margin = list(t = 20)
            )
      })
      output$pred_cum_note <- renderText({
         res <- fin_forecast_result()
         hist_df <- res$history %>% arrange(.data$year) %>% mutate(value = cumsum(.data$sales))
         fc_df <- res$forecast %>% arrange(.data$year) %>% mutate(value = cumsum(.data$yhat))
         fin_validate(fin_need(nrow(hist_df) > 0, "데이터를 불러오세요"))
         fin_validate(fin_need(nrow(fc_df) > 0, "예측을 실행하면 누적값을 볼 수 있습니다."))
         actual_last <- tail(hist_df$value, 1)
         forecast_last <- tail(fc_df$value, 1)
         fin_validate(fin_need(all(is.finite(c(actual_last, forecast_last))), "누적 값을 계산할 수 없습니다."))
         diff <- forecast_last - actual_last
         fmt <- scales::label_number(scale_cut = scales::cut_short_scale())
         if (abs(diff) < 1e-6) return("예측 누적 매출이 실제와 거의 동일한 수준입니다.")
         direction <- if (diff > 0) "높습니다" else "낮습니다"
         paste0("예측 누적 매출이 실제 대비 ", direction, " (차이 약 ", fmt(abs(diff)), ").")
      })

      output$pred_error_hist <- renderPlotly({
         res <- fin_forecast_result()
         fitted <- res$fitted
         fin_validate(fin_need(nrow(fitted) > 0, "잔차를 계산할 데이터가 부족합니다. 예측을 다시 실행하세요."))
         plot_ly(fitted, x = ~resid, type = "histogram", nbinsx = min(12, nrow(fitted))) %>%
            layout(
               xaxis = list(title = "예측 - 실제", tickformat = "~s"),
               yaxis = list(title = "빈도"),
               margin = list(t = 20)
            )
      })
      output$pred_error_hist_note <- renderText({
         res <- fin_forecast_result()
         fitted <- res$fitted
         fin_validate(fin_need(nrow(fitted) > 0, "잔차를 계산할 데이터가 부족합니다. 예측을 다시 실행하세요."))
         resid_vals <- fitted$resid
         valid <- resid_vals[is.finite(resid_vals)]
         fin_validate(fin_need(length(valid) > 0, "잔차 데이터가 부족합니다. 예측을 다시 실행하세요."))
         pos_share <- mean(valid > 0)
         neg_share <- mean(valid < 0)
         zero_share <- 1 - pos_share - neg_share
         paste0(
            "실제보다 높게 추정한 구간 ", scales::percent(pos_share, accuracy = 1),
            ", 낮게 추정한 구간 ", scales::percent(neg_share, accuracy = 1),
            if (zero_share > 0.01) paste0(", 거의 일치 ", scales::percent(zero_share, accuracy = 1)) else "",
            " 수준입니다."
         )
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
      observeEvent(input$apply_targets, {
         target_values$turn <- if (!is.null(input$target_turn)) input$target_turn else target_values$turn
         target_values$growth <- if (!is.null(input$target_growth)) input$target_growth / 100 else target_values$growth
         update_step_status(3, TRUE)
         showNotification("목표가 적용되었습니다. 리스크/경고 메시지를 다시 계산합니다.", type = "message", duration = 4)
      })

      pred_highlights <- reactive({
         res <- fin_forecast_result()
         fc <- res$forecast %>% arrange(.data$year)
         fin_validate(fin_need(nrow(fc) > 0, "예측 결과가 없습니다."))
         latest <- fc %>% slice_tail(n = 1)
         hist_last <- res$history %>% arrange(.data$year) %>% slice_tail(n = 1)
         current_sales <- if (nrow(hist_last)) hist_last$sales[[1]] else NA_real_
         growth <- if (!is.na(current_sales) && current_sales != 0) {
            (latest$yhat[[1]] - current_sales) / current_sales
         } else NA_real_
         band_ratio <- median((fc$yhat_upper - fc$yhat_lower) / fc$yhat, na.rm = TRUE)
         df <- fin_combined_df()
         focus <- pred_focus_source()
         latest_year_actual <- max(df$year, na.rm = TRUE)
         inv_turn <- df %>%
            filter(.data$source == focus, .data$year == latest_year_actual) %>%
            summarize(val = mean(.data$inventory_turnover, na.rm = TRUE), .groups = "drop") %>%
            pull(.data$val)
         warn_label <- "안심 수준"
         warn_level <- "safe"
         if (!is.na(band_ratio) && band_ratio > growth_target()) {
            warn_label <- "밴드 폭 경고"
            warn_level <- "danger"
         } else if (!is.na(growth) && growth < -0.05) {
            warn_label <- "매출 감소 우려"
            warn_level <- "warn"
         } else if (!is.na(inv_turn) && inv_turn < turn_target()) {
            warn_label <- "재고 턴 주의"
            warn_level <- "warn"
         }
         action <- if (!is.na(growth) && growth < -0.05) {
            "재고·비용 축소"
         } else if (!is.na(inv_turn) && inv_turn < turn_target()) {
            "재고 턴 회복 우선"
         } else if (!is.na(growth) && growth > 0.1) {
            "재고 선제 확보"
         } else {
            "보합: 재고 점검"
         }
         band_label <- if (is.na(band_ratio)) "데이터 필요" else paste0("±", scales::percent(band_ratio / 2, accuracy = 1))
         forecast_label <- ifelse(is.finite(latest$yhat[[1]]), paste0(round(latest$yhat[[1]] / 1e8, 1), "억"), "자료 부족")
         growth_txt <- if (is.na(growth)) "전년 대비 수치는 부족합니다." else paste0("전년 대비 ", scales::percent(growth, accuracy = 0.1), " 수준입니다.")
         band_sentence <- if (is.na(band_ratio)) "불확실성 정보가 부족합니다." else paste0("예측 불확실성은 ", band_label, " 입니다.")
         headline <- if (is.na(growth)) {
            "예측 데이터 확인 필요"
         } else if (growth > 0.1) {
            "가파른 성장 전망"
         } else if (growth < -0.05) {
            "감소 위험 신호"
         } else {
            "완만한 성장 전망"
         }
         history_years <- if (!is.null(res$history$year)) range(res$history$year, na.rm = TRUE) else c(NA, NA)
         meta <- if (all(is.finite(history_years))) {
            paste0("학습 기간 ", history_years[1], "–", history_years[2], " · 예측 기간 ", length(unique(fc$year)), "년")
         } else {
            "학습/예측 기간 정보를 확인하세요."
         }
         list(
            year = latest$year[[1]],
            forecast_label = forecast_label,
            warn_label = warn_label,
            warn_level = warn_level,
            action_label = action,
            band_label = band_label,
            summary_sentence = paste0(latest$year, "년 예상 매출은 약 ", forecast_label, "이며 ", growth_txt, " ", band_sentence),
            meta_text = meta,
            headline = headline,
            growth = growth
         )
      })

      output$pred_summary_card <- renderUI({
         metrics <- pred_highlights()
         cls <- switch(metrics$warn_level,
                       danger = "pred-status-pill danger",
                       warn = "pred-status-pill warn",
                       safe = "pred-status-pill safe",
                       "pred-status-pill neutral")
         div(
            class = "pred-summary-card",
            tags$span(class = cls, metrics$warn_label),
            div(
               class = "pred-summary-text",
               tags$h4(metrics$headline),
               tags$p(metrics$summary_sentence),
               div(class = "pred-summary-meta", metrics$meta_text)
            )
         )
      })

      output$pred_kpi_forecast <- renderUI({
         metrics <- pred_highlights()
         div(
            class = "pred-kpi-card",
            div(class = "kpi-label", paste0(metrics$year, " 예상 매출")),
            div(class = "kpi-value", metrics$forecast_label),
            div(class = "kpi-sub", "전망 기준: Prophet 예측 결과")
         )
      })

      output$pred_kpi_signal <- renderUI({
         metrics <- pred_highlights()
         div(
            class = "pred-kpi-card",
            div(class = "kpi-label", "예측 신호"),
            div(class = "kpi-value", metrics$warn_label),
            div(class = "kpi-sub", "회전율 · 성장률 · 밴드 폭 기준")
         )
      })

      output$pred_kpi_band <- renderUI({
         metrics <- pred_highlights()
         div(
            class = "pred-kpi-card",
            div(class = "kpi-label", "예측 불확실성"),
            div(class = "kpi-value", metrics$band_label),
            div(class = "kpi-sub", "밴드 폭 / 예상치 대비 비율")
         )
      })

      output$pred_action_chip <- renderUI({
         metrics <- pred_highlights()
         tags$span(class = "pred-action-chip", metrics$action_label)
      })

      output$pred_accuracy <- renderTable({
         res <- fin_forecast_result()
         fitted <- res$fitted
         fin_validate(fin_need(nrow(fitted) > 0, "정확도 계산을 위한 학습 데이터가 부족합니다."))
         mae <- mean(abs(fitted$resid), na.rm = TRUE)
         mape <- mean(abs(fitted$resid / fitted$actual), na.rm = TRUE)
         last_resid <- fitted %>% arrange(desc(.data$year)) %>% slice_head(n = 1) %>% pull(.data$resid)
         fmt_amount <- function(val) {
            if (is.na(val)) return("자료 부족")
            paste0(scales::number(val / 1e8, accuracy = 0.01), " 억 원")
         }
         fmt_accuracy <- function(val) {
            if (is.na(val)) return("자료 부족")
            paste0(scales::number((1 - val) * 100, accuracy = 0.1), "%")
         }
         tibble(
            Metric = c("평균 오차 수량", "정확도 (%)", "최근 연도 잔차"),
            Value = c(fmt_amount(mae), fmt_accuracy(mape), fmt_amount(last_resid))
         )
      })

      pred_summary_text <- reactive({
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
      output$pred_summary <- renderText(pred_summary_text())
      output$pred_summary_plus <- renderText(pred_summary_text())

      pred_detail_1_text <- reactive({
         res <- fin_forecast_result()
         yr_range <- range(res$history$year, na.rm = TRUE)
         fin_validate(fin_need(all(is.finite(yr_range)), "학습 데이터가 부족합니다."))
         paste0("학습 데이터: ", yr_range[1], "년 ~ ", yr_range[2], "년, 예측 기간: ", res$horizon, "년")
      })
      output$pred_detail_1 <- renderText(pred_detail_1_text())
      output$pred_detail_1_plus <- renderText(pred_detail_1_text())

      output$pred_interval_note <- renderText({
         res <- fin_forecast_result()
         fc <- res$forecast %>% arrange(.data$year)
         fin_validate(fin_need(nrow(fc) > 0, "예측 결과가 없습니다."))
         band_ratio <- median((fc$yhat_upper - fc$yhat_lower) / fc$yhat, na.rm = TRUE)
         if (is.na(band_ratio)) return("리본 폭을 계산할 수 없어 불확실성 안내가 제한됩니다.")
         paste0("평균 리본 폭 ", scales::percent(band_ratio, accuracy = 1), ". 이 구간을 벗어나면 이상 징후로 간주하세요.")
      })

      pred_detail_2_text <- reactive({
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
      output$pred_detail_2 <- renderText(pred_detail_2_text())
      output$pred_detail_2_plus <- renderText(pred_detail_2_text())

      pred_risk_data <- reactive({
         res <- fin_forecast_result()
         fc <- res$forecast %>% arrange(.data$year)
         hist_df <- res$history %>% arrange(.data$year)
         fin_validate(fin_need(nrow(fc) > 0, "예측 결과가 없습니다."))
         fin_validate(fin_need(nrow(hist_df) > 0, "과거 실측 데이터를 불러오세요."))
         hist_last <- hist_df %>% slice_tail(n = 1)
         growth <- if (!is.null(hist_last$sales) && !is.na(hist_last$sales) && hist_last$sales != 0) {
            (fc$yhat[1] - hist_last$sales) / hist_last$sales
         } else NA_real_
         band_ratio <- median((fc$yhat_upper - fc$yhat_lower) / fc$yhat, na.rm = TRUE)
         df <- fin_combined_df()
         focus <- pred_focus_source()
         latest_year <- max(df$year, na.rm = TRUE)
         inv_turn <- df %>%
            filter(.data$source == focus, .data$year == latest_year) %>%
            summarize(val = mean(.data$inventory_turnover, na.rm = TRUE), .groups = "drop") %>%
            pull(.data$val)
         list(
            band_ratio = band_ratio,
            growth = growth,
            hist_sales = hist_last$sales,
            hist_year = hist_last$year,
            forecast_first = fc$yhat[1],
            forecast_year = fc$year[1],
            inv_turn = inv_turn
         )
      })

      output$pred_risk_gauge <- renderPlotly({
         metrics <- pred_risk_data()
         fin_validate(fin_need(is.finite(metrics$band_ratio), "예측 폭 정보를 계산할 수 없습니다."))
         ratio <- max(metrics$band_ratio, 0)
         range_max <- if (ratio > 1) ratio * 1.1 else 1
         plot_ly(
            type = "indicator",
            mode = "gauge+number",
            value = ratio,
            number = list(valueformat = ".0%"),
            gauge = list(
               axis = list(range = list(0, range_max), tickformat = ".0%"),
               steps = list(
                  list(range = c(0, min(0.2, range_max)), color = "#b7e4c7"),
                  list(range = c(min(0.2, range_max), min(0.4, range_max)), color = "#f9e79f"),
                  list(range = c(min(0.4, range_max), range_max), color = "#f5b7b1")
               ),
               threshold = list(
                  line = list(color = "#c0392b", width = 4),
                  value = min(0.4, range_max)
               )
            ),
            domain = list(x = c(0, 1), y = c(0, 1)),
            title = list(text = "예측 폭 (리본 대비)")
         ) %>%
            layout(margin = list(t = 40, b = 0, l = 0, r = 0))
      })

      output$pred_growth_bar <- renderPlotly({
         metrics <- pred_risk_data()
         fin_validate(fin_need(all(is.finite(c(metrics$hist_sales, metrics$forecast_first))), "성장률 데이터를 계산할 수 없습니다."))
         df <- tibble(
            구분 = c(
               paste0(metrics$hist_year, "년 실제"),
               paste0(metrics$forecast_year, "년 예측")
            ),
            값 = c(metrics$hist_sales, metrics$forecast_first)
         )
         plot_ly(
            df,
            x = ~구분,
            y = ~값 / 1e8,
            type = "bar",
            text = ~paste0(round(값 / 1e8, 1), " 억"),
            textposition = "outside",
            hovertemplate = "%{x}: %{text}<extra></extra>",
            marker = list(color = c("#2980b9", "#1abc9c"))
         ) %>%
            layout(
               yaxis = list(title = "금액(억 원)"),
               margin = list(t = 40)
            )
      })

      output$pred_risk <- renderUI({
         metrics <- pred_risk_data()
         msgs <- c()
         if (!is.na(metrics$growth) && metrics$growth < -0.05) msgs <- c(msgs, "단기 매출 감소 위험이 있습니다.")
         if (!is.na(metrics$band_ratio) && metrics$band_ratio > 0.4) msgs <- c(msgs, "예측 구간이 넓어 불확실성이 높습니다.")
         if (!is.na(metrics$band_ratio) && metrics$band_ratio > growth_target()) msgs <- c(msgs, "밴드 폭이 목표 대비 커서 보수적 발주가 필요합니다.")
         if (!is.na(metrics$inv_turn) && metrics$inv_turn < turn_target()) msgs <- c(msgs, paste0("재고 턴 목표(", turn_target(), "회) 미달 상태입니다."))
         if (length(msgs) == 0) return(HTML("<p><strong>리스크:</strong> 중대한 위험 신호 없음.</p>"))
         HTML(paste0("<p><strong>리스크:</strong></p><ul>", paste(sprintf("<li>%s</li>", msgs), collapse = ""), "</ul>"))
      })

      output$pred_action <- renderUI({
         metrics <- pred_risk_data()
         if (!is.na(metrics$growth) && metrics$growth < -0.05) {
            return(HTML("<p><strong>액션:</strong> 비용/재고 축소, 프로모션·채널 전환으로 단기 수요를 방어하세요.</p>"))
         }
         if (!is.na(metrics$inv_turn) && metrics$inv_turn < turn_target()) {
            return(HTML("<p><strong>액션:</strong> 재고 턴 목표를 회복하기 위해 슬로우무버를 정리하고 발주량을 일시 축소하세요.</p>"))
         }
         if (!is.na(metrics$growth) && metrics$growth > 0.1) {
            return(HTML("<p><strong>액션:</strong> 매출 증가 예상. 리드타임 고려해 핵심 상품 재고를 선제 확보하세요.</p>"))
         }
         HTML("<p><strong>액션:</strong> 보합세 예상. 안전재고를 재점검하고 변동성이 큰 품목을 모니터링하세요.</p>")
      })

      pred_accuracy_metrics <- reactive({
         res <- fin_forecast_result()
         fitted <- res$fitted
         fin_validate(fin_need(nrow(fitted) > 0, "정확도 계산을 위한 학습 데이터가 부족합니다."))
         mae <- mean(abs(fitted$resid), na.rm = TRUE)
         mape <- mean(abs(fitted$resid / fitted$actual), na.rm = TRUE)
         last_resid <- fitted %>% arrange(desc(.data$year)) %>% slice_head(n = 1) %>% pull(.data$resid)
         list(mae = mae, mape = mape, last_resid = last_resid)
      })

      output$pred_accuracy_plot <- renderPlotly({
         metrics <- pred_accuracy_metrics()
         accuracy_df <- tibble(
            metric = c("평균 오차 수량(억)", "최근 잔차(억)", "정확도 (%)"),
            value = c(metrics$mae / 1e8, metrics$last_resid / 1e8, metrics$mape * 100),
            unit = c("억", "억", "%")
         )
         plot_ly(
            accuracy_df,
            x = ~metric,
            y = ~value,
            type = "bar",
            text = ~ifelse(unit == "%", paste0(round(value, 1), "%"), paste0(round(value, 1), " 억")),
            textposition = "auto",
            hovertemplate = "%{x}: %{text}<extra></extra>",
            marker = list(color = c("#5dade2", "#48c9b0", "#f7dc6f"))
         ) %>%
            layout(
               yaxis = list(title = "값 (억/%)"),
               margin = list(t = 40)
            )
      })

      output$pred_accuracy_note <- renderText({
         metrics <- pred_accuracy_metrics()
         fmt_num <- function(v, unit) {
            if (unit == "%") paste0(round(v, 1), "%") else paste0(round(v / 1e8, 1), " 억 원")
         }
         mae_txt <- fmt_num(metrics$mae, "억")
         mape_txt <- fmt_num(metrics$mape * 100, "%")
         resid_txt <- fmt_num(metrics$last_resid, "억")
         paste0("평균 오차 수량 ", mae_txt, ", 최근 잔차 ", resid_txt, ", 정확도 ", mape_txt, " 입니다.")
      })
      pred_sku_summary <- reactive({ NULL })
      pred_channel_summary <- reactive({ NULL })
      observe({
         df <- pred_channel_summary()
         if (!is.null(df) && nrow(df) > 0) update_step_status(3, TRUE)
      })
      output$sku_table <- renderDT({ NULL })
      output$channel_bar <- renderPlotly(plotly_empty())
      # 메모 입력 제거 (no-op)
      output$report_csv <- downloadHandler(
         filename = function() "report.csv",
         content = function(file) {
            df <- tryCatch(fin_combined_df(), error = function(e) NULL)
            if (is.null(df) || nrow(df) == 0) {
               readr::write_csv(tibble(message = "데이터가 없습니다."), file)
               return()
            }
            readr::write_csv(df, file)
         }
      )
      output$report_pdf <- downloadHandler(
         filename = function() "report.pdf",
         content = function(file) {
            df <- tryCatch(fin_combined_df(), error = function(e) NULL)
            diag <- tryCatch(values$diagnosis_res, error = function(e) NULL)
            fc <- tryCatch(values$forecast_res, error = function(e) NULL)
            grDevices::pdf(file, width = 8.5, height = 11)
            par(mar = c(1, 1, 1, 1))
            plot.new()
            text(0.5, 0.92, "재고·매출 요약 리포트", cex = 1.4, font = 2)
            y_pos <- 0.82
            if (!is.null(diag)) {
               lines <- c(
                  paste0("포지션: ", ifelse(identical(diag$quadrant, "Dog"), "위험 구간 (과재고)", "건강 구간 (효율)")),
                  paste0("재고 턴: ", ifelse(is.na(diag$user_turnover), "-", sprintf("%.1f회", diag$user_turnover)),
                         " / 업종 평균 ", ifelse(is.na(diag$avg_turnover), "-", sprintf("%.1f회", diag$avg_turnover))),
                  paste0("이익률: ", ifelse(is.na(diag$user_margin), "-", scales::percent(diag$user_margin, accuracy = 0.1)),
                         " / 업종 평균 ", scales::percent(diag$avg_margin, accuracy = 0.1))
               )
               for (ln in lines) {
                  text(0.05, y_pos, ln, adj = 0, cex = 1)
                  y_pos <- y_pos - 0.05
               }
            }
            if (!is.null(fc) && !is.null(fc$forecast)) {
               fc_tbl <- fc$forecast %>% arrange(.data$year)
               first_fc <- fc_tbl %>% slice_head(n = 1)
               band_ratio <- median((fc_tbl$yhat_upper - fc_tbl$yhat) / fc_tbl$yhat, na.rm = TRUE)
               lines <- c(
                  paste0("예상 매출(", first_fc$year, "): ", scales::label_number(scale_cut = scales::cut_short_scale())(first_fc$yhat)),
                  paste0("불확실성 폭: ", ifelse(is.na(band_ratio), "-", scales::percent(band_ratio, accuracy = 0.1)))
               )
               for (ln in lines) {
                  text(0.05, y_pos, ln, adj = 0, cex = 1)
                  y_pos <- y_pos - 0.05
               }
            }
            if (!is.null(df) && nrow(df)) {
               latest_year <- max(df$year, na.rm = TRUE)
               latest <- df %>% filter(.data$year == latest_year)
               sales_txt <- scales::label_number(scale_cut = scales::cut_short_scale())(sum(latest$sales, na.rm = TRUE))
               it_txt <- round(mean(latest$inventory_turnover, na.rm = TRUE), 2)
               text(0.05, y_pos, paste0("최근 연도(", latest_year, ") 매출: ", sales_txt, " / 재고 턴: ", it_txt), adj = 0)
            }
            grDevices::dev.off()
         }
      )

      ## 4. Monthly update ------------------------
      output$MonthlyUpdate <- 
         renderUI({
            tags$iframe(
               src = SNZ_link,
               seamless = "seamless",
               frameborder = 0,
               height="800", width="100%")
         })
   }
