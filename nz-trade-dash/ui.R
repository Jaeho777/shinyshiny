####
library(shinydashboard)
library(shiny)
library(dplyr)
library(tidyr)
library(ggplot2)
library(highcharter)
library(plotly)
library(lubridate)
library(stringr)
library(withr)
library(treemap)
library(DT)
library(shinyBS)
library(shinyjs)
library(WDI)
library(geosphere)
library(magrittr)
library(shinycssloaders)
options(spinner.color = "#006272")
library(timevis)

## load data
load("list_snz_commodity_ex.rda") ## pre-defined commodity list form SNZ
load("list_snz_commodity_im.rda") ## pre-defined commodity list form SNZ
load("list_country.rda") ## Country grouped by region
load("dtf_shiny_commodity_service_ex.rda") ## principle commodity from StatsNZ -- exports

## setup global variables
maxYear <- tolower(paste0(dtf_shiny_commodity_service_ex$Note[1], " ", max(dtf_shiny_commodity_service_ex$Year)))
maxYear <- gsub("q1", "March", maxYear)
maxYear <- gsub("q2", "June", maxYear)
maxYear <- gsub("q3", "September", maxYear)
maxYear <- gsub("q4", "December", maxYear)

maxYear_lb <- gsub("year ended March", "Mar", maxYear)
maxYear_lb <- gsub("year ended June", "Jun", maxYear_lb)
maxYear_lb <- gsub("year ended September", "Sep", maxYear_lb)
maxYear_lb <- gsub("year ended December", "Dec", maxYear_lb)

maxYear_lb <- paste0(
  substr(maxYear_lb, 1, 3),
  " ",
  substr(maxYear_lb, nchar(maxYear_lb) - 1, nchar(maxYear_lb))
)


## load functions
source("helper_funs.R")


## build ui.R -----------------------------------
## 1. header -------------------------------
header <-
  dashboardHeader(
    title = HTML("패션 재고관리 예측 시스템"),
    titleWidth = 250,
    tags$li(
      class = "dropdown",
      actionLink("go_dashboard", icon("dashboard"))
    )
  )

## 2. siderbar ------------------------------
siderbar <-
	  dashboardSidebar(
	    width = 200,
	    sidebarMenu(
	      id = "sidebar",
	      selected = NULL,
	      ## 1st tab: Financial benchmarking 메인 대시보드 -----------
	      menuItem(
	        "Main Dashboard",
	        tabName = "dashboard",
	        icon = icon("line-chart")
	      ),
	      ## 2nd tab: 내 기업 상세 분석 (detail graph)
	      menuItem(
	        "내 기업 상세 분석",
	        tabName = "detail_graph",
	        icon = icon("info-circle")
	      ),
	      ## 3rd tab: 예측 결과 (Prophet)
	      menuItem(
	        "예측 분석",
	        tabName = "prediction_graph",
	        icon = icon("line-chart")
	      ),
	      menuItem("분석 탭", tabName = "analysis_graph", icon = icon("bar-chart")),
	      menuItem("예측 탭", tabName = "prediction_graph", icon = icon("line-chart")),
	      ## Financial benchmarking settings panel (shown when Main Dashboard is active)
	      div(
	        id = "sidebar_fin_bench",
	        conditionalPanel(
	          "input.sidebar === 'dashboard' || input.sidebar === 'detail_graph' || input.sidebar === 'prediction_graph' || input.sidebar === 'analysis_graph' || input.sidebar === 'prediction_graph'",
	          tags$hr(),
	          h4("설정"),
	          textInput("fin_corp_query", "상장사 검색", placeholder = "예: 한섬, 020000"),
	          actionButton("fin_corp_search", "검색"),
	          selectInput("fin_corp_pick", "상장사 선택", choices = c(), selected = NULL),
	          actionButton("fin_fetch_dart", "DART 불러오기"),
	          actionButton("fin_load_demo", "데모 데이터 로드"),
	          hr(),
	          fileInput("fin_upload", "내 가게 파일 업로드", accept = c(".xlsx", ".xls", ".csv")),
	          uiOutput("fin_mapping_ui"),
	          hr(),
	          numericInput("fin_forecast_y", "예측 연도 수", value = 3, min = 1, max = 5),
	          actionButton("fin_do_forecast", "예측 실행", class = "btn-primary")
	        )
	      )
	    )
	  )

## 3. body --------------------------------
body <- dashboardBody(
  ## 3.0. CSS styles in header ----------------------------
  tags$head(
    # ## JS codes
    # tags$script(src = "fixedElement.js" ),
    # tags$style(HTML(".scroller_anchor{height:0px; margin:0; padding:0;};
    #                  .scroller{background: white;
    #                   border: 1px solid #CCC;
    #                   margin:0 0 10px;
    #                   z-index:100;
    #                   height:50px;
    #                   font-size:18px;
    #                   font-weight:bold;
    #                   text-align:center;
    #                  width:500px;}")),

    # tags$script(src = "world.js" ),
    tags$script("document.title = 'New Zealand Trade Intelligence Dashboard'"),

	    ### Styles
	    tags$style(HTML("
	      .small-box {
	        height: 80px;
	        padding-top: 6px;
	        padding-bottom: 4px;
	      }
	      .small-box h3 {
	        margin: 0;
	        line-height: 1.15;
	      }
	    ")),
    tags$style(HTML(".fa { font-size: 35px; }")),
    tags$style(HTML(".glyphicon { font-size: 33px; }")), ## use glyphicon package
    tags$style(HTML(".fa-dashboard { font-size: 20px; }")),
    tags$style(HTML(".fa-globe { font-size: 20px; }")),
    tags$style(HTML(".fa-barcode { font-size: 20px; }")),
    tags$style(HTML(".tab-content { padding-left: 20px; padding-right: 30px; }")),
    tags$style(HTML(".fa-wrench { font-size: 15px; }")),
    tags$style(HTML(".fa-refresh { font-size: 15px; }")),
    tags$style(HTML(".fa-search { font-size: 15px; }")),
    tags$style(HTML(".fa-comment { font-size: 20px; }")),
    tags$style(HTML(".fa-share-alt { font-size: 20px; }")),
	    tags$style(HTML(".fa-envelope { font-size: 20px; }")),
	    tags$style(HTML(".fa-question-circle { font-size: 20px; }")),
	    tags$style(HTML(".fa-chevron-circle-down { font-size: 15px; }")),
	    tags$style(HTML(".fa-bell { font-size: 17px; }")),
	    tags$style(HTML(".fa-check { font-size: 14px; }")),
	    tags$style(HTML(".fa-times { font-size: 14px; }")),
	    ## 헤더 제목 왼쪽 정렬
	    tags$style(HTML("
	      .main-header .logo {
	        text-align: left;
	        padding-left: 15px;
	      }
	    ")),
	    ## 사이드바 메뉴를 조금 더 왼쪽으로
	    tags$style(HTML("
	      .main-sidebar .sidebar .sidebar-menu > li > a {
	        padding-left: 10px;
	      }
	    ")),
	    ## valueBox 텍스트 대비 강화
	    tags$style(HTML("
	      /* 기본 valueBox 스타일 */
	      .small-box h3 { color: #ffffff !important; }
	      .small-box p  { color: #111111 !important; }
	      /* Financial KPI 전용: 제목/값 모두 흰색, 제목을 위에 표시 */
	      .small-box .fin-kpi-title {
	        display: block;
	        color: #ffffff !important;
	        font-size: 12px;
	        font-weight: 400;
	        margin-bottom: 1px;
	      }
	      .small-box .fin-kpi-value {
	        display: block;
	        color: #ffffff !important;
	        font-size: 26px;
	        font-weight: 700;
	        margin-top: 0;
	      }
	    ")),

    # tags$style(HTML(".fa-twitter { font-size: 10px; color:red;}")),
    # tags$style(HTML(".fa-facebook { font-size: 10px; color:red;}")),
    # tags$style(HTML(".fa-google-plus { font-size: 10px; color:red;}")),
    # tags$style(HTML(".fa-pinterest-p { font-size: 10px; color:red;}")),
    # tags$style(HTML(".fa-linkedin { font-size: 10px; color:red;}")),
    # tags$style(HTML(".fa-tumblr { font-size: 10px; color:red;}")),
    tags$style(HTML("
      .viz-text-lg { font-size: 16px; line-height: 1.5; }
      .viz-kpi-sub { font-size: 12px; font-weight: 400; }
    ")),
    tags$style(HTML("
      .friendly-intro {
        background: #f7f9fc;
        border: 1px solid #e0e6f0;
        border-radius: 10px;
        padding: 12px 16px;
        margin-bottom: 14px;
        color: #0f294d;
        font-size: 15px;
      }
      .friendly-intro .pill-chip {
        display: inline-block;
        padding: 6px 10px;
        border-radius: 12px;
        background: #eef4ff;
        color: #0f294d;
        font-weight: 700;
        margin-right: 6px;
        margin-bottom: 6px;
        font-size: 13px;
      }
      .friendly-list {
        padding-left: 18px;
        font-size: 15px;
        margin-bottom: 0;
      }
      .friendly-list li { margin-bottom: 6px; }
      .impact-box .small-box {
        min-height: 120px;
        border-radius: 10px;
        box-shadow: 0 4px 10px rgba(0,0,0,0.08);
      }
      .impact-box .small-box h3 { font-size: 30px; }
      .impact-box .small-box p  { font-size: 15px; }
      .assist-text {
        font-size: 14px;
        color: #3c4a64;
      }
    ")),

	    ## modify the dashboard's skin color (palette 1)
	    tags$style(HTML("
	                       /* logo */
	                       .skin-blue .main-header .logo {
	                       background-color: #1F3A93;
	                       }

	                       /* logo when hovered */
	                       .skin-blue .main-header .logo:hover {
	                       background-color: #1F3A93;
	                       }

	                       /* navbar (rest of the header) */
	                       .skin-blue .main-header .navbar {
	                       background-color: #1F3A93;
	                       }

	                       /* active selected tab in the sidebarmenu */
	                       .skin-blue .main-sidebar .sidebar .sidebar-menu .active a{
	                       background-color: #1F3A93;
	                                 }
	                       ")),

	    ## override valueBox background colors for palette 1
	    tags$style(HTML("
	      .bg-blue {
	        background-color: #4A90E2 !important;
	      }
	      .bg-green {
	        background-color: #27AE60 !important;
	      }
	      .bg-yellow {
	        background-color: #F1C40F !important;
	      }
	    ")),

    ## modify icon size in the sub side bar menu
    tags$style(HTML("
                       /* change size of icons in sub-menu items */
                      .sidebar .sidebar-menu .treeview-menu>li>a>.fa {
                      font-size: 15px;
                      }

                      .sidebar .sidebar-menu .treeview-menu>li>a>.glyphicon {
                      font-size: 13px;
                      }

                      /* Hide icons in sub-menu items */
                      .sidebar .sidebar-menu .treeview>a>.fa-angle-left {
                      display: none;
                      }
                      ")),
    tags$style(HTML("hr {border-top: 1px solid #000000;}")),

    ## to not show error message in shiny
    tags$style(HTML(".shiny-output-error { visibility: hidden; }")),
    tags$style(HTML(".shiny-output-error:before { visibility: hidden; }")),

    ## heand dropdown menu size
    # tags$style(HTML('.navbar-custom-menu>.navbar-nav>li>.dropdown-menu { width:100px;}'))
    tags$style(HTML(".navbar-custom-menu>.navbar-nav>li:last-child>.dropdown-menu { width:10px; font-size:10px; padding:1px; margin:1px;}")),
    tags$style(HTML(".navbar-custom-menu> .navbar-nav> li:last-child > .dropdown-menu > h4 {width:0px; font-size:0px; padding:0px; margin:0px;}")),
    tags$style(HTML(".navbar-custom-menu> .navbar-nav> li:last-child > .dropdown-menu > p {width:0px; font-size:0px; padding:0px; margin:0px;}"))
  ),

  ## 3.1 Dashboard body --------------
  tabItems(
    ## Main dashboard: 재무 벤치마킹 ----------------------------------------------------------
    tabItem(
      tabName = "dashboard",
      ## contents for the dashboard tab
      div(
        id = "main_wait_message",
        h1("Note, initial load may take up to 10 seconds.",
          style = "color:darkblue", align = "center"
        ),
        tags$hr()
      ),

      # 1.1 Export/import board ---------------------------
      # div(class = 'scroller_anchor'),
      # div(class = 'scroller', ) ,

      h1(paste0("New Zealand trade for the ", maxYear)),
      fluidRow(
        valueBoxOutput("ExTotBox") %>% withSpinner(type = 4),
        valueBoxOutput("ImTotBox"),
        valueBoxOutput("BlTotBox")
      ),
      h2(paste0("Goods")),
      fluidRow(
        valueBoxOutput("ExGBox"),
        valueBoxOutput("ImGBox"),
        valueBoxOutput("BlGBox")
      ),
      h2(paste0("Services")),
      fluidRow(
        valueBoxOutput("ExSBox"),
        valueBoxOutput("ImSBox"),
        valueBoxOutput("BlSBox")
      ),

      ## 1.2 Time serise plot ----------------------------------------
      h2(paste0("New Zealand trade over the past 20 years")),
      fluidRow(
        column(width = 6, h4("Goods and services trade", align = "center"), highchartOutput("IEGSLineHc")),
        column(width = 6, h4("Trade balance", align = "center"), highchartOutput("GSTotalBalanceLineHc"))
      ),


      ## 1.3 Table shows growth rate ---------------------------------
      h2(paste0("Short, medium, and long term growth")),
      p("Compound annual growth rate (CAGR) for the past 1, 5, 10 and 20 years"),
      # fluidRow( h2(paste0("Short, medium, and long term growth")),
      #          p("Compound annual growth rate (CAGR) for the past 1, 5, 10 and 20 years") ),
      fluidRow(dataTableOutput("GrowthTab")),
      div(
        id = "message_to_show_more",
        tags$hr(),
        tags$h3("Click on the 'Show more details' button to display additional information on free trade agreements, and imports/exports by commodities and markets."),
        actionButton("btn_show_more",
          paste0(" Show more details"),
          icon = icon("chevron-circle-down"),
          style = "padding-top:3px; padding-bottom:3px;padding-left:5px;padding-right:5px;font-size:120% "
        )
      ),
      div(id = "show_more_detail"),
      shinyjs::hidden(div(
        id = "load_more_message",
        tags$hr(),
        tags$h1("Loading...", align = "center")
      ))
    ),

	    ## Financial benchmarking tab
	    tabItem(
	      tabName = "fin_bench",
	      h3("분석 결과"),
	      fluidRow(
	        box(
	          width = 12, status = "success", solidHeader = FALSE,
	          uiOutput("fin_kpi_row"),
	          br(),
	          textOutput("fin_summary"),
	          br(),
	          plotlyOutput("fin_ts_plot"),
	          plotlyOutput("fin_quad_plot"),
	          plotlyOutput("fin_fc_plot"),
	          tableOutput("fin_fc_table")
	        )
	      )
	    ),

    tabItem(
      tabName = "analysis_graph",
      h3("내 매장 vs 비슷한 업체 한눈 비교"),
      fluidRow(
        class = "impact-box",
        valueBoxOutput("analysis_insight_main", width = 4),
        valueBoxOutput("analysis_warning", width = 4),
        valueBoxOutput("analysis_action", width = 4)
      ),
      fluidRow(
        box(
          title = "이번 달 핵심 3줄 요약",
          width = 8,
          status = "primary",
          solidHeader = TRUE,
          uiOutput("analysis_top3"),
          div(class = "assist-text", "요약은 최신 연도와 선택된 기업을 기준으로 자동 작성됩니다.")
        ),
        box(
          title = "바로 확인/추천 행동",
          width = 4,
          status = "success",
          solidHeader = TRUE,
          uiOutput("analysis_actions_friendly"),
          div(class = "assist-text", "재고회전·ROA·매출 증감에 따라 제안이 달라집니다.")
        )
      ),
      fluidRow(
        box(
          title = "데이터 상태/안내",
          width = 4,
          status = "warning",
          solidHeader = TRUE,
          htmlOutput("analysis_quality", class = "viz-text-lg"),
          uiOutput("analysis_alerts", class = "viz-text-lg"),
          tags$hr(),
          div(class = "viz-text-lg", textOutput("analysis_desc_2")),
          div(class = "assist-text", "데모 데이터가 자동 채워져 있습니다. 업로드나 DART 불러오기로 교체 가능.")
        ),
        box(
          title = "주요 지표/매출 흐름",
          width = 8,
          status = "primary",
          solidHeader = TRUE,
          uiOutput("analysis_kpi_row"),
          plotlyOutput("analysis_plot_1"),
          div(class = "viz-text-lg", textOutput("analysis_delta_note"))
        )
      ),
      fluidRow(
        box(
          title = "재고 효율·이익 비교",
          width = 6,
          status = "success",
          solidHeader = TRUE,
          plotlyOutput("analysis_plot_2"),
          div(class = "viz-text-lg", textOutput("analysis_desc_1"))
        ),
        box(
          title = "재고/이익 위치 한눈에",
          width = 6,
          status = "info",
          solidHeader = TRUE,
          plotlyOutput("analysis_plot_3"),
          div(class = "viz-text-lg", textOutput("analysis_desc_3"))
        )
      )
    ),

    tabItem(
      tabName = "prediction_graph",
      h3("재고·매출 예측 (쉽게 보기)"),
      fluidRow(
        class = "impact-box",
        valueBoxOutput("pred_insight_main", width = 4),
        valueBoxOutput("pred_warning", width = 4),
        valueBoxOutput("pred_action_box", width = 4)
      ),
      fluidRow(
        box(
          title = "이번 달 알아두면 좋은 점",
          width = 8,
          status = "primary",
          solidHeader = TRUE,
          uiOutput("pred_top3"),
          div(class = "assist-text", "예측 추세·불확실성·학습 기간을 한 줄씩 정리했습니다.")
        ),
        box(
          title = "추천 행동/알림",
          width = 4,
          status = "success",
          solidHeader = TRUE,
          uiOutput("pred_action_simple"),
          div(class = "assist-text", "아래 품질/정확도 박스에서 데이터 상황을 함께 확인하세요.")
        )
      ),
      fluidRow(
        box(
          title = "앞으로 흐름(연도별)",
          width = 8,
          status = "primary",
          solidHeader = TRUE,
          plotlyOutput("pred_ts_plot"),
          div(class = "viz-text-lg", textOutput("pred_summary")),
          div(class = "viz-text-lg", textOutput("pred_detail_1")),
          div(class = "assist-text", "예측 구간(리본)을 함께 보며 여유 재고/부족 재고를 가늠하세요.")
        ),
        box(
          title = "예측 품질/안내",
          width = 4,
          status = "warning",
          solidHeader = TRUE,
          htmlOutput("pred_quality", class = "viz-text-lg"),
          tableOutput("pred_accuracy"),
          uiOutput("pred_risk", class = "viz-text-lg"),
          div(class = "viz-text-lg", textOutput("pred_detail_2"))
        )
      ),
      fluidRow(
        box(
          title = "예측 오차/폭",
          width = 12,
          status = "success",
          solidHeader = TRUE,
          plotlyOutput("pred_fc_error_plot"),
          div(class = "viz-text-lg", textOutput("pred_resid_note"))
        )
      )
    ),

    ## Detail tab: 내 기업 상세 분석 ----------------------------------------------------------
    tabItem(
      tabName = "detail_graph",
      h3("내 기업에 대한 자세한 설명"),
      fluidRow(
        valueBoxOutput("fin_kpi_sales"),
        valueBoxOutput("fin_kpi_it"),
        valueBoxOutput("fin_kpi_roa")
      ),
      br(),
      fluidRow(
        box(
          title = "내 기업 추이",
          width = 6,
          status = "primary",
          solidHeader = TRUE,
          plotlyOutput("detail_plot_1", height = "320px")
        ),
        box(
          title = "연도별 성장률 + 재고 비율",
          width = 6,
          status = "primary",
          solidHeader = TRUE,
          plotlyOutput("detail_plot_2", height = "320px")
        )
      ),
      fluidRow(
        box(
          title = "재무 구조",
          width = 6,
          status = "primary",
          solidHeader = TRUE,
          plotlyOutput("detail_plot_3", height = "320px")
        ),
        box(
          title = "추가 그래프",
          width = 6,
          status = "primary",
          solidHeader = TRUE,
          plotlyOutput("detail_plot_4", height = "320px")
        )
      ),
      fluidRow(
        box(
          title = "설명",
          width = 12,
          status = "primary",
          solidHeader = TRUE,
          htmlOutput("detail_desc_1"),
          br(),
          htmlOutput("detail_desc_2"),
          br(),
          htmlOutput("detail_desc_3")
        )
      ),
      fluidRow(
        box(
          title = "액션 플랜",
          width = 12,
          status = "primary",
          solidHeader = TRUE,
          htmlOutput("detail_action")
        )
      )
    ),

    ## Prediction tab: Prophet 기반 예측 결과 -----------------------------------------------
    tabItem(
      tabName = "prediction_graph",
      h3("Prophet 기반 예측 결과"),
      fluidRow(
        valueBoxOutput("fin_kpi_sales"),
        valueBoxOutput("fin_kpi_it"),
        valueBoxOutput("fin_kpi_roa")
      ),
      br(),
      fluidRow(
        box(
          title = "예측 시계열",
          width = 6,
          status = "primary",
          solidHeader = TRUE,
          plotlyOutput("pred_ts_plot", height = "320px")
        ),
        box(
          title = "Trend / 시즌 컴포넌트",
          width = 6,
          status = "primary",
          solidHeader = TRUE,
          plotlyOutput("pred_comp_plot", height = "320px")
        )
      ),
      fluidRow(
        box(
          title = "예측 오차 / 실제 비교",
          width = 6,
          status = "primary",
          solidHeader = TRUE,
          plotlyOutput("pred_fc_error_plot", height = "320px")
        ),
        box(
          title = "예측 오차 박스플롯",
          width = 6,
          status = "primary",
          solidHeader = TRUE,
          plotlyOutput("pred_error_box", height = "320px")
        )
      ),
      fluidRow(
        box(
          title = "누적 매출 (실제 vs 예측)",
          width = 12,
          status = "primary",
          solidHeader = TRUE,
          plotlyOutput("pred_cum_plot", height = "300px")
        )
      ),
      fluidRow(
        box(
          title = "요약 & 인사이트",
          width = 12,
          status = "primary",
          solidHeader = TRUE,
          textOutput("pred_summary"),
          br(),
          textOutput("pred_detail_1"),
          br(),
          textOutput("pred_detail_2")
        )
      )
    )
  )
)


## put UI together --------------------
ui <-
  dashboardPage(header, siderbar, body)
