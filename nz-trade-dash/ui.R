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
    disable = FALSE,
    titleWidth = 250,
    dropdownMenuCustom(
      type = "message",
      customSentence = customSentence,
      messageItem(
        from = "TR_SharedMailbox@mbie.govt.nz", #' Feedback and suggestions',
        message =  "", # paste0("TR_SharedMailbox@mbie.govt.nz" ),
        icon = icon("envelope"),
        href = "mailto:TR_SharedMailbox@mbie.govt.nz"
      ),
      icon = icon("comment")
    ),
    tags$li(class = "dropdown",
      actionLink("go_fin", icon("line-chart"))
    )
  )

## 2. siderbar ------------------------------
siderbar <-
	  dashboardSidebar(
	    width = 230,
    sidebarMenu(
      id = "sidebar",
      selected = "tab_data",
      style = "position: relative; overflow: visible;",
      menuItem("데이터 준비", tabName = "tab_data", icon = icon("upload")),
      menuItem("현황 진단", tabName = "tab_diagnosis", icon = icon("stethoscope")),
      menuItem("수요 예측", tabName = "tab_prediction", icon = icon("chart-line")),
      menuItem("발주/액션 플랜", tabName = "tab_action", icon = icon("clipboard-check")),
      menuItem("사용 설명", tabName = "usage_guide", icon = icon("info-circle")),
      hr(),
      selectInput(
        "global_year",
        "기준 연도",
        choices = sort(unique(dtf_shiny_commodity_service_ex$Year), decreasing = TRUE),
        selected = max(dtf_shiny_commodity_service_ex$Year)
      )
    )
	  )

## 3. body --------------------------------
body <- dashboardBody(
  ## 3.0. CSS styles in header ----------------------------
  tags$head(
    tags$script("document.title = 'New Zealand Trade Intelligence Dashboard'"),
    tags$link(rel = "stylesheet", type = "text/css", href = "custom.css")
  ),

  ## 3.1 Dashboard body --------------
  useShinyjs(),
  uiOutput("step_timeline"),
  tabItems(
    tabItem(
      tabName = "tab_data",
      h3("Step 1. Why: 패션 재고가 돈을 갉아먹고 있습니다"),
      fluidRow(
        box(
          width = 12,
          status = "primary",
          solidHeader = TRUE,
          title = NULL,
          class = "hero-box",
          div(
            class = "hero-left",
            h2("재고 비용을 30% 줄여주는 예측 파트너"),
            p("데이터를 올리면 곧바로 과재고/기회손실을 판별하고, 다음 달 망할지 흥할지 예측한 뒤 발주 지침을 내려줍니다.")
          ),
          div(
            class = "hero-steps",
            div(class = "hero-step", strong("1) Why"), tags$span("패션 재고는 현금흐름을 잠식합니다.")),
            div(class = "hero-step", strong("2) Diagnosis"), tags$span("내 가게가 과재고인지 기회손실인지 1분 진단")),
            div(class = "hero-step", strong("3) Prediction"), tags$span("다음 달을 예측하고 불확실성(밴드)까지 제시")),
            div(class = "hero-step", strong("4) Action"), tags$span("발주를 줄이거나 늘렸을 때의 현금 효과를 즉시 계산"))
          )
        )
      ),
      fluidRow(
        box(
          title = "데이터 소스 불러오기 (상장사/DART/데모)",
          width = 5,
          status = "primary",
          solidHeader = TRUE,
          textInput("fin_corp_query", "상장사 검색", placeholder = "예: 한섬, 020000"),
          fluidRow(
            column(6, actionButton("fin_corp_search", "검색", class = "btn-primary btn-block")),
            column(6, actionButton("fin_load_demo", "데모 데이터 로드", class = "btn-default btn-block"))
          ),
          selectInput("fin_corp_pick", "상장사 선택", choices = c(), selected = NULL),
          actionButton("fin_fetch_dart", "DART 불러오기", class = "btn-info"),
          tags$div(class = "assist-text", "데모/상장사 데이터를 불러오면 전체 흐름을 빠르게 미리 볼 수 있습니다.")
        ),
        box(
          title = "내 파일 업로드 & 매핑",
          width = 7,
          status = "info",
          solidHeader = TRUE,
          fileInput("fin_upload", "내 가게 파일 업로드", accept = c(".xlsx", ".xls", ".csv")),
          tags$div(
            class = "sidebar-upload-help",
            downloadButton(
              "fin_template",
              "샘플 템플릿",
              class = "btn btn-template btn-xs btn-block",
              style = "margin:4px 0 0 0; width:100%; box-sizing:border-box;"
            ),
            tags$div(
              class = "assist-text help-lines",
              tags$div(tags$strong("필수"), ": 연도 / 매출 / 재고"),
              tags$div(tags$strong("선택"), ": 순이익 / 자산 / 원가 / SKU / 채널")
            )
          ),
          uiOutput("fin_mapping_ui"),
          numericInput("fin_forecast_y", "예측 연도 수", value = 3, min = 1, max = 5),
          actionButton("fin_do_forecast", "예측 실행", class = "btn-primary"),
          actionButton(
            "go_step2_after_upload",
            "현황 진단으로 이동",
            class = "btn-success btn-sm",
            style = "margin-left:6px;"
          )
        )
      ),
      fluidRow(
        box(
          title = "Pre-Check: 데이터 건전성 신호등",
          width = 12,
          status = "success",
          solidHeader = TRUE,
          uiOutput("data_health_signals"),
          uiOutput("tab_lock_notice"),
          tags$div(
            class = "assist-text",
            "업로드 후 결측/연도 범위를 자동으로 점검하고, 이상이 없을 때만 진단/예측 탭이 활성화됩니다."
          )
        )
      ),
      fluidRow(
        box(
          title = "진행 가이드",
          width = 12,
          status = "success",
          solidHeader = TRUE,
          tags$div(
            class = "friendly-intro",
            tags$span(class = "pill-chip", "Step1 업로드"),
            tags$span(class = "pill-chip", "Step2 매핑"),
            tags$span(class = "pill-chip", "Step3 예측 실행"),
            tags$ul(
              class = "friendly-list",
              tags$li("필수 컬럼(연도/매출/재고)만 맞춰도 예측과 액션 추천을 바로 볼 수 있습니다."),
              tags$li("템플릿으로 포맷을 확인한 뒤 업로드하면 매핑 시간이 줄어듭니다."),
              tags$li("업로드 → Pre-Check 통과 → 진단 탭 활성화 순서로 따라오면 놓치는 단계가 없습니다.")
            )
          )
        )
      )
    ),

    tabItem(
      tabName = "tab_diagnosis",
      h3("Step 2. Diagnosis: 과재고인가, 기회손실인가?"),
      fluidRow(
        box(
          width = 12,
          status = "success",
          solidHeader = TRUE,
          title = "📊 한 줄 결론",
          uiOutput("diag_status_copy")
        )
      ),
      fluidRow(
        valueBoxOutput("fin_kpi_sales", width = 4),
        valueBoxOutput("fin_kpi_it", width = 4),
        valueBoxOutput("fin_kpi_roa", width = 4)
      ),
      fluidRow(
        box(
          title = "BCG 매트릭스 (재고회전율 vs 영업이익률)",
          width = 8,
          status = "info",
          solidHeader = TRUE,
          plotlyOutput("fin_quad_plot", height = "380px")
        ),
        box(
          title = "내 위치 해석",
          width = 4,
          status = "primary",
          solidHeader = TRUE,
          div(class = "diag-panel",
              uiOutput("diag_quadrant_label"),
              uiOutput("diag_metrics")
          ),
          tags$div(class = "diag-actions", uiOutput("analysis_actions_friendly")),
          tags$hr(),
          actionButton("go_to_action", "액션 플랜 보기", icon = icon("arrow-right"), class = "btn-primary btn-block btn-lg")
        )
      ),
      fluidRow(
        box(
          title = "추가 진단",
          width = 12,
          status = "warning",
          solidHeader = TRUE,
          collapsible = TRUE,
          collapsed = FALSE,
          fluidRow(
            box(
              title = "매출·재고 추세",
              width = 7,
              status = "info",
              solidHeader = TRUE,
              plotlyOutput("analysis_plot_1"),
              div(class = "viz-text-lg", textOutput("analysis_delta_note"))
            ),
            box(
              title = "데이터 품질/알림",
              width = 5,
              status = "warning",
              solidHeader = TRUE,
              htmlOutput("analysis_quality", class = "viz-text-lg"),
              uiOutput("analysis_alerts", class = "viz-text-lg"),
              div(class = "viz-text-lg", textOutput("analysis_desc_2"))
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
              title = "벤치마킹 테이블",
              width = 6,
              status = "primary",
              solidHeader = TRUE,
              tableOutput("fin_fc_table")
            )
          )
        )
      )
    ),

    tabItem(
      tabName = "tab_prediction",
      h3("Step 3. 수요 예측: 다음 달 흐름과 불확실성 보기"),
      tabBox(
        width = 12,
        tabPanel(
          "핵심 전망",
          fluidRow(
            class = "impact-box",
            valueBoxOutput("pred_insight_main", width = 3),
            valueBoxOutput("pred_warning", width = 3),
            valueBoxOutput("pred_action_box", width = 3),
            valueBoxOutput("pred_interval_box", width = 3)
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
              div(class = "assist-text", "리본 범위를 벗어나면 이상 징후입니다.")
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
              div(class = "viz-text-lg", textOutput("pred_interval_note"))
            ),
            box(
              title = "불확실성/정확도",
              width = 4,
              status = "warning",
              solidHeader = TRUE,
              htmlOutput("pred_quality", class = "viz-text-lg"),
              tableOutput("pred_accuracy"),
              uiOutput("pred_risk", class = "viz-text-lg"),
              div(class = "viz-text-lg", textOutput("pred_detail_2"))
            )
          )
        ),
        tabPanel(
          "정확도 · 세부 지표",
          fluidRow(
            box(
              title = "리스크 & 권장 행동",
              width = 4,
              status = "warning",
              solidHeader = TRUE,
              plotlyOutput("pred_risk_gauge", height = "230px"),
              div(class = "viz-text-lg", uiOutput("pred_risk")),
              tags$hr(),
              plotlyOutput("pred_growth_bar", height = "180px"),
              div(class = "viz-text-lg", uiOutput("pred_action"))
            ),
            box(
              title = "정확도/오차 분포",
              width = 4,
              status = "info",
              solidHeader = TRUE,
              plotlyOutput("pred_accuracy_plot", height = "260px"),
              div(class = "assist-text", "평균 오차 수량 · 정확도(%) · 최근 잔차로 성능을 확인합니다."),
              div(class = "viz-text-lg", textOutput("pred_accuracy_note")),
              plotlyOutput("pred_error_box", height = "220px"),
              div(class = "viz-text-lg", textOutput("pred_error_box_note"))
            ),
            box(
              title = "추가 지표",
              width = 4,
              status = "primary",
              solidHeader = TRUE,
              plotlyOutput("pred_comp_plot_plus", height = "220px"),
              div(class = "viz-text-lg", textOutput("pred_summary_plus")),
              plotlyOutput("pred_error_hist", height = "180px"),
              div(class = "viz-text-lg", textOutput("pred_error_hist_note"))
            )
          ),
          fluidRow(
            box(
              title = "누적 매출 (실제 vs 예측)",
              width = 6,
              status = "primary",
              solidHeader = TRUE,
              plotlyOutput("pred_cum_plot", height = "260px"),
              div(class = "viz-text-lg", textOutput("pred_detail_2_plus")),
              div(class = "assist-text", textOutput("pred_cum_note"))
            ),
            box(
              title = "예측 오차/폭",
              width = 6,
              status = "success",
              solidHeader = TRUE,
              plotlyOutput("pred_fc_error_plot"),
              div(class = "viz-text-lg", textOutput("pred_resid_note"))
            )
          ),
          br()
        )
      )
    ),

    tabItem(
      tabName = "tab_action",
      h3("Step 4. Action: 발주/액션 플랜"),
      fluidRow(
        valueBoxOutput("action_target_box"),
        valueBoxOutput("action_inventory_gap_box"),
        valueBoxOutput("action_cash_box")
      ),
      fluidRow(
        box(
          title = "시뮬레이션 (목표 재고회전율/성장)",
          width = 4,
          status = "warning",
          solidHeader = TRUE,
          sliderInput("target_turn", "목표 재고회전율", value = 3, min = 0, max = 10, step = 0.1),
          sliderInput("target_growth", "매출 성장 목표 (%)", value = 10, min = -100, max = 300, step = 1),
          tags$div(class = "assist-text", "슬라이더를 움직이면 목표 재고·현금화 가능 금액이 즉시 업데이트됩니다."),
          actionButton("apply_targets", "신호등 업데이트", class = "btn-primary btn-block")
        ),
        box(
          title = "다음 액션",
          width = 8,
          status = "primary",
          solidHeader = TRUE,
          div(class = "next-action", htmlOutput("detail_action"))
        )
      ),
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
          title = "예측(실제 + 예상)",
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
          title = "리포트 공유",
          width = 12,
          status = "info",
          solidHeader = TRUE,
          id = "share_panel",
          downloadButton("report_pdf", "최종 보고서 다운로드", class = "btn btn-success btn-lg"),
          downloadButton("report_csv", "CSV 내보내기")
        )
      )
    ),

    tabItem(
      tabName = "usage_guide",
      h3("이 대시보드를 활용하는 방법"),
      fluidRow(
        box(
          title = "1단계: 데이터 준비",
          width = 12,
          status = "primary",
          solidHeader = TRUE,
          tags$ul(
            tags$li("탭 '데이터 준비'에서 상장사 검색 또는 `내 가게 파일 업로드`로 데이터를 불러옵니다."),
            tags$li("필요 시 `DART 불러오기`나 `데모 데이터 로드` 버튼을 눌러 예시 데이터를 확인합니다."),
            tags$li("예측 실행 전 최소 3개 연도의 매출 데이터가 포함되어 있는지 확인하세요.")
          )
        )
      ),
      fluidRow(
        box(
          title = "2단계: 현황 진단",
          width = 12,
          status = "success",
          solidHeader = TRUE,
          tags$p("현황 진단 탭에서 KPI/BCG/트렌드로 내 위치를 한눈에 확인합니다."),
          tags$ul(
            tags$li("인사이트/경고/추천 행동 valueBox를 먼저 확인해 이번 시즌 핵심 메시지를 파악합니다."),
            tags$li("산업 평균 대비 KPI, BCG 사분면, 데이터 품질 알림을 통해 리스크를 조기 발견합니다."),
            tags$li("이 단계에서 1차 발주/프로모션 여부를 결정할 수 있습니다.")
          )
        )
      ),
      fluidRow(
        box(
          title = "3단계: AI 수요 예측",
          width = 12,
          status = "info",
          solidHeader = TRUE,
          tags$p("예측 탭의 핵심 요약과 모델 성능 상세/세분화 탭으로 신뢰도를 점검하세요."),
          tags$ul(
            tags$li("핵심 요약에서 신호등과 오차폭을 보고 빠르게 판단합니다."),
            tags$li("모델 성능 탭에서 Trend/시즌, 오차 지표를 검토합니다."),
            tags$li("SKU/채널별 세분화 탭에서 과대/과소 예측 구간을 찾습니다.")
          )
        )
      ),
      fluidRow(
        box(
          title = "4단계: 의사결정 & 공유",
          width = 12,
          status = "warning",
          solidHeader = TRUE,
          tags$ul(
            tags$li("액션 플랜 탭에서 목표 재고 턴/성장을 조정하며 What-if 시뮬레이션을 합니다."),
            tags$li("추천 행동과 액션 메모를 남기고 최종 보고서를 PDF/CSV로 공유합니다."),
            tags$li("새로운 데이터가 들어오면 1단계부터 반복하여 최신 계획을 유지하세요.")
          )
        )
      )
    )
  )
)


## put UI together --------------------
ui <-
  dashboardPage(header, siderbar, body)
