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
      hr()
    )
	  )

## 3. body --------------------------------
body <- dashboardBody(
  ## 3.0. CSS styles in header ----------------------------
  tags$head(
    tags$script("document.title = 'Fashion Inventory Forecasting System'"),
    tags$link(rel = "stylesheet", type = "text/css", href = "custom.css")
  ),

  ## 3.1 Dashboard body --------------
  useShinyjs(),
  uiOutput("step_timeline"),
  tabItems(
    tabItem(
      tabName = "tab_data",
      fluidRow(
        column(
          width = 9,
          box(
            width = 12,
            title = NULL,
            solidHeader = FALSE,
            status = NULL,
            class = "hero-panel",
            div(
              class = "hero-content",
              div(
                class = "hero-copy",
                h1("데이터 업로드하고 재고 예측 시작하기"),
                p("3단계로 내 쇼핑몰 데이터를 올리고, 진단/예측까지 한 번에 진행"),
                actionButton("cta_upload", "데이터 업로드 시작하기", class = "btn btn-primary btn-lg hero-cta"),
                p(class = "hero-alt", "또는 상장사 / 데모 데이터로 체험해볼 수 있어요.")
              ),
              div(
                class = "hero-feature-cards",
                div(class = "hero-feature-card",
                    tags$span(class = "feature-label", "Why"),
                    strong("재고 vs. 현금 흐름"),
                    p("재고가 현금흐름에 미치는 영향을 한 줄로 요약해 드려요.")
                ),
                div(class = "hero-feature-card",
                    tags$span(class = "feature-label", "Diagnosis"),
                    strong("내 가게 상태 진단"),
                    p("과재고인지 기회손실인지 1분 안에 파악할 수 있어요.")
                ),
                div(class = "hero-feature-card",
                    tags$span(class = "feature-label", "Prediction"),
                    strong("예측 & 액션"),
                    p("다음 달 수요를 예측하고 발주 가이드를 제안해요.")
                )
              )
            )
          ),
          box(
            width = 12,
            title = tagList(
              tags$span(class = "step-label", HTML("Step<br>1")),
              tags$span(class = "step-title-text", "데이터 소스 선택")
            ),
            solidHeader = TRUE,
            status = NULL,
            class = "step-box",
            p(class = "assist-text", "내 데이터 업로드, 상장사/DART 불러오기, 데모 데이터 중 하나를 선택하세요."),
            tabsetPanel(
              id = "data_source_tabs",
              type = "tabs",
              tabPanel(
                title = "내 파일 업로드",
                div(
                  class = "step-section",
                  fileInput("fin_upload", "엑셀/CSV 업로드", accept = c(".xlsx", ".xls", ".csv")),
                  div(
                    class = "upload-guidelines",
                    downloadButton(
                      "fin_template",
                      "샘플 템플릿 다운로드",
                      class = "btn btn-template btn-sm"
                    ),
                    tags$ul(
                      class = "upload-points",
                      tags$li("필수 컬럼: 연도, 매출, 재고"),
                      tags$li("선택 컬럼: SKU, 채널, 순이익 등 정밀 진단용")
                    )
                  ),
                  uiOutput("fin_mapping_ui")
                )
              ),
              tabPanel(
                title = "상장사 / DART",
                div(
                  class = "step-section",
                  textInput("fin_corp_query", "상장사 검색", placeholder = "예: 한섬, 020000"),
                  fluidRow(
                    column(6, actionButton("fin_corp_search", "검색", class = "btn btn-primary btn-block")),
                    column(6, actionButton("fin_fetch_dart", "DART 불러오기", class = "btn btn-default btn-block"))
                  ),
                  selectInput("fin_corp_pick", "상장사 선택", choices = c(), selected = NULL),
                  tags$div(class = "assist-text", "DART API 키가 없으면 데모 데이터가 자동으로 로드됩니다.")
                )
              ),
              tabPanel(
                title = "데모 데이터",
                div(
                  class = "step-section demo-section",
                  tags$p("데모 시나리오를 불러와 전체 흐름을 빠르게 체험해보세요."),
                  tags$ul(
                    class = "demo-list",
                    tags$li("여성 의류 쇼핑몰 (트렌드형)"),
                    tags$li("남성 스트리트 브랜드 (시즌형)"),
                    tags$li("아동복/완구 (롱테일형)")
                  ),
                  actionButton("fin_load_demo", "데모 데이터 로드", class = "btn btn-default"),
                  tags$div(class = "assist-text", "로드 후 바로 Pre-Check와 진단 단계를 확인할 수 있어요.")
                )
              )
            )
          ),
          box(
            width = 12,
            title = tagList(
              tags$span(class = "step-label", HTML("Step<br>2")),
              tags$span(class = "step-title-text", "기본 옵션 설정")
            ),
            solidHeader = TRUE,
            status = NULL,
            class = "step-box",
            p(class = "assist-text", "예측 기준 연도와 필요한 예측 기간을 선택하세요."),
            fluidRow(
              column(
                width = 6,
                selectInput(
                  "global_year",
                  "기준 연도",
                  choices = sort(unique(dtf_shiny_commodity_service_ex$Year), decreasing = TRUE),
                  selected = max(dtf_shiny_commodity_service_ex$Year)
                )
              ),
              column(
                width = 6,
                numericInput("fin_forecast_y", "예측 연도 수", value = 3, min = 1, max = 5)
              )
            ),
            tags$div(class = "assist-text", "필수 옵션만 남겨 핵심 설정에 집중할 수 있도록 정리했습니다.")
          ),
          box(
            width = 12,
            title = tagList(
              tags$span(class = "step-label", HTML("Step<br>3")),
              tags$span(class = "step-title-text", "업로드 & Pre-Check 결과")
            ),
            solidHeader = TRUE,
            status = NULL,
            class = "step-box",
            p(class = "assist-text", "업로드 직후 자동으로 Pre-Check가 실행되고, 모든 신호가 정상일 때 다음 단계 버튼이 활성화됩니다."),
            div(class = "precheck-cards", uiOutput("data_health_signals")),
            uiOutput("tab_lock_notice"),
            div(
              class = "step-actions",
              actionButton("go_step2_after_upload", "현황 진단으로 이동", class = "btn btn-primary btn-lg"),
              actionButton("fin_do_forecast", "예측 실행", class = "btn btn-outline btn-lg"),
              actionLink("back_to_step1", "내 데이터 다시 선택하기", class = "step-link")
            )
          )
        ),
        column(
          width = 3,
          box(
            width = 12,
            title = "이 페이지에서 할 일",
            solidHeader = TRUE,
            status = NULL,
            class = "help-card",
            tags$ul(
              class = "help-list",
              tags$li("1단계: 데이터를 선택하거나 업로드합니다."),
              tags$li("2단계: 기준 연도와 예측 기간을 정합니다."),
              tags$li("3단계: Pre-Check 통과 후 진단/예측으로 이동합니다.")
            )
          ),
          box(
            width = 12,
            title = "업로드할 파일 예시",
            solidHeader = TRUE,
            status = NULL,
            class = "help-card",
            tags$ul(
              class = "help-list",
              tags$li("연도, 매출, 재고는 반드시 포함"),
              tags$li("카테고리/SKU가 있으면 세부 진단 가능"),
              tags$li("엑셀/CSV 모두 지원하며, 첫 행은 헤더로 유지")
            )
          ),
          box(
            width = 12,
            title = "자주 묻는 질문",
            solidHeader = TRUE,
            status = NULL,
            collapsible = TRUE,
            collapsed = TRUE,
            class = "help-card",
            tags$details(
              tags$summary("엑셀 형식은 어떻게 맞추나요?"),
              tags$p("샘플 템플릿을 내려받아 컬럼명을 맞추면 자동 매핑됩니다.")
            ),
            tags$details(
              tags$summary("연도가 2개뿐인데 가능한가요?"),
              tags$p("Pre-Check에서 최소 연도 수 안내를 드리며, 3개 이상일 때 예측이 활성화됩니다.")
            ),
            tags$details(
              tags$summary("데이터 품질이 걱정돼요."),
              tags$p("결측/이상치는 Pre-Check 카드에서 바로 확인 가능합니다.")
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
          title = NULL,
          solidHeader = FALSE,
          status = NULL,
          class = "diag-summary-box",
          uiOutput("diag_status_copy")
        )
      ),
      fluidRow(
        column(width = 4, uiOutput("fin_kpi_sales")),
        column(width = 4, uiOutput("fin_kpi_it")),
        column(width = 4, uiOutput("fin_kpi_roa"))
      ),
      fluidRow(
        box(
          title = "BCG 매트릭스 (재고회전율 vs 영업이익률)",
          width = 8,
          status = NULL,
          solidHeader = TRUE,
          class = "diag-main-box",
          plotlyOutput("fin_quad_plot", height = "380px")
        ),
        box(
          title = "내 위치 해석",
          width = 4,
          status = NULL,
          solidHeader = TRUE,
          class = "diag-main-box diag-interpret-box",
          div(class = "diag-interpret-header",
              uiOutput("diag_quadrant_label")
          ),
          div(class = "diag-metric-list", uiOutput("diag_metrics")),
          tags$div(class = "diag-actions", uiOutput("analysis_actions_friendly")),
          tags$hr(),
          actionButton("go_to_action", "액션 플랜 보기", icon = icon("arrow-right"), class = "btn btn-primary btn-block")
        )
      ),
      fluidRow(
        box(
          title = "추가 진단",
          width = 12,
          status = NULL,
          solidHeader = TRUE,
          collapsible = TRUE,
          collapsed = TRUE,
          class = "diag-extra-box",
          tabsetPanel(
            id = "diag_extra_tabs",
            type = "tabs",
            tabPanel(
              "추세 / 비교",
              fluidRow(
                box(
                  title = "매출·재고 추세",
                  width = 7,
                  status = NULL,
                  solidHeader = TRUE,
                  class = "diag-sub-box",
                  plotlyOutput("analysis_plot_1"),
                  div(class = "viz-text-lg", textOutput("analysis_delta_note"))
                ),
                box(
                  title = "재고 효율·이익 비교",
                  width = 5,
                  status = NULL,
                  solidHeader = TRUE,
                  class = "diag-sub-box",
                  plotlyOutput("analysis_plot_2"),
                  div(class = "viz-text-lg", textOutput("analysis_desc_1"))
                )
              )
            ),
            tabPanel(
              "데이터 품질 / 알림",
              fluidRow(
                box(
                  title = "데이터 품질/알림",
                  width = 12,
                  status = NULL,
                  solidHeader = TRUE,
                  class = "diag-sub-box",
                  htmlOutput("analysis_quality", class = "viz-text-lg"),
                  uiOutput("analysis_alerts", class = "viz-text-lg"),
                  div(class = "viz-text-lg", textOutput("analysis_desc_2"))
                )
              )
            ),
            tabPanel(
              "벤치마킹 테이블",
              fluidRow(
                box(
                  title = "벤치마킹 테이블",
                  width = 12,
                  status = NULL,
                  solidHeader = TRUE,
                  class = "diag-sub-box",
                  tableOutput("fin_fc_table")
                )
              )
            )
          )
        )
      )
    ),

    tabItem(
      tabName = "tab_prediction",
      h3("Step 3. 수요 예측: 다음 달 흐름과 불확실성 보기"),
      fluidRow(
        box(
          width = 12,
          title = NULL,
          solidHeader = FALSE,
          class = "pred-summary-box",
          uiOutput("pred_summary_card")
        )
      ),
      fluidRow(
        box(
          width = 12,
          title = "예측 기준 데이터",
          solidHeader = TRUE,
          class = "pred-main-box pred-source-box",
          uiOutput("pred_source_ui")
        )
      ),
      fluidRow(
        column(width = 4, uiOutput("pred_kpi_forecast")),
        column(width = 4, uiOutput("pred_kpi_signal")),
        column(width = 4, uiOutput("pred_kpi_band"))
      ),
      fluidRow(
        box(
          title = "이번 달 알아두면 좋은 점",
          width = 7,
          solidHeader = TRUE,
          class = "pred-brief-box",
          uiOutput("pred_top3"),
          div(class = "assist-text", "예측 추세·불확실성·학습 기간을 한눈에 요약했습니다.")
        ),
        box(
          title = "추천 행동 / 알림",
          width = 5,
          solidHeader = TRUE,
          class = "pred-brief-box",
          uiOutput("pred_action_chip"),
          uiOutput("pred_action_simple"),
          div(class = "assist-text", "리본 폭이 커지면 보수적으로 발주하고, 작은 폭이면 과감하게 움직여도 좋아요.")
        )
      ),
      fluidRow(
        valueBoxOutput("fin_kpi_sales", width = 4),
        valueBoxOutput("fin_kpi_it", width = 4),
        valueBoxOutput("fin_kpi_roa", width = 4)
      ),
      fluidRow(
        box(
          title = tagList("앞으로 흐름(연도별)", tags$span(class = "pred-meta-note", "기준 : 매출 / 단위 : 억 원")),
          width = 8,
          solidHeader = TRUE,
          class = "pred-main-box",
          plotlyOutput("pred_ts_plot"),
          div(class = "viz-text-lg", textOutput("pred_summary")),
          div(class = "viz-text-lg", textOutput("pred_detail_1")),
          div(class = "viz-text-lg", textOutput("pred_interval_note"))
        ),
        box(
          title = "불확실성 / 정확도",
          width = 4,
          solidHeader = TRUE,
          class = "pred-main-box pred-uncertainty-box",
          htmlOutput("pred_quality", class = "viz-text-lg"),
          tableOutput("pred_accuracy"),
          uiOutput("pred_risk", class = "viz-text-lg"),
          div(class = "viz-text-lg", textOutput("pred_detail_2"))
        )
      ),
      fluidRow(
        box(
          title = "추가 분석",
          width = 12,
          solidHeader = TRUE,
          collapsible = TRUE,
          collapsed = TRUE,
          class = "pred-extra-box",
          tabsetPanel(
            id = "pred_extra_tabs",
            type = "tabs",
            tabPanel(
              "정확도 · 세부 지표",
              fluidRow(
                box(
                  title = "리스크 & 권장 행동",
                  width = 4,
                  solidHeader = TRUE,
                  class = "pred-detail-box",
                  plotlyOutput("pred_risk_gauge", height = "230px"),
                  div(class = "viz-text-lg", uiOutput("pred_risk")),
                  tags$hr(),
                  plotlyOutput("pred_growth_bar", height = "180px"),
                  div(class = "viz-text-lg", uiOutput("pred_action"))
                ),
                box(
                  title = "정확도/오차 분포",
                  width = 4,
                  solidHeader = TRUE,
                  class = "pred-detail-box",
                  plotlyOutput("pred_accuracy_plot", height = "260px"),
                  div(class = "assist-text", "평균 오차 수량 · 정확도(%) · 최근 잔차로 성능을 확인합니다."),
                  div(class = "viz-text-lg", textOutput("pred_accuracy_note")),
                  plotlyOutput("pred_error_box", height = "220px"),
                  div(class = "viz-text-lg", textOutput("pred_error_box_note"))
                ),
                box(
                  title = "추가 지표",
                  width = 4,
                  solidHeader = TRUE,
                  class = "pred-detail-box",
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
                  solidHeader = TRUE,
                  class = "pred-detail-box",
                  plotlyOutput("pred_cum_plot", height = "260px"),
                  div(class = "viz-text-lg", textOutput("pred_detail_2_plus")),
                  div(class = "assist-text", textOutput("pred_cum_note"))
                ),
                box(
                  title = "예측 오차/폭",
                  width = 6,
                  solidHeader = TRUE,
                  class = "pred-detail-box",
                  plotlyOutput("pred_fc_error_plot"),
                  div(class = "viz-text-lg", textOutput("pred_resid_note"))
                )
              ),
              br()
            )
          )
        )
      )
    ),

    tabItem(
      tabName = "tab_action",
      h3("Step 4. Action: 발주/액션 플랜"),
      fluidRow(
        box(
          width = 12,
          title = NULL,
          solidHeader = FALSE,
          class = "action-summary-box",
          uiOutput("action_summary_card")
        )
      ),
      fluidRow(
        valueBoxOutput("action_target_box"),
        valueBoxOutput("action_inventory_gap_box"),
        valueBoxOutput("action_cash_box")
      ),
      fluidRow(
        column(width = 4, uiOutput("action_kpi_target")),
        column(width = 4, uiOutput("action_kpi_gap")),
        column(width = 4, uiOutput("action_kpi_cash"))
      ),
      fluidRow(
        box(
          width = 12,
          title = "시뮬레이션 설정",
          solidHeader = TRUE,
          class = "action-control-box",
          sliderInput("target_turn", "목표 재고회전율", value = 3, min = 0, max = 10, step = 0.1),
          sliderInput("target_growth", "매출 성장 목표 (%)", value = 10, min = -100, max = 300, step = 1),
          tags$div(class = "assist-text", "슬라이더를 조정하면 위 요약 카드와 아래 그래프들이 자동으로 업데이트됩니다."),
          actionButton("apply_targets", "신호등 업데이트", class = "btn btn-primary"),
          tags$div(class = "assist-text", "목표 값을 저장하면 다음 액션 권장안과 리포트에도 반영됩니다.")
        )
      ),
      fluidRow(
        box(
          title = "내 기업 추이",
          width = 6,
          solidHeader = TRUE,
          class = "action-chart-box",
          plotlyOutput("detail_plot_1", height = "350px")
        ),
        box(
          title = "연도별 성장률 + 재고 비율",
          width = 6,
          solidHeader = TRUE,
          class = "action-chart-box",
          plotlyOutput("detail_plot_2", height = "350px")
        )
      ),
      fluidRow(
        box(
          title = "재무 구조",
          width = 6,
          solidHeader = TRUE,
          class = "action-chart-box",
          plotlyOutput("detail_plot_3", height = "320px")
        ),
        box(
          title = "예측(실제 + 예상)",
          width = 6,
          solidHeader = TRUE,
          class = "action-chart-box",
          plotlyOutput("detail_plot_4", height = "320px")
        )
      ),
      fluidRow(
        box(
          title = "다음 액션",
          width = 5,
          solidHeader = TRUE,
          class = "action-text-box",
          htmlOutput("detail_action")
        ),
        box(
          title = "설명",
          width = 7,
          solidHeader = TRUE,
          class = "action-text-box",
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
          solidHeader = TRUE,
          class = "action-share-box",
          tags$p("현재 시나리오 설정과 결과 요약을 포함한 보고서를 내려받을 수 있습니다."),
          downloadButton("report_pdf", "최종 보고서 다운로드", class = "btn btn-success btn-lg"),
          downloadButton("report_csv", "CSV 내보내기", class = "btn btn-default"),
          tags$div(class = "assist-text", "PDF/CSV에는 시뮬레이션 설정, 요약 수치, 주요 그래프 캡션이 포함됩니다.")
        )
      )
    ),

    tabItem(
      tabName = "usage_guide",
      h3("이 대시보드를 활용하는 방법"),
      fluidRow(
        box(
          title = "빠른 시작",
          width = 12,
          solidHeader = TRUE,
          class = "usage-quickstart-box",
          tags$p("이 대시보드를 처음 쓰신다면 아래 순서만 따라오세요."),
          tags$ol(
            class = "quickstart-list",
            tags$li("데이터 준비 탭에서 엑셀/CSV를 업로드하거나 상장사/데모 데이터를 불러옵니다."),
            tags$li("현황 진단과 수요 예측 탭을 순서대로 열어 결과를 확인합니다."),
            tags$li("발주/액션 플랜 탭에서 슬라이더를 움직이며 시나리오를 조정합니다.")
          ),
          actionButton("go_to_tab_data", "1단계로 이동", class = "btn btn-primary")
        )
      ),
      fluidRow(
        box(
          title = "단계별 안내",
          width = 12,
          solidHeader = TRUE,
          class = "usage-accordion-box",
          bsCollapse(
            id = "usage_collapse",
            multiple = FALSE,
            open = "step1",
            bsCollapsePanel(
              title = "1단계: 데이터 준비",
              value = "step1",
              tags$ul(
                tags$li("상장사 검색 또는 ‘내 가게 파일 업로드’로 데이터를 불러옵니다."),
                tags$li("업로드 후 Pre-Check에서 연도/필수 컬럼 이상 여부를 확인합니다."),
                tags$li("정상일 때만 ‘현황 진단으로 이동’ 버튼이 활성화됩니다.")
              ),
              actionLink("go_to_step1", "데이터 준비 탭 열기")
            ),
            bsCollapsePanel(
              title = "2단계: 현황 진단",
              value = "step2",
              tags$ul(
                tags$li("KPI/BCG/트렌드로 현재 위치를 한눈에 확인합니다."),
                tags$li("초록/노랑/빨강 신호등으로 이번 시즌 리스크를 확인합니다."),
                tags$li("이 단계에서 1차 발주/프로모션 여부를 결정할 수 있습니다.")
              ),
              actionLink("go_to_step2", "현황 진단 탭 열기")
            ),
            bsCollapsePanel(
              title = "3단계: AI 수요 예측",
              value = "step3",
              tags$ul(
                tags$li("다음 해/다음 달 예상 매출과 불확실성 리본을 확인합니다."),
                tags$li("핵심 상품의 예측 구간(상승/하락)을 시각적으로 비교합니다."),
                tags$li("SKU/채널별 과다/과소 예측 구간을 탐색합니다.")
              ),
              actionLink("go_to_step3", "수요 예측 탭 열기")
            ),
            bsCollapsePanel(
              title = "4단계: 의사결정 & 공유",
              value = "step4",
              tags$ul(
                tags$li("목표 턴/성장률을 조정해 What-if 시뮬레이션을 합니다."),
                tags$li("발주/액션 플랜에서 추천 행동 리스트를 확인합니다."),
                tags$li("PDF/CSV로 내보내 팀/외부와 공유합니다.")
              ),
              actionLink("go_to_step4", "액션 플랜 탭 열기")
            )
          )
        )
      ),
      fluidRow(
        box(
          title = "FAQ",
          width = 12,
          solidHeader = TRUE,
          class = "usage-faq-box",
          tags$ul(
            tags$li(
              tags$strong("Q. 데이터 형식이 헷갈려요."),
              tags$p("데이터 준비 탭에서 샘플 템플릿을 다운로드해 동일한 구조로 채워주세요.")
            ),
            tags$li(
              tags$strong("Q. 예측 값이 이상해 보여요."),
              tags$p("학습 연도가 3개 미만이면 정확도가 떨어질 수 있습니다. 데이터 기간을 늘려주세요.")
            ),
            tags$li(
              tags$strong("Q. 리포트는 어디에서 다운받나요?"),
              tags$p("액션 플랜 탭 맨 아래에서 PDF/CSV를 내려받을 수 있습니다.")
            )
          )
        ),
      fluidRow(
        box(
          title = "용어 설명",
          width = 12,
          solidHeader = TRUE,
          class = "usage-glossary-box",
          tags$dl(
            tags$dt("SKU"),
            tags$dd("Stock Keeping Unit의 약자로, 재고를 구분하는 최소 단위(예: 색상/사이즈 조합)입니다."),
            tags$dt("Pre-Check"),
            tags$dd("업로드한 데이터가 필수 컬럼과 최소 연도 수를 충족하는지 자동으로 점검하는 단계입니다."),
            tags$dt("재고회전율"),
            tags$dd("연간 매출원가를 평균 재고로 나눈 지표로, 재고가 얼마나 빠르게 팔리는지 나타냅니다."),
            tags$dt("리본/불확실성"),
            tags$dd("예측값 주변의 신뢰 구간으로, 값이 넓을수록 변동성이 크다는 의미입니다."),
            tags$dt("ROA"),
            tags$dd("Return on Assets의 약자로 자산 대비 이익률입니다. 순이익을 총자산으로 나누어 계산하며, 자산 활용 효율성을 나타냅니다."),
            tags$dt("BCG 매트릭스"),
            tags$dd("재고회전율과 영업이익률을 기준으로 사업을 분류하는 그래프입니다. 캐시카우/스타/도그 구간 등으로 현재 위치를 파악합니다."),
            tags$dt("리드타임"),
            tags$dd("주문 이후 제품을 공급받기까지 걸리는 시간입니다. 수요가 늘거나 줄 때 리드타임을 감안해 발주해야 합니다.")
          )
        )
      )
      )
    )
  )
)


## put UI together --------------------
ui <-
  dashboardPage(header, siderbar, body)
