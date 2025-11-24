# nz-trade-dash dashboard structure

This Shiny app is already organized as a dashboard (shinydashboard). The notes below document the layout and file responsibilities for the mini‑project report.

## High-level layout
- Header: title and share/feedback menus (`ui.R`).
- Sidebar: tab navigation and per-tab filters (country/market selectors, commodity selectors, reset/build buttons).
- Body tabs:
  - `dashboard` (Main Dashboard): KPI value boxes and headline charts.
  - `country_intel` (Market Intelligence): maps and partner trends.
  - `ci_exports` / `ci_imports` / `ci_intel_by_hs` (Commodity Intelligence): treemaps, key product trends, HS lookups.
  - Additional detail sections are revealed via “Show more” logic in `server.R`.

## File responsibilities
```
nz-trade-dash/
├─ ui.R                 # dashboardPage: header, sidebar menu, tabItems with outputs
├─ server.R             # server function: progress flow, reactives, render* calls
├─ share_load.R         # shared libraries, future/memoise setup, data loads (.rda)
├─ helper_funs.R        # data wrangling helpers, CAGR, treemap helper, etc.
├─ app.R                # separate financial benchmarking Shiny app (not tied to dashboard tabs)
├─ *.rda                # preprocessed datasets (trade, concordance, lists)
└─ www/                 # static assets (e.g., MBIE logo)
```

## Data/control flow (dashboard)
```
share_load.R
  ├─ loads libraries and helper_funs.R
  ├─ loads .rda datasets (dtf_shiny_full, commodity/country tables, concordance)
  └─ sets globals (maxYear, memoised ct_search)
       ↓ (source)
server.R
  ├─ initializes progress steps and value box metrics
  ├─ builds filtered data frames per tab (country/commodity selections)
  ├─ renders outputs: valueBox, highchart, DataTable, timevis
  └─ handles UI events (show more, build reports, download)
       ↓ (outputs consumed)
ui.R
  ├─ defines header/sidebar/tabItems
  ├─ declares output placeholders (valueBoxOutput/highchartOutput/etc.)
  └─ wires tab-specific inputs (selectize, radioButtons, actionButtons)
```

## Run command (from repository root)
```sh
R -e "shiny::runApp('nz-trade-dash')"
```

## Notes for extension
- Add new tabs/boxes in `ui.R` and corresponding `render*` logic in `server.R`.
- Keep shared data and helper functions in `share_load.R` / `helper_funs.R` to avoid duplication.
