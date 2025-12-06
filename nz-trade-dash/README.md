# New Zealand Trade Intelligence

---

- Developed by: [Wei Zhang](https://community.rstudio.com/u/weizhang/summary)
- App on gallery: https://gallery.shinyapps.io/nz-trade-dash

---

This is a public version of the source code for the [New Zealand Trade Intelligence Dashboard](http://tradeintelligence.mbie.govt.nz). The Dashboard is built with R, JavaScript and Shiny.

This code is published under a [Creative Commons license](http://creativecommons.org/licenses/by/3.0/nz/). While all care and diligence has been used, MBIE gives no warranty it is error free and will not be liable for any loss or damage suffered by the use directly, or indirectly, of the information.

## 문제 정의 (Problem Definition)

국내 중견 패션 브랜드의 재무·공급망 담당자는 시즌별 발주와 현금흐름을 동시에 관리해야 하지만, 사내 ERP 데이터만으로는 **“얼마나 생산 혹은 발주해야 하는가?”**를 명확히 판단하기 어렵습니다. 본 대시보드는 다음과 같은 가설을 해결하도록 설계되어 있습니다.

1. **사용자**: 시즌·월 단위로 재고/매출을 조정해야 하는 재고기획자(MD)와 재무 관리자.
2. **상황**: 지난 3~5년의 월별 매출/재고 데이터를 기반으로 Prophet 모델로 예측을 생성하고, 경쟁사/시장 지표와 비교하여 위험도를 판단해야 함.
3. **의사결정**: 예측 불확실성이 높을수록 발주량을 줄이고, 매출 증가 시그널이 있을 때는 핵심 품목을 선발주해야 함. 이를 위해
   - `재고·매출 예측 (쉽게 보기)` 섹션은 한 줄 인사이트·예측 품질·추천 행동을 제공하여 경영진이 빠르게 판단하도록 돕습니다.
   - `재고·매출 예측 (상세 보기)` 섹션은 예측 밴드, 누적 매출, 잔차 분석을 연결해 “왜 이런 판단을 내렸는지” 데이터 근거를 제공합니다.

즉, 이 프로젝트는 **“어떤 시즌에 어떤 수준의 발주/프로모션 전략을 취해야 하는가?”**라는 비즈니스 질문을 구체적으로 정의하고, 데이터를 통해 실행 가능한 답을 제시하는 데 목적을 둡니다.
