import os, io, re, time, math, random, threading, zipfile, unicodedata
from datetime import datetime, timedelta
from xml.etree import ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd
import plotly.express as px
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import streamlit as st
from prophet import Prophet

# ---- requests_cache: optional ----
try:
    import requests_cache
    HAS_REQUESTS_CACHE = True
except Exception:
    requests_cache = None
    HAS_REQUESTS_CACHE = False

# -------------------------------
# App config
# -------------------------------
st.set_page_config(page_title="재무 벤치마킹 대시보드", page_icon="📊", layout="wide")

# --- UX 개선: Professional-looking CSS to hide footer and menu ---
st.markdown("""
    <style>
        /* "Made with Streamlit" 푸터 숨기기 */
        footer {visibility: hidden;}
        /* Streamlit 메인 메뉴(햄버거 버튼) 숨기기 */
        #MainMenu {visibility: hidden;}
        /* 컨테이너 간 간격 추가 */
        .st-emotion-cache-1jicfl2 { 
            margin-bottom: 15px;
        }
    </style>
""", unsafe_allow_html=True)

# --- [v4.0] 세션 상태 초기화 ---
if "data_loaded" not in st.session_state:
    st.session_state.data_loaded = False
    st.session_state.df_all = pd.DataFrame()
    st.session_state.df_dart = pd.DataFrame()
    st.session_state.df_my_company = pd.DataFrame()
    st.session_state.name_dart = ""
    st.session_state.name_my_company = "My Company"
    st.session_state.forecast_y = 3
    st.session_state.lenient_skips = {}
# --- 세션 초기화 끝 ---

# [v4.8] 통합 타이틀
st.title("📊 1:1 재무 벤치마킹 대시보드 (v4.8)")
st.caption("(상장사 1곳) vs. (내 가게의 연도별 '기초' 데이터) 비교")


# -------------------------------
# [v0.0] DART 관련 Helper Functions
# (DART API 키 로드, HTTP 세션, DART 데이터 파싱, 기업 검색 등)
# -------------------------------
def load_api_key() -> str | None:
    key = os.getenv("DART_API_KEY")
    if key: return key
    try:
        from dotenv import load_dotenv
        load_dotenv()
        key = os.getenv("DART_API_KEY")
        if key: return key
    except Exception: pass
    import os.path as _p
    cand_paths = [
        _p.expanduser("~/.streamlit/secrets.toml"),
        _p.join(os.getcwd(), ".streamlit", "secrets.toml"),
    ]
    if any(_p.exists(p) for p in cand_paths):
        try: return st.secrets["DART_API_KEY"]
        except Exception: pass
    return None

def _make_session():
    s = requests.Session()
    retry = Retry(
        total=5, backoff_factor=0.6,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s

SESSION = _make_session()
RATE_LOCK = threading.Lock()
LAST_CALL = [0.0]
MIN_SPACING = 0.15

def http_get(url, params=None, timeout=40):
    with RATE_LOCK:
        dt = time.time() - LAST_CALL[0]
        if dt < MIN_SPACING:
            time.sleep(MIN_SPACING - dt)
        LAST_CALL[0] = time.time()
    r = SESSION.get(url, params=params, timeout=timeout)
    if r.status_code == 429:
        wait = int(r.headers.get("Retry-After", 2))
        time.sleep(wait + random.uniform(0, 0.5))
        r = SESSION.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    return r

if HAS_REQUESTS_CACHE:
    requests_cache.install_cache("dart_cache", expire_after=60 * 60 * 12)

def fetch_corp_codes_cached(api_key: str) -> pd.DataFrame:
    url = f"https://opendart.fss.or.kr/api/corpCode.xml?crtfc_key={api_key}"
    resp_bytes = http_get(url, timeout=40).content
    with zipfile.ZipFile(io.BytesIO(resp_bytes)) as zf:
        xml_name = None
        for name in zf.namelist():
            lower = name.lower()
            if lower.endswith("corpcode.xml") or lower.endswith("coropcode.xml"):
                xml_name = name; break
        if xml_name is None:
            if "CORPCODE.xml" in zf.namelist(): xml_name = "CORPCODE.xml"
            else: raise RuntimeError(f"CORPCODE.xml을 ZIP에서 찾지 못했습니다. namelist={zf.namelist()}")
        with zf.open(xml_name) as f:
            tree = ET.parse(f)
    corp_code_list, corp_name_list, stock_code_list = [], [], []
    for el in tree.iterfind(".//list"):
        corp_code_list.append(el.findtext("corp_code") or "")
        corp_name_list.append(el.findtext("corp_name") or "")
        stock_code_list.append(el.findtext("stock_code") or "")
    if not corp_code_list: raise RuntimeError("CORPCODE.xml 파싱 결과 0행")
    df = pd.DataFrame({
        "corp_code": [str(x) for x in corp_code_list],
        "corp_name": [str(x) for x in corp_name_list],
        "stock_code": [str(x) for x in stock_code_list],
    })
    try: df.to_csv("corp_codes.csv", index=False, encoding="utf-8")
    except Exception: pass
    return df

def get_corp_codes(api_key: str) -> pd.DataFrame:
    base_cols = ["corp_code", "corp_name", "stock_code"]
    try:
        df = pd.read_csv("corp_codes.csv", dtype=str, encoding="utf-8")
        for c in base_cols:
            if c not in df.columns: df[c] = ""
        return df[base_cols]
    except Exception: pass
    try:
        df = pd.read_parquet("corp_codes.parquet")
        for c in base_cols:
            if c not in df.columns: df[c] = ""
            else: df[c] = df[c].astype(str)
        return df[base_cols]
    except Exception: pass
    df = fetch_corp_codes_cached(api_key)
    for c in base_cols:
        if c not in df.columns: df[c] = ""
        else: df[c] = df[c].astype(str)
    return df[base_cols]

def _norm(s: str) -> str:
    if s is None: return ""
    s = str(s); s = unicodedata.normalize("NFKC", s)
    s = re.sub(r"[\s\-\_\.\&\/\(\)\[\]\,\+]+", "", s).lower()
    s = s.replace("주식회사", "").replace("(주)", "").replace("㈜", "")
    return s
ALIAS_MAP = {"f&f": "에프앤에f", "ff": "에프앤에f", "fnf": "에프앤에프", "thehandsome": "한섬", "handsome": "한섬", "shinsegaeinternational": "신세계인터내셔날", "shinsegaeintl": "신세계인터내셔날", "kolonindustries": "코오롱인더스트리",}
def _alias_to_kor(q_norm: str): return ALIAS_MAP.get(q_norm)
def _and_contains(series: pd.Series, tokens: list[str]) -> pd.Series:
    norm_series = series.fillna("").astype(str).apply(_norm)
    mask = pd.Series([True] * len(norm_series), index=norm_series.index)
    for t in tokens:
        if not t: continue
        mask = mask & norm_series.str.contains(re.escape(t))
    return mask
def search_corp_smart(corp_df: pd.DataFrame, query: str, limit: int = 30) -> pd.DataFrame:
    if not query: return corp_df.head(limit)
    q = query.strip(); q_norm = _norm(q)
    if re.fullmatch(r"\d{6}", q):
        hit = corp_df.loc[corp_df["stock_code"] == q]
        if not hit.empty: return hit.head(limit)
    alias = _alias_to_kor(q_norm); tokens = []
    if alias: tokens = [_norm(alias)]
    else: tokens = [_norm(tok) for tok in re.split(r"\s+", q) if tok.strip()]
    mask_name = _and_contains(corp_df["corp_name"], tokens); res = corp_df[mask_name]
    if not res.empty: return res.head(limit)
    if "stock_name" in corp_df.columns:
        mask_stockname = _and_contains(corp_df["stock_name"], tokens); res2 = corp_df[mask_stockname]
        if not res2.empty: return res2.head(limit)
    loose = corp_df[corp_df["corp_name"].astype(str).str.contains(query, case=False, na=False) | (corp_df["stock_name"].astype(str).str.contains(query, case=False, na=False) if "stock_name" in corp_df.columns else False)]
    return loose.head(limit)

def to_num_safe(x):
    try: return float(str(x).replace(",", ""))
    except Exception: return None
ACCOUNT_EXACT = {"INVENTORY": ["재고자산"],"SALES": ["매출액", "매출액(수익)"],"COGS": ["매출원가"],"TOTAL_ASSETS": ["자산총계", "총자산"],"NET_INCOME": ["당기순이익", "당기순이익(손실)", "지배기업의소유주지분에귀속되는당기순이익"],"GROSS_PROFIT": ["매출총이익", "매출총이익(손실)"],"TOTAL_REVENUE": ["영업수익", "수익", "매출액(영업수익)"],"COMPREHENSIVE_INCOME": ["총포괄손익", "당기총포괄손익"]}
ACCOUNT_CONTAINS = {"INVENTORY_PARTS": ["상품", "제품", "반제품", "재공품", "원재료", "저장품"],"SALES": ["매출", "수익"],"NET_INCOME": ["당기순이익", "지배기업", "순이익"],"TOTAL_ASSETS": ["자산총계", "총자산"],"COGS": ["매출원가"]}
ACCOUNT_IDS = {"INVENTORY": ["ifrs-full_Inventories", "ifrs_Inventories"],"TOTAL_ASSETS": ["ifrs-full_Assets", "ifrs_Assets"],"NET_INCOME": ["ifrs-ProfitLoss", "ifrs-full_ProfitLoss"],"SALES": ["ifrs-Revenue", "ifrs-full_Revenue"],"COGS": ["dart_CostOfSales"]}
def pick_first_exact(df, names):
    for nm in names:
        hit = df.loc[df["account_nm"] == nm]
        if not hit.empty: return to_num_safe(hit["thstrm_amount"].iloc[0])
    return None
def pick_by_contains(df, substr_list):
    cand = df[df["account_nm"].apply(lambda s: any(ss in str(s) for ss in substr_list))]
    if cand.empty: return None
    vals = cand["thstrm_amount"].apply(to_num_safe).dropna()
    if vals.empty: return None
    return float(vals.iloc[vals.abs().argmax()])
def pick_by_ids(df, id_list):
    if "account_id" not in df.columns or not id_list: return None
    cand = df[df["account_id"].isin(id_list)]
    if cand.empty: return None
    return to_num_safe(cand["thstrm_amount"].iloc[0])
def inventory_from_parts(df):
    parts = []
    for part in ACCOUNT_CONTAINS["INVENTORY_PARTS"]:
        sub = df[df["account_nm"] == part]
        if sub.empty: sub = df[df["account_nm"].astype(str).str.contains(part)]
        if not sub.empty:
            v = to_num_safe(sub["thstrm_amount"].iloc[0])
            if v is not None: parts.append(v)
    return sum(parts) if parts else None
def get_accounts(api_key: str, corp_code: str, year: int, fs_div: str, rc: str) -> pd.DataFrame:
    url = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"
    params = {"crtfc_key": api_key, "corp_code": corp_code, "bsns_year": str(year), "reprt_code": rc, "fs_div": fs_div}
    data = http_get(url, params=params, timeout=40).json()
    if data.get("status") == "000":
        df = pd.DataFrame(data.get("list", []))
        if "thstrm_amount" in df.columns: df["thstrm_amount"] = df["thstrm_amount"].fillna("0")
        for c in ["account_nm", "account_id", "thstrm_amount"]:
            if c not in df.columns: df[c] = None
        return df
    return pd.DataFrame()
def fetch_year_block(api_key, corp_code, year):
    reprt_codes = ["11011", "11014", "11012", "11013"]
    fs_divs = ["CFS", "OFS"]
    for rc in reprt_codes:
        for fs in fs_divs:
            df = get_accounts(api_key, corp_code, year, fs, rc)
            if not df.empty: return (year, df)
    return (year, pd.DataFrame())
def fetch_panel_parallel(api_key: str, corp_code: str, years: list, max_workers: int = 4) -> dict[int, pd.DataFrame]:
    out = {}
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = {ex.submit(fetch_year_block, api_key, corp_code, y): y for y in years}
        for fut in as_completed(futs):
            y, df = fut.result()
            out[y] = df
    return out
def extract_metrics(year_df_map: dict[int, pd.DataFrame], years: list, strict: bool = True):
    rows = []; skipped = []
    for y in years:
        got = year_df_map.get(y, pd.DataFrame())
        if got.empty:
            msg = f"[OpenDART] {y}년 데이터 없음"
            if strict: raise ValueError(msg)
            skipped.append((y, msg)); continue
        inv = (pick_first_exact(got, ACCOUNT_EXACT["INVENTORY"]) or pick_by_ids(got, ACCOUNT_IDS.get("INVENTORY", [])) or inventory_from_parts(got))
        sales = (pick_first_exact(got, ACCOUNT_EXACT["SALES"]) or pick_by_ids(got, ACCOUNT_IDS.get("SALES", [])) or pick_first_exact(got, ACCOUNT_EXACT["TOTAL_REVENUE"]) or pick_by_contains(got, ACCOUNT_CONTAINS["SALES"]))
        cogs = (pick_first_exact(got, ACCOUNT_EXACT.get("COGS", [])) or pick_by_contains(got, ACCOUNT_CONTAINS["COGS"]))
        assets = (pick_first_exact(got, ACCOUNT_EXACT["TOTAL_ASSETS"]) or pick_by_ids(got, ACCOUNT_IDS.get("TOTAL_ASSETS", [])) or pick_by_contains(got, ACCOUNT_CONTAINS["TOTAL_ASSETS"]))
        ni = (pick_first_exact(got, ACCOUNT_EXACT["NET_INCOME"]) or pick_by_ids(got, ACCOUNT_IDS.get("NET_INCOME", [])) or pick_by_contains(got, ACCOUNT_CONTAINS["NET_INCOME"]) or pick_first_exact(got, ACCOUNT_EXACT["COMPREHENSIVE_INCOME"]))
        gp = pick_first_exact(got, ACCOUNT_EXACT["GROSS_PROFIT"])
        core_ok = (assets is not None) and (ni is not None) and (inv is not None) and ((sales is not None) or (cogs is not None))
        if not core_ok:
            msg = f"[OpenDART] {y}년 핵심 계정 누락"
            if strict: raise ValueError(msg)
            skipped.append((y, msg)); continue
        roa = (ni / assets) * 100.0 if assets else None
        inv_turn = (cogs / inv) if (cogs is not None and inv) else ((sales / inv) if (sales is not None and inv) else None)
        gp_margin = (gp / sales * 100.0) if (gp is not None and sales not in (None, 0)) else None
        rows.append({"Year": y, "재고자산": inv, "매출액": sales, "매출총이익": gp, "매출총이익률(%)": gp_margin, "자산총계": assets, "당기순이익": ni, "ROA(%)": roa, "재고회전율": inv_turn})
    base_cols = ["Year", "재고자산", "매출액", "매출총이익", "매출총이익률(%)", "자산총계", "당기순이익", "ROA(%)", "재고회전율", "Date"]
    if len(rows) == 0:
        if strict: raise ValueError("선택한 구간에서 유효한 연도가 없습니다.")
        return pd.DataFrame(columns=base_cols), skipped
    df = pd.DataFrame(rows)
    if "Year" in df.columns: df = df.sort_values("Year")
    df["Date"] = pd.to_datetime(df["Year"].astype(int).astype(str) + "-12-31", errors="coerce")
    need = ["Year", "Date", "재고자산", "자산총계", "당기순이익", "ROA(%)", "재고회전율"]
    if strict and df[need].isna().any().any():
        bad = df[df[need].isna().any(axis=1)]["Year"].tolist()
        raise ValueError(f"[검증 실패] 필수 지표 미산출 연도: {bad}")
    if strict:
        return df, []
    else:
        df = df.dropna(subset=["재고자산", "자산총계", "당기순이익", "ROA(%)", "재고회전율"])
        return df, skipped
# --- [v0.0] DART 함수 끝 ---


# --- [v3.0] SKU/파일 관련 Helper Function ---
def find_best_match(column_list, keywords):
    norm = lambda s: str(s).lower().replace("_", "").replace(" ", "")
    for col in column_list:
        norm_col = norm(col)
        if norm_col in keywords:
            return col
    for keyword in keywords:
        for col in column_list:
            if keyword in norm(col):
                return col
    return ""
# --- [v3.0] SKU 함수 끝 ---


# -------------------------------
# [v4.1] Sidebar (데이터 소스 설정) - "계산" 기능 추가
# -------------------------------
st.sidebar.header("📊 1:1 비교 데이터 설정")

# --- [v4.0] 데이터 소스 1: DART ---
st.sidebar.subheader("데이터 소스 1: 상장사 (DART)")
API_KEY_DART = load_api_key()

if not API_KEY_DART:
    st.sidebar.error(
        "OpenDART API 키가 없습니다.\n"
        "상장사 비교를 위해 API 키가 필요합니다.\n"
        "(환경변수, .env, .streamlit/secrets.toml 중 하나에 'DART_API_KEY' 설정)"
    )
    corp_df = pd.DataFrame() # 빈 DF로 설정
else:
    try:
        with st.spinner("기업 코드 목록 불러오는 중..."):
            corp_df = get_corp_codes(API_KEY_DART)
    except Exception as e:
        st.sidebar.error(f"기업 코드 목록을 가져오지 못했습니다: {e}")
        corp_df = pd.DataFrame()

if 'corp_df' not in locals() or corp_df.empty:
    st.sidebar.warning("DART 기업 코드를 불러올 수 없어 상장사 선택이 비활성화됩니다.")
    name1_dart = ""
    code1 = ""
else:
    q1 = st.sidebar.text_input("비교할 상장사 1곳 검색 (예: 한섬)", "한섬")
    cand1 = search_corp_smart(corp_df, q1, limit=30)
    if cand1.empty:
        st.sidebar.error("기업 검색 결과가 비었습니다.")
        name1_dart = ""
        code1 = ""
    else:
        def _label(row):
            code = row.get("stock_code", "")
            stock = row.get("stock_name", "")
            parts = [row["corp_name"]]
            if code: parts.append(f"[{code}]")
            if stock: parts.append(f"- {stock}")
            return " ".join(parts)
        sel1 = st.sidebar.selectbox("상장사 선택", options=cand1.index.tolist(), format_func=lambda i: _label(cand1.loc[i]))
        code1 = cand1.loc[sel1, "corp_code"]
        name1_dart = cand1.loc[sel1, "corp_name"]

# --- [v4.1] 데이터 소스 2: My Company (파일 업로드) ---
st.sidebar.subheader("데이터 소스 2: 내 가게 (파일)")
name_my_company = st.sidebar.text_input("내 가게 이름", "My Company")
file_data_my = st.sidebar.file_uploader("내 가게의 '연도별 기초' 데이터 파일 (CSV/Excel)", type=["csv", "xlsx"])

# [v4.1] '스마트 열 추천'을 위한 열 매핑 (기초 데이터)
guess_year, guess_inv, guess_sales, guess_gp, guess_cogs, guess_ni, guess_assets = "", "", "", "", "", "", ""
df_preview_my = None

if file_data_my:
    try:
        file_buffer = io.BytesIO(file_data_my.read())
        if file_data_my.name.endswith('.csv'):
            df_preview_my = pd.read_csv(file_buffer, nrows=10)
        else:
            df_preview_my = pd.read_excel(file_buffer, nrows=10)
        file_data_my.seek(0)
        all_columns = df_preview_my.columns.tolist()

        YEAR_KEYWORDS = ['year', '연도', 'yr', '연']
        INV_KEYWORDS = ['재고자산', 'inventory', 'stock_value', 'inv']
        SALES_KEYWORDS = ['매출액', 'sales', 'revenue']
        # [v4.4] GP 키워드 수정 (COGS와 겹칠 수 있으므로)
        GP_KEYWORDS = ['매출총이익', 'gross_profit', 'gp']
        # [v4.1] 비율 계산을 위한 '기초' 데이터 키워드
        COGS_KEYWORDS = ['매출원가', 'cogs', 'cost_of_goods_sold']
        NI_KEYWORDS = ['당기순이익', 'net_income', 'ni']
        ASSETS_KEYWORDS = ['자산총계', 'total_assets', 'assets']
        
        guess_year = find_best_match(all_columns, YEAR_KEYWORDS)
        guess_inv = find_best_match(all_columns, INV_KEYWORDS)
        guess_sales = find_best_match(all_columns, SALES_KEYWORDS)
        guess_gp = find_best_match(all_columns, GP_KEYWORDS)
        guess_cogs = find_best_match(all_columns, COGS_KEYWORDS)
        guess_ni = find_best_match(all_columns, NI_KEYWORDS)
        guess_assets = find_best_match(all_columns, ASSETS_KEYWORDS)
        
    except Exception as e:
        st.sidebar.error(f"파일 미리보기 중 오류: {e}")
        file_data_my = None

st.sidebar.info("파일의 '연도(Year)' 열은 필수입니다. 비율 계산을 위해 '기초' 데이터 열을 매핑하세요.")

with st.sidebar.expander("내 가게 파일 '열(Column)' 매핑하기", expanded=(file_data_my is not None)):
    if df_preview_my is not None:
        st.dataframe(df_preview_my.head(3), use_container_width=True)
        st.caption("⬆️ 자동 감지를 위해 파일 상위 3줄을 미리 봅니다.")
        
    st.markdown("**(필수) 공통**")
    col_my_year = st.text_input("연도 (Year) 열", value=guess_year)
    
    st.markdown("**(필수) 차트 1, 3용**")
    col_my_inv = st.text_input("재고자산 열", value=guess_inv)
    col_my_sales = st.text_input("매출액 열", value=guess_sales)
    
    st.markdown("**(필수) 차트 2용 (비율 계산)**")
    col_my_cogs = st.text_input("매출원가 (COGS) 열", value=guess_cogs)
    col_my_net_income = st.text_input("당기순이익 (NI) 열", value=guess_ni)
    col_my_total_assets = st.text_input("자산총계 (Assets) 열", value=guess_assets)
    # [v4.4] 매출총이익은 이제 '선택' 사항. 없으면 자동 계산됨.
    col_my_gp = st.text_input("매출총이익 (GP) 열 (선택)", value=guess_gp)

# --- [v4.0] 공통 설정 ---
st.sidebar.subheader("공통 분석 설정")
year_start, year_end = st.sidebar.select_slider(
    "분석 연도 구간",
    options=list(range(2015, datetime.now().year + 1)),
    value=(2019, max(2019, datetime.now().year - 1))
)
YEARS = list(range(year_start, year_end + 1))

lenient = st.sidebar.checkbox(
    "Lenient 모드 (DART 결측 연도 제외)",
    value=False,
    help="DART 데이터 수집 시 결측 연도가 있어도 중단하지 않고 계속 진행합니다."
)

forecast_y = st.sidebar.slider(
    "예측 연도 수 (미래)", 
    1, 5, 3, 
    help="[재고 예측 시나리오] 탭에서 사용할 미래 예측 연도 수입니다."
)

st.sidebar.markdown("---")

# --- [v4.4] 데이터 로드 버튼 (GP 계산 로직 추가) ---
if st.sidebar.button("데이터 불러오기 및 비교", type="primary"):
    df_dart = pd.DataFrame()
    df_my_company = pd.DataFrame()
    
    # 1. DART 데이터 로드
    if not code1 or not API_KEY_DART:
        st.sidebar.warning("DART 상장사가 선택되지 않았거나 API 키가 없습니다.")
    else:
        try:
            with st.spinner(f"{name1_dart} DART 데이터 수집/가공 중..."):
                year_map1 = fetch_panel_parallel(API_KEY_DART, code1, YEARS, max_workers=4)
                df_dart, skips1 = extract_metrics(year_map1, YEARS, strict=(not lenient))
                
                if df_dart.empty:
                    st.sidebar.error(f"{name1_dart}의 DART 데이터가 없습니다.")
                else:
                    df_dart["브랜드"] = name1_dart
                    st.session_state.lenient_skips = {'skip1': skips1}
                    st.session_state.df_dart = df_dart
                    st.session_state.name_dart = name1_dart

        except Exception as e:
            st.error(f"DART 데이터 처리 중 오류: {e}")

    # 2. '내 가게' 데이터 로드 및 '계산'
    if not file_data_my:
        st.sidebar.warning("'내 가게' 파일이 업로드되지 않았습니다.")
    elif not col_my_year:
        st.sidebar.error("'내 가게' 파일의 '연도 (Year)' 열을 반드시 매핑해야 합니다.")
    else:
        try:
            with st.spinner(f"'{name_my_company}' 파일 데이터 가공 및 '비율 계산' 중..."):
                df_upload = pd.read_csv(file_data_my) if file_data_my.name.endswith('.csv') else pd.read_excel(file_data_my)
                
                # [v4.1] 필요한 '기초' 열만 매핑
                df_my = pd.DataFrame()
                
                # '연도'는 필수
                df_my["Year"] = pd.to_numeric(df_upload[col_my_year], errors='coerce')

                # [v4.4] 기초 데이터 매핑
                base_map_dict = {
                    "재고자산": col_my_inv,
                    "매출액": col_my_sales,
                    "COGS": col_my_cogs,
                    "NI": col_my_net_income,
                    "Assets": col_my_total_assets,
                    "매출총이익": col_my_gp # [v4.4] GP도 일단 매핑
                }
                
                for key_metric, col_name in base_map_dict.items():
                    if col_name and col_name in df_upload.columns:
                        # [v4.4] GP는 키가 겹치므로 .get()으로 안전하게
                        if col_name in df_upload:
                            df_my[key_metric] = pd.to_numeric(df_upload.get(col_name), errors='coerce')
                
                df_my = df_my.dropna(subset=["Year"])

                # [v4.4] '비율' 및 'GP' 자동 계산
                
                # 1. 재고회전율 = 매출원가 / 재고자산
                if "COGS" in df_my.columns and "재고자산" in df_my.columns:
                    df_my["재고회전율"] = (df_my["COGS"] / df_my["재고자산"]).replace([np.inf, -np.inf], np.nan)
                
                # 2. ROA(%) = (당기순이익 / 자산총계) * 100
                if "NI" in df_my.columns and "Assets" in df_my.columns:
                    df_my["ROA(%)"] = (df_my["NI"] / df_my["Assets"] * 100).replace([np.inf, -np.inf], np.nan)
                
                # 3. [v4.4] 매출총이익 (GP)
                # 만약 GP가 이미 매핑되어 있지 *않다면*, '매출액'과 '매출원가'로 계산
                if "매출총이익" not in df_my.columns:
                    if "매출액" in df_my.columns and "COGS" in df_my.columns:
                        df_my["매출총이익"] = df_my["매출액"] - df_my["COGS"]
                        st.sidebar.success("'매출총이익' 자동 계산 완료 (매출액 - 매출원가)")
                    
                # DART 데이터와 공통 컬럼 설정
                df_my["Date"] = pd.to_datetime(df_my["Year"].astype(int).astype(str) + "-12-31", errors="coerce")
                df_my["브랜드"] = name_my_company
                
                st.session_state.df_my_company = df_my
                st.session_state.name_my_company = name_my_company

        except Exception as e:
            st.error(f"'내 가게' 파일 처리 중 오류: {e}")
            st.exception(e) # [v4.4] 디버그를 위해 오류 상세 표시

    # --- [v4.7] 데이터 통합 및 보정 (버그 수정) ---
    df_concat = pd.concat(
        [st.session_state.df_dart, st.session_state.df_my_company], 
        ignore_index=True
    )

    # [v4.7] !!! 4분면 차트용 데이터 보정 (Key Fix) !!!
    # 4분면 차트에 필요한 6개 열을 정의
    required_cols_for_plot = [
        "Year", "브랜드", "Date", 
        "재고회전율", "ROA(%)", "매출총이익", 
        "재고자산", "매출액"
    ]
    
    # 6개 열이 모두 존재하도록 보장 (없으면 NaN으로 채움)
    for col in required_cols_for_plot:
        if col not in df_concat.columns:
            df_concat[col] = np.nan # 열 자체가 없으면 NaN으로 생성

    st.session_state.df_all = df_concat # 보정된 DF를 세션에 저장
    # --- [v4.7] 수정 끝 ---
    
    if not st.session_state.df_all.empty:
        st.session_state.data_loaded = True
        st.session_state.forecast_y = forecast_y
        st.success("데이터 로드 완료!")
    else:
        st.session_state.data_loaded = False
        st.error("분석할 데이터가 없습니다.")


# -------------------------------
# [v4.0] 메인 패널
# -------------------------------

if not st.session_state.data_loaded:
    st.markdown("---")
    with st.container(border=True):
        st.header("Welcome to the 1:1 Benchmarking Dashboard!")
        st.write("")
        st.write("좌측 사이드바에서 '상장사' 1곳을 선택하고, '내 가게'의 연도별 데이터 파일을 업로드하세요.")
        st.write("그 다음 [데이터 불러오기 및 비교] 버튼을 눌러 분석을 시작하세요.")
        st.write("")
    st.stop()

# [v4.0] 데이터 로드 완료 시
df_all = st.session_state.df_all
name1 = st.session_state.name_dart
name2 = st.session_state.name_my_company
forecast_y = st.session_state.forecast_y
skips = st.session_state.lenient_skips.get('skip1', [])

if skips:
    with st.expander(f"LENIENT 모드: {name1}의 일부 연도 데이터가 제외되었습니다."):
        st.write(f"• {name1} 제외: {skips}")

# --- [v4.0] KPI (두 회사 비교) ---
st.markdown("---")
with st.container(border=True):
    st.subheader("핵심 지표 요약 (선택 기간 평균)")
    col1, col2 = st.columns(2)
    
    df1 = df_all[df_all["브랜드"] == name1]
    df2 = df_all[df_all["브랜드"] == name2]
    
    with col1:
        st.markdown(f"#### {name1} (DART)")
        if not df1.empty:
            df1_inv = pd.to_numeric(df1['재고자산'], errors='coerce').mean()
            df1_roa = pd.to_numeric(df1['ROA(%)'], errors='coerce').mean()
            df1_turn = pd.to_numeric(df1['재고회전율'], errors='coerce').mean()
            st.metric("평균 재고자산", f"{int(df1_inv):,} 원")
            st.metric("평균 ROA(%)", f"{df1_roa:.2f} %")
            st.metric("평균 재고회전율", f"{df1_turn:.2f} 회")
        else: st.warning("데이터 없음")
    with col2:
        st.markdown(f"#### {name2} (My Company)")
        if not df2.empty:
            df2_inv = pd.to_numeric(df2['재고자산'], errors='coerce').mean()
            df2_roa = pd.to_numeric(df2['ROA(%)'], errors='coerce').mean()
            df2_turn = pd.to_numeric(df2['재고회전율'], errors='coerce').mean()
            st.metric("평균 재고자산", f"{int(df2_inv):,} 원" if not pd.isna(df2_inv) else "데이터 없음")
            st.metric("평균 ROA(%)", f"{df2_roa:.2f} %" if not pd.isna(df2_roa) else "데이터 없음")
            st.metric("평균 재고회전율", f"{df2_turn:.2f} 회" if not pd.isna(df2_turn) else "데이터 없음")
        else: st.warning("데이터 없음")

# --- [v4.0] 메인 탭 (기존 탭 1의 하위 탭) ---
with st.container(border=True):
    tab1_1, tab1_2, tab1_3 = st.tabs([
        "지표 탐색", 
        "효율성 vs 수익성 (4분면 분석)", 
        "재고 예측 시나리오"
    ])

    with tab1_1:
        st.subheader("지표 추이 비교")
        # [v4.1] '매출총이익'도 리스트에 추가 (내 가게 파일에서 가져올 수 있으므로)
        indicator = st.selectbox("지표 선택", ["재고자산", "매출액", "매출총이익", "ROA(%)", "재고회전율"], index=0, key="tab1_indicator")
        
        # [v4.2] 로그 스케일 체크박스 추가
        use_log_scale_tab1 = st.checkbox("로그 스케일(Log Scale) 사용 (규모 차이 클 때)", value=True, 
                                         help="금액(예: 매출액, 재고자산)의 규모 차이가 너무 커서 추세 비교가 어려울 때 사용하세요.",
                                         key="log_scale_tab1")

        # [v4.0] 해당 지표가 없는 회사는 제외
        chart_df = df_all.dropna(subset=[indicator])
        
        fig1 = px.line(
            chart_df, x="Date", y=indicator, color="브랜드", markers=True,
            title=f"{indicator} 추이 ({name1} vs {name2})", template="plotly_white"
        )
        
        # [v4.2] 로그 스케일 적용
        if use_log_scale_tab1:
            # [v4.4] 0 또는 음수 값이 있을 수 있으므로 try-except 추가
            try:
                if chart_df[indicator].min() > 0:
                    fig1.update_layout(yaxis_type="log")
                else:
                    st.warning("로그 스케일은 0 또는 음수 값이 있으면 적용할 수 없습니다. (일반 스케일로 표시)")
                    fig1.update_layout(yaxis_type="linear")
            except Exception:
                fig1.update_layout(yaxis_type="linear")
        
        st.plotly_chart(fig1, use_container_width=True)
        with st.expander("비교 데이터 보기"):
            st.dataframe(chart_df.sort_values(["브랜드", "Year"]).reset_index(drop=True), use_container_width=True)
with tab1_2:
    st.subheader("재고회전율 vs ROA (4분면 매트릭스)")
    st.info(
        """
        이 차트는 **4분면 매트릭스**로, 기업의 재고 효율성과 수익성을 한눈에 보여줍니다. 
        점선은 두 기업 데이터의 **평균**을 의미합니다.
        * **X축 (재고회전율):** 높을수록 '효율적' (빨리 판매)
        * **Y축 (ROA):** 높을수록 '수익성'이 높음
        """
    )
    
    chart_df = df_all.copy()

    # 필수 값 채우기
    chart_df["재고회전율"] = chart_df["재고회전율"].fillna(0)
    chart_df["ROA(%)"] = chart_df["ROA(%)"].fillna(0)
    chart_df["매출총이익"] = chart_df["매출총이익"].fillna(1_000_000_000)
    chart_df["재고자산"] = chart_df["재고자산"].fillna(0)
    chart_df["매출액"] = chart_df["매출액"].fillna(0)

    if chart_df.empty:
        st.warning("4분면 차트를 그릴 데이터가 부족합니다. (재고회전율, ROA 필요)")
        st.write("사이드바의 '내 가게 파일 매핑'에서 [재고자산, 매출원가, 당기순이익, 자산총계] 열이 모두 올바르게 매핑되었는지 확인하세요.")
    else:
        # 평균은 원본(df_all) 기준
        x_mean = pd.to_numeric(df_all['재고회전율'], errors='coerce').mean()
        y_mean = pd.to_numeric(df_all['ROA(%)'], errors='coerce').mean()

        x_max_data = pd.to_numeric(chart_df['재고회전율'], errors='coerce').max()
        x_min_data = pd.to_numeric(chart_df['재고회전율'], errors='coerce').min()
        x_min = min(0, x_min_data) - (x_max_data * 0.05)
        x_max = x_max_data * 1.1

        y_max_data = pd.to_numeric(chart_df['ROA(%)'], errors='coerce').max()
        y_min_data = pd.to_numeric(chart_df['ROA(%)'], errors='coerce').min()
        y_min = min(0, y_min_data) - (y_max_data * 0.05)
        y_max = y_max_data * 1.1

        # ① 상장사(한섬 등)만 따로
        api_df = chart_df[chart_df["브랜드"] != name2].copy()
        # ② 내 가게만 따로
        my_df = chart_df[chart_df["브랜드"] == name2].copy()

        # 상장사만 기본 산점도로 그림 (파란 점들)
        fig2 = px.scatter(
            api_df,
            x="재고회전율",
            y="ROA(%)",
            color="브랜드",
            # size="매출총이익",  # ← 이 줄 삭제
            hover_data=["Year", "재고자산", "매출액"],
            title="효율성-수익성 관계 (4분면 분석)",
            template="plotly_white",
            range_x=[x_min, x_max],
            range_y=[y_min, y_max],
        )

        # 상장사 점 크기 통일 (예: 12)
        fig2.update_traces(
            marker=dict(size=16),
            selector=dict(mode="markers")
        )


        # 4분면 영역 & 기준선
        fig2.add_shape(type="rect", layer="below", x0=x_mean, y0=y_mean, x1=x_max, y1=y_max,
                       fillcolor="rgba(110, 200, 110, 0.1)", line_width=0)
        fig2.add_shape(type="rect", layer="below", x0=x_min, y0=y_min, x1=x_mean, y1=y_mean,
                       fillcolor="rgba(200, 110, 110, 0.1)", line_width=0)
        fig2.add_shape(type="rect", layer="below", x0=x_min, y0=y_mean, x1=x_mean, y1=y_max,
                       fillcolor="rgba(200, 200, 200, 0.1)", line_width=0)
        fig2.add_shape(type="rect", layer="below", x0=x_mean, y0=y_min, x1=x_max, y1=y_mean,
                       fillcolor="rgba(200, 200, 200, 0.1)", line_width=0)
        fig2.add_shape(type="line", layer="above", x0=x_mean, y0=y_min, x1=x_mean, y1=y_max,
                       line=dict(color="gray", width=2, dash="dash"))
        fig2.add_shape(type="line", layer="above", x0=x_min, y0=y_mean, x1=x_max, y1=y_mean,
                       line=dict(color="gray", width=2, dash="dash"))

        # 내 가게는 overlay로만, 굵은 빨간 점
        if not my_df.empty:
            fig2.add_scatter(
                x=my_df["재고회전율"],
                y=my_df["ROA(%)"],
                mode="markers",
                name=f"{name2} (내 가게)",
                marker=dict(
                    color="red",
                    size=16,                 # 상장사(12)보다 살짝 크게
                    symbol="circle",
                    line=dict(color="red", width=1.8),
                ),
                hovertext=my_df["Year"],
                showlegend=True,
    )


        st.plotly_chart(fig2, use_container_width=True)

        st.markdown("### 브랜드별 상관계수 (재고회전율 vs ROA)")
        corr_tbl = (
            chart_df.groupby("브랜드")
            .apply(lambda x: round(
                pd.to_numeric(x["재고회전율"], errors='coerce').corr(
                    pd.to_numeric(x["ROA(%)"], errors='coerce')
                ), 3))
            .reset_index(name="상관계수")
        )
        st.dataframe(corr_tbl, use_container_width=True)


    with tab1_3:
        st.subheader(f"재고자산 예측 (향후 {forecast_y}년)")
        
        use_log_transform = st.checkbox("로그 변환(Log Transform) 사용 (모델 안정화)", value=True, 
                                        help="데이터 변동성이 클 경우 이 옵션을 켜면 예측 모델이 더 안정화될 수 있습니다.",
                                        key="log_transform_model")
        
        # [v4.3] 차트 시각화용 로그 스케일
        use_log_scale_tab3 = st.checkbox("로그 스케일(Log Scale) 차트 사용 (규모 차이 비교)", value=True, 
                                         help="두 회사의 재고 규모 차이가 너무 커서 추세 비교가 어려울 때 사용하세요.",
                                         key="log_scale_tab3")
        
        horizon = forecast_y
        fig3 = px.line(title=f"{name1} vs {name2} 재고자산 예측", template="plotly_white")
        has_data = False
        
        # [v4.0] df_all을 순회하며 예측
        for nm in df_all["브랜드"].unique():
            dfb = df_all[df_all["브랜드"] == nm]
            
            ts = dfb[["Date", "재고자산"]].dropna()
            if len(ts) < 6:
                st.warning(f"⚠️ {nm}: Prophet 학습에 데이터가 부족합니다(최소 6 관측 필요). 예측을 건너뜁니다.")
                continue
                
            try:
                df_prophet = ts.rename(columns={"Date": "ds", "재고자산": "y"})
                
                # [v4.4] Prophet도 0이나 음수 값 처리
                if use_log_transform:
                    if (df_prophet['y'] <= 0).any():
                        st.warning(f"⚠️ {nm}: 재고자산에 0 또는 음수 값이 있어 Prophet 로그 변환을 적용할 수 없습니다.")
                        use_log_transform_model = False
                    else:
                        use_log_transform_model = True
                        df_prophet['y'] = np.log(df_prophet['y']) # log1p/expm1 대신 log/exp 사용
                else:
                    use_log_transform_model = False
                
                m = Prophet()
                m.fit(df_prophet) 
                
                fc = m.make_future_dataframe(periods=horizon, freq="Y")
                pred = m.predict(fc)
                
                if use_log_transform_model:
                    pred['yhat'] = np.exp(pred['yhat']) # exp
                    pred['yhat'] = pred['yhat'].clip(lower=0) 

                fig3.add_scatter(x=pred["ds"], y=pred["yhat"], name=f"{nm} (예측)", line=dict(dash='dot'))
                fig3.add_scatter(x=ts["Date"], y=ts["재고자산"], mode='lines+markers', name=f"{nm} (관측)")
                has_data = True
                
            except Exception as e:
                st.error(f"{nm} 예측 중 오류 발생: {e}")
        
        # [v4.3] 로그 스케일 차트 적용
        if use_log_scale_tab3:
            # [v4.4] 0 또는 음수 값이 있을 수 있으므로 try-except 추가
            try:
                # 0보다 큰 값이 하나라도 있는지 확인
                if df_all['재고자산'].dropna().gt(0).any():
                     fig3.update_layout(yaxis_type="log")
                else:
                    st.warning("로그 스케일은 모든 값이 0 또는 음수이므로 적용할 수 없습니다.")
                    fig3.update_layout(yaxis_type="linear")
            except Exception:
                 fig3.update_layout(yaxis_type="linear")

        if has_data:
            st.plotly_chart(fig3, use_container_width=True)
        else:
            st.error("두 기업 모두 예측을 수행할 수 없습니다.")


# --- [v4.0] 캡션 ---
st.markdown("---")
st.caption("© 2025 Team 1 | [v4.8] 1:1 벤치마킹 대시보드 (4분면 Y축 버그 수정)")