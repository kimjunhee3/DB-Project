import streamlit as st
import sqlite3
import datetime
import pandas as pd
import plotly.graph_objects as go
from PIL import Image, ImageDraw, ImageFont 
import io 
from pathlib import Path 
import unicodedata 
import re 

# --- [1] 페이지 설정 (최상단 고정) ---
st.set_page_config(page_title="중고 아이폰 분석", layout="wide")

# --- CSS 스타일 (카드 디자인 적용) ---
st.markdown("""
<style>
    /* 전체 배경색과 폰트 설정 (선택 사항) */
    .block-container {
        padding-top: 2rem;
    }

    /* KPI 카드 공통 스타일 */
    .kpi-card {
        background-color: #ffffff;
        border-radius: 15px;
        padding: 20px;
        margin: 10px 0;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1); /* 부드러운 그림자 */
        text-align: center;
        transition: transform 0.2s; /* 호버 효과를 위한 전환 */
    }
    
    .kpi-card:hover {
        transform: translateY(-5px); /* 마우스 올리면 살짝 떠오름 */
        box-shadow: 0 10px 15px rgba(0, 0, 0, 0.15);
    }

    /* 카드별 상단 포인트 컬러 */
    .card-blue { border-top: 5px solid #3498db; }
    .card-green { border-top: 5px solid #2ecc71; }
    .card-purple { border-top: 5px solid #9b59b6; }

    /* 텍스트 스타일 */
    .kpi-title {
        font-size: 1.1rem;
        color: #7f8c8d;
        font-weight: 600;
        margin-bottom: 10px;
    }
    
    .kpi-value {
        font-size: 2.2rem;
        font-weight: 800;
        color: #2c3e50;
        margin: 0;
    }
    
    .kpi-caption {
        font-size: 0.85rem;
        color: #95a5a6;
        margin-top: 5px;
    }
</style>
""", unsafe_allow_html=True)
# --------------------

# --- [2] 파일 경로 설정 ---
BASE_DIR = Path(__file__).parent 
MAP_FILE_PATH = BASE_DIR / "dong_gu_map.csv" 
# ✅ 코드 파일과 같은 위치에서 찾도록 수정
DB_FILE = BASE_DIR / "project2.db"

# --- [3] 지도 좌표 설정 (사용자 지정 좌표 유지) ---
SEOUL_GU_COORDINATES = {
    '도봉구': (285, 60),  '노원구': (322, 80),  '강북구': (258, 88),
    '은평구': (167, 106),  '성북구': (258, 140), '중랑구': (340, 147),
    '서대문구': (178, 165), '종로구': (225, 160), '동대문구': (303, 160),
    '마포구': (148, 187), '중구': (243, 190),   '성동구': (290, 202),
    '광진구': (335, 210), '강동구': (395, 198),
    '강서구': (55, 180),  '양천구': (90, 240),  '구로구': (75, 270),
    '영등포구': (146, 230), '동작구': (190, 258), '용산구': (225, 230),
    '금천구': (130, 300), '관악구': (190, 300), '서초구': (255, 283),
    '강남구': (305, 265), '송파구': (360, 250)
}


# --- DB 연결 ---
def get_db_connection():
    try:
        conn = sqlite3.connect(f"file:{DB_FILE}?mode=ro", uri=True) 
        return conn
    except Exception as e:
        st.error(f"DB 연결 오류: {e}")
        return None

# --- 정규화 함수 ---
def normalize_key(text):
    if pd.isna(text) or text == "": return None
    text = str(text)
    text = unicodedata.normalize('NFC', text)
    text = re.sub(r'[^가-힣a-zA-Z0-9]', '', text)
    return text

# --- 쿼리 생성 헬퍼 ---
def build_dynamic_query_parts(platform, model, start_date, end_date):
    params = []
    where_clause = " WHERE p.posted_date BETWEEN ? AND ? "
    params.extend([str(start_date), str(end_date)])

    if platform != '전체':
        where_clause += " AND pf.name = ? "
        params.append(platform)

    if model == 'iPhone 16 Pro':
        where_clause += " AND (pr.model LIKE ? OR pr.model LIKE ?) "
        params.extend(['%iPhone 16 Pro%', '%아이폰 16 프로%'])
    elif model == 'iPhone 16':
        where_clause += " AND (pr.model LIKE ? OR pr.model LIKE ?) "
        params.extend(['%iPhone 16%', '%아이폰 16%'])
        where_clause += " AND pr.model NOT LIKE '%Pro%' AND pr.model NOT LIKE '%프로%' "
    elif model == 'iPhone 15 Pro':
        where_clause += " AND (pr.model LIKE ? OR pr.model LIKE ?) "
        params.extend(['%iPhone 15 Pro%', '%아이폰 15 프로%'])
    elif model == 'iPhone 15': 
        where_clause += " AND (pr.model LIKE ? OR pr.model LIKE ?) "
        params.extend(['%iPhone 15%', '%아이폰 15%'])
        where_clause += " AND pr.model NOT LIKE '%Pro%' AND pr.model NOT LIKE '%프로%' "
    elif model == 'iPhone 14 Pro':
        where_clause += " AND (pr.model LIKE ? OR pr.model LIKE ?) "
        params.extend(['%iPhone 14 Pro%', '%아이폰 14 프로%'])
    elif model == 'iPhone 14': 
        where_clause += " AND (pr.model LIKE ? OR pr.model LIKE ?) "
        params.extend(['%iPhone 14%', '%아이폰 14%'])
        where_clause += " AND pr.model NOT LIKE '%Pro%' AND pr.model NOT LIKE '%프로%' "
    return where_clause, params

# --- KPI 함수 ---
@st.cache_data
def fetch_kpi_and_ids(platform, model, start_date, end_date):
    where_clause, params = build_dynamic_query_parts(platform, model, start_date, end_date)
    sql = f"""
    SELECT COUNT(p.post_id), AVG(p.price_krw), GROUP_CONCAT(p.post_id, ', ')
    FROM posts AS p
    JOIN platforms AS pf ON p.platform_id = pf.platform_id
    JOIN products AS pr ON p.product_id = pr.product_id
    {where_clause}
    """
    conn = get_db_connection()
    if conn is None: return 0, 0, []
    try:
        cursor = conn.cursor()
        cursor.execute(sql, params)
        result = cursor.fetchone()
        if result: return (result[0] or 0, result[1] or 0, [])
    except Exception: pass
    finally: conn.close()
    return 0, 0, []

# --- 매핑 사전 로드 ---
def load_mapping_dict(map_file_path):
    mapping_dict = {}
    if not map_file_path.exists(): return mapping_dict
    try:
        try: map_df = pd.read_csv(map_file_path, encoding='utf-8-sig')
        except: map_df = pd.read_csv(map_file_path, encoding='cp949')
        
        map_df.columns = map_df.columns.str.strip()
        for _, row in map_df.iterrows():
            key = normalize_key(row['dong'])
            if key: mapping_dict[key] = str(row['sigungu']).strip()
    except: pass
    return mapping_dict

# --- 지역별 데이터 함수 ---
@st.cache_data
def fetch_regional_data(platform, model, start_date, end_date, map_file_path):
    where_clause, params = build_dynamic_query_parts(platform, model, start_date, end_date)
    sql = f"""
    SELECT p.post_id, r.sigungu, r.dong 
    FROM posts AS p
    JOIN platforms AS pf ON p.platform_id = pf.platform_id
    JOIN products AS pr ON p.product_id = pr.product_id
    JOIN regions AS r ON p.region_id = r.region_id
    {where_clause}
    """
    conn = get_db_connection()
    if conn is None: return pd.DataFrame(columns=['sigungu', 'count'])
    try: df = pd.read_sql_query(sql, conn, params=params)
    except: return pd.DataFrame(columns=['sigungu', 'count'])
    finally: conn.close()
    if df.empty: return pd.DataFrame(columns=['sigungu', 'count'])

    mapping_dict = load_mapping_dict(map_file_path)

    def fill_missing_gu(row):
        if row['sigungu'] and str(row['sigungu']).strip() not in ['None', 'nan', '']:
            return row['sigungu']
        clean_dong = normalize_key(row['dong'])
        if not clean_dong: return "지역 미기재"
        return mapping_dict.get(clean_dong, "지역 미기재")

    df['final_gu'] = df.apply(fill_missing_gu, axis=1)
    
    result_df = df.groupby('final_gu').size().reset_index(name='count')
    result_df = result_df.rename(columns={'final_gu': 'sigungu'})
    result_df = result_df.sort_values('count', ascending=False)
    return result_df

# --- 매핑 실패(미기재) 상세 목록 함수 ---
@st.cache_data
def fetch_unmapped_details(platform, model, start_date, end_date, map_file_path):
    where_clause, params = build_dynamic_query_parts(platform, model, start_date, end_date)
    sql = f"""
    SELECT r.dong 
    FROM posts AS p
    JOIN platforms AS pf ON p.platform_id = pf.platform_id
    JOIN products AS pr ON p.product_id = pr.product_id
    JOIN regions AS r ON p.region_id = r.region_id
    {where_clause}
    AND (r.sigungu IS NULL OR r.sigungu = '')
    """
    conn = get_db_connection()
    if conn is None: return pd.DataFrame()
    df = pd.read_sql_query(sql, conn, params=params)
    conn.close()
    
    if df.empty:
        return pd.DataFrame(columns=['동 이름(원본)', '매물 수'])

    mapping_dict = load_mapping_dict(map_file_path)
    
    df['dong'] = df['dong'].fillna("(지역 정보 없음)")
    df.loc[df['dong'].astype(str).str.strip() == '', 'dong'] = "(지역 정보 없음)"
    
    def is_unmapped(row):
        if row['dong'] == "(지역 정보 없음)": return True
        clean_dong = normalize_key(row['dong'])
        if not clean_dong: return True 
        return clean_dong not in mapping_dict 

    unmapped = df[df.apply(is_unmapped, axis=1)]
    
    if unmapped.empty:
        return pd.DataFrame(columns=['동 이름(원본)', '매물 수'])

    result = unmapped['dong'].value_counts().reset_index()
    result.columns = ['동 이름(원본)', '매물 수']
    return result

# --- 플랫폼 데이터 함수 ---
@st.cache_data
def fetch_platform_data(model, start_date, end_date):
    where_clause, params = build_dynamic_query_parts('전체', model, start_date, end_date)
    sql = f"""
    SELECT pf.name, COUNT(p.post_id) as count
    FROM posts AS p
    JOIN platforms AS pf ON p.platform_id = pf.platform_id
    JOIN products AS pr ON p.product_id = pr.product_id
    {where_clause}
    GROUP BY pf.name ORDER BY count DESC
    """
    conn = get_db_connection()
    if conn is None: return pd.DataFrame(columns=['name', 'count'])
    try: return pd.read_sql_query(sql, conn, params=params)
    finally: conn.close()

# --- 가격 추이 함수 ---
@st.cache_data
def fetch_price_trend_data(platform, model, start_date, end_date):
    where_clause, params = build_dynamic_query_parts(platform, model, start_date, end_date)
    sql = f"""
    SELECT p.posted_date, AVG(p.price_krw) as avg_price
    FROM posts AS p
    JOIN platforms AS pf ON p.platform_id = pf.platform_id
    JOIN products AS pr ON p.product_id = pr.product_id
    {where_clause}
    GROUP BY p.posted_date ORDER BY p.posted_date ASC
    """
    conn = get_db_connection()
    if conn is None: return pd.DataFrame(columns=['posted_date', 'avg_price'])
    try: return pd.read_sql_query(sql, conn, params=params)
    finally: conn.close()

# --- 💡 [수정] 지도 이미지 함수 (색상 로직: 초록 -> 노랑 -> 빨강) ---
def generate_map_overlay(region_df):
    try:
        base_image = Image.open("서울지도보기.jpg").convert("RGBA")
        draw = ImageDraw.Draw(base_image)
    except FileNotFoundError: return None 
    
    valid_df = region_df[region_df['sigungu'].isin(SEOUL_GU_COORDINATES.keys())]
    if valid_df.empty: return base_image 
    
    REF_MAX_COUNT = 40.0 
    
    for _, row in valid_df.iloc[::-1].iterrows():
        gu_name = row['sigungu']
        count = row['count']
        
        coords = SEOUL_GU_COORDINATES.get(gu_name)
        if coords: 
            x, y = coords
            
            # 비율
            ratio = min(count / REF_MAX_COUNT, 1.0)
            
            # 크기: 5px ~ 25px
            radius = 5 + (ratio * 20)
            
            # 💡 색상: 초록(적음) -> 노랑(중간) -> 빨강(많음)
            if count <= 5:
                outline_color = (0, 200, 0, 255) # 진한 초록
            elif count <= 15:
                outline_color = (255, 215, 0, 255) # 진한 노랑(Gold)
            else:
                outline_color = (255, 0, 0, 255) # 빨강
            
            # 원 그리기 (내부 비움)
            draw.ellipse(
                (x - radius, y - radius, x + radius, y + radius), 
                fill=None, 
                outline=outline_color, 
                width=3 
            )
            
            # 숫자 표시
            if radius > 8:
                try:
                    font = ImageFont.load_default() 
                    text = str(count)
                    text_w = len(text) * 6 
                    text_h = 10
                    # 글자도 테두리 색과 동일하게
                    draw.text((x - text_w/2, y - text_h/2), text, fill=outline_color, font=font, stroke_width=0)
                except: pass

    img_buffer = io.BytesIO()
    base_image.save(img_buffer, format='PNG')
    img_buffer.seek(0)
    return img_buffer

# --- Plotly 한글 설정 ---
plotly_config = {
    'displaylogo': False,
    'modeBarButtonsToRemove': ['select2d', 'lasso2d'],
    'locale': 'ko', 
    'toImageButtonOptions': {'format': 'png', 'filename': 'custom_image', 'height': 500, 'width': 700, 'scale': 1},
}

# --- UI 메인 ---
st.title('📱 중고 아이폰 시장 분석 대시보드')
st.caption("플랫폼, 기종, 지역별 데이터를 기반으로 시장 동향을 분석합니다.")

with st.container(border=True):
    col1, col2, col3, col4 = st.columns([1.3, 1.5, 2, 1.2]) 
    with col1: platform = st.radio("**플랫폼**", options=['전체', '당근마켓', '중고나라', '번개장터'], index=2, horizontal=True)
    with col2: model = st.selectbox("**아이폰 기종**", options=['iPhone 14', 'iPhone 14 Pro', 'iPhone 15', 'iPhone 15 Pro', 'iPhone 16', 'iPhone 16 Pro'], index=5)
    with col3: date_range = st.date_input("**기간**", value=(datetime.date(2025, 10, 3), datetime.date(2025, 11, 9)), format="YYYY-MM-DD")
    with col4: st.write(""); analysis_button = st.button("🔍 분석 실행", type="primary", use_container_width=True)

st.divider() 
# 💡 디자인 변경: KPI 컨테이너의 border를 제거하여 카드 그림자가 더 잘 보이게 함
kpi_container = st.container() 
chart_container = st.container()

if analysis_button and len(date_range) == 2:
    start_date, end_date = date_range
    
    total_count, avg_price, id_list = fetch_kpi_and_ids(platform, model, start_date, end_date)
    region_df = fetch_regional_data(platform, model, start_date, end_date, MAP_FILE_PATH)
    platform_df = fetch_platform_data(model, start_date, end_date) 
    price_trend_df = fetch_price_trend_data(platform, model, start_date, end_date) 
    map_image = generate_map_overlay(region_df)
    
    unmapped_details_df = fetch_unmapped_details(platform, model, start_date, end_date, MAP_FILE_PATH)

    # 💡 [디자인 수정] HTML/CSS를 활용한 카드형 레이아웃 적용
    with kpi_container:
        kpi_space_left, kpi1, kpi2, kpi3, kpi_space_right = st.columns([0.5, 2, 2, 2, 0.5])
        
        # 최다 거래 지역 계산
        valid_regions = region_df[region_df['sigungu'] != '지역 미기재']
        most_frequent_region = valid_regions.iloc[0]['sigungu'] if not valid_regions.empty else "-"
        most_frequent_count = valid_regions.iloc[0]['count'] if not valid_regions.empty else 0

        # --- KPI 1: 총 매물 수 (Blue Card) ---
        with kpi1: 
            st.markdown(f"""
            <div class="kpi-card card-blue">
                <div class="kpi-title">📦 총 매물 수</div>
                <div class="kpi-value">{total_count:,.0f} 건</div>
                <div class="kpi-caption">선택 기간 내 전체 매물</div>
            </div>
            """, unsafe_allow_html=True)
        
        # --- KPI 2: 평균 가격 (Green Card) ---
        with kpi2: 
            st.markdown(f"""
            <div class="kpi-card card-green">
                <div class="kpi-title">💰 평균 가격</div>
                <div class="kpi-value">{avg_price:,.0f} 원</div>
                <div class="kpi-caption">기간 내 전체 매물의 평균값</div>
            </div>
            """, unsafe_allow_html=True)

        # --- KPI 3: 최다 거래 지역 (Purple Card) ---
        with kpi3: 
            region_caption = f"총 {most_frequent_count}건" if most_frequent_region != "-" else "-"
            st.markdown(f"""
            <div class="kpi-card card-purple">
                <div class="kpi-title">🗺️ 최다 거래 지역</div>
                <div class="kpi-value">{most_frequent_region}</div>
                <div class="kpi-caption">{region_caption}</div>
            </div>
            """, unsafe_allow_html=True)
        
        st.write("") # 여백 추가

    with chart_container:
        chart_col1, chart_col2 = st.columns(2)
        with chart_col1:
            st.subheader("📍 지역별 매물 분포 (전체)")
            with st.container(border=True):
                if map_image:
                    # 지도 이미지는 가운데 정렬 효과를 위해 컬럼 사용
                    c1, c2, c3 = st.columns([1, 8, 1])
                    with c2:
                        st.image(map_image, use_container_width=True)
                    
                    if not region_df.empty:
                        display_df = region_df[region_df['sigungu'] != '지역 미기재'].copy()
                        
                        displayed_count = display_df['count'].sum()
                        if displayed_count < total_count:
                            null_count = total_count - displayed_count
                            null_row = pd.DataFrame({'sigungu': ['NULL 값 존재'], 'count': [null_count]})
                            display_df = pd.concat([display_df, null_row], ignore_index=True)

                        # 💡 [수정] 데이터프레임 출력 방식 개선 (꽉 차게)
                        st.dataframe(
                            display_df.rename(columns={"sigungu": "구"}).set_index('구'), 
                            column_config={
                                "구": st.column_config.TextColumn("구", width="medium"),
                                "count": st.column_config.NumberColumn("매물 수", format="%d건")
                            }, 
                            use_container_width=True,
                            height=300
                        )
                        
                        unknown_count = region_df[region_df['sigungu'] == '지역 미기재']['count'].sum()
                        if unknown_count > 0:
                            st.divider()
                            st.warning(f"⚠️ **지역 미기재 데이터: 총 {unknown_count}건**")
                            with st.expander("🔻 미기재 상세 내역 보기 (동 이름)"):
                                if not unmapped_details_df.empty:
                                    st.markdown("##### 🚨 잘못 입력한 '동' 목록")
                                    st.dataframe(
                                        unmapped_details_df.set_index("동 이름(원본)"), 
                                        use_container_width=True
                                    )
                else: st.error("서울지도보기.jpg 없음")

        with chart_col2:
            st.subheader("📊 플랫폼별 현황")
            with st.container(border=True): 
                if not platform_df.empty:
                    color_map = {'중고나라': '#77DD77', '번개장터': '#FF6961', '당근마켓': '#FFB347'}
                    platform_colors = [color_map.get(name, '#D3D3D3') for name in platform_df['name']]
                    
                    pull_values = [0.0] * len(platform_df)
                    line_widths = [0] * len(platform_df)
                    line_colors = ['#FFFFFF'] * len(platform_df)
                    
                    if platform != '전체':
                        try:
                            idx = platform_df[platform_df['name'] == platform].index[0]
                            pull_values[idx] = 0.1
                            line_widths[idx] = 2
                            line_colors[idx] = '#000000'
                        except: pass 

                    fig = go.Figure(data=[go.Pie(
                        labels=platform_df['name'], 
                        values=platform_df['count'], 
                        hole=.4, 
                        pull=pull_values, 
                        textinfo='label+percent',
                        texttemplate="%{label}<br>%{percent:.1%}",
                        textposition='inside',
                        hovertemplate="<b>%{label}</b><br>매물 수: %{value}건<br>비율: %{percent}<extra></extra>",
                        marker=dict(colors=platform_colors, line=dict(color=line_colors, width=line_widths))
                    )])
                    fig.update_layout(
                        margin=dict(l=0, r=0, t=0, b=0), 
                        legend=dict(orientation="h", yanchor="bottom", y=-0.1, xanchor="center", x=0.5),
                        annotations=[dict(text='플랫폼', x=0.5, y=0.5, font_size=16, showarrow=False)]
                    )
                    st.plotly_chart(fig, use_container_width=True, config=plotly_config)
                else: st.info("데이터 없음")
            
            st.subheader("📈 일별 평균 가격 변동") 
            with st.container(border=True):
                if not price_trend_df.empty:
                    fig_line = go.Figure(data=go.Scatter(
                        x=price_trend_df['posted_date'], 
                        y=price_trend_df['avg_price'], 
                        mode='lines+markers',
                        hovertemplate="평균가: %{y:,.0f}원<extra></extra>"
                    ))
                    fig_line.update_layout(
                        margin=dict(l=0, r=0, t=20, b=0), 
                        height=300, 
                        hovermode="x unified",
                        xaxis=dict(tickformat="%Y-%m-%d", hoverformat="%Y년 %m월 %d일")
                    )
                    st.plotly_chart(fig_line, use_container_width=True, config=plotly_config)
                else: st.info("데이터 없음")
