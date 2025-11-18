# bungae_clean_v3_10_korcols_i16.py
# -*- coding: utf-8 -*-

import re, json, time, random
from datetime import datetime, timedelta, timezone
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

PLATFORM_NAME = "번개장터"
START_URL = (
    "https://m.bunjang.co.kr/search/products"
    "?category_id=6007000001&order=score&q=%EC%95%84%EC%9D%B4%ED%8F%B0%2016"
)
OUT_CSV = "bunjang_i16_clean.csv"
MAX_ITEMS = 400
SCROLL_PAUSE = 1.0
HEADLESS = True

# ---- 필터 옵션 ----
MIN_PRICE = 250_000
EXCLUDE_NO_REGION = False               # 시/도만 있어도 저장 (필요시 True로)
EXCLUDE_DELIVERY_ONLY = True            # '택배만/직거래 불가' 등 강한 신호만 제외

# ---- 비본체 제외 키워드/패턴 ----
NEGATIVE_KWS = [
    "케이스","case","casetify","케이스티파이","범퍼","링","그립톡","보호필름","필름",
    "강화유리","액정보호","충전기","케이블","어댑터","무선충전","거치대","스트랩",
    "팝소켓","렌즈보호","카메라필름","후면필름","전면필름","배터리케이스","스티커",
    "magsafe","맥세이프","에어태그","케이스만","스트랩세트","카드수납"
]
# iPhone 16 일반만 남기고 Pro/Pro Max/Plus/미니는 제외
EXCLUDE_MODEL = [
    r"\bpro\s*max\b", r"\bpromax\b", "프로맥스", "맥스",
    r"\bplus\b", "플러스",
    r"\bmini\b", "미니",
    r"\bpro\b", "프로"
]

BADGES_TO_STRIP = [
    "검수가능","검수 가능","검수완료","검수 완료",
    "배송비포함","배송비 포함","무료배송",
    "AD","광고","파워링크"
]
HARD_BLOCK = ["매입","매입합니다","사요","삽니다","상사","도매","교환","교환가능","교신"]

# ---- 컬러 표준어/동의어 사전 ----
COLOR_SYNONYMS = [
    (r"\b스그\b", "스페이스그레이"),
    (r"스페이스\s*그레이|스페이스그레이|space\s*gray", "스페이스그레이"),
    (r"그래파이트|graphite", "스페이스그레이"),
    (r"블랙\s*티타늄|블랙티타늄|블랙|검정|black", "블랙"),
    (r"데저트\s*티타늄|데저트", "데저트"),
    (r"화이트|white", "화이트"),
    (r"실버|silver", "실버"),
    (r"그레이|grey|gray", "그레이"),
    (r"골드|gold", "골드"),
    (r"핑크|pink", "핑크"),
    (r"그린|green", "그린"),
    (r"블루|blue", "블루"),
    (r"네이비|navy", "네이비"),
    (r"티타늄", "티타늄"),
]
COLOR_WORDS = [
    "화이트","실버","스페이스그레이","그린","핑크","블랙","골드","로즈",
    "블루","그래파이트","그레이","티타늄","자색","자주","네이비","데저트"
]
ALLOW_COLORS_SET = set(COLOR_WORDS)

# 아이폰 16(일반) 판별: '아이폰 16' / 'iphone 16' 이면서 pro/plus/mini 등 부정 후행 금지
TARGET_PAT = re.compile(
    r"(아이폰\s*16(?!\s*(프로|pro|pro\s*max|promax|플러스|plus|미니))|"
    r"iphone\s*16(?!\s*(pro|pro\s*max|promax|plus|min[iy]?)))",
    re.IGNORECASE
)
NEG_PAT = re.compile("|".join([re.escape(k) for k in NEGATIVE_KWS]), re.IGNORECASE)
EXC_PAT = re.compile("|".join(EXCLUDE_MODEL), re.IGNORECASE)
HARD_BLOCK_PAT = re.compile("|".join([re.escape(k) for k in HARD_BLOCK]), re.IGNORECASE)
BADGE_STRIP_PAT = re.compile("|".join([re.escape(k) for k in BADGES_TO_STRIP]), re.IGNORECASE)
PRICE_PATTERN = re.compile(r"(\d{1,3}(?:,\d{3})+)\s*원")

KST = timezone(timedelta(hours=9))

def rs(a=0.6, b=1.3): time.sleep(random.uniform(a,b))
def clean_text(s: str) -> str:
    if not s: return ""
    s = BADGE_STRIP_PAT.sub(" ", s)
    s = re.sub(r"[\r\n\t]+"," ", s)
    s = re.sub(r"\s{2,}"," ", s)
    return s.strip()
def to_int(s: str) -> int:
    if not s: return 0
    s = re.sub(r"[^\d]","", s)
    return int(s) if s else 0
def is_target_phone(text: str) -> bool:
    if not text: return False
    t = text.lower()
    if not TARGET_PAT.search(t): return False
    if EXC_PAT.search(t): return False
    if NEG_PAT.search(t): return False
    if HARD_BLOCK_PAT.search(t): return False
    return True

# ===== 용량 추출 =====
_RE_TB = re.compile(r"\b(\d+(?:\.\d+)?)\s*(?:TB|티비|테라)\b", re.IGNORECASE)
_RE_GB = re.compile(r"\b(\d{2,4})\s*(?:GB|G\b|기가|지비|기)\b", re.IGNORECASE)
_RE_LABEL_CTX = re.compile(r"(?:용량|저장\s*공간|저장용량|메모리)\s*[:\-]?\s*(?:약\s*)?(\d{2,4})\s*(?:GB|G\b|기가|지비|기)?", re.IGNORECASE)
_RE_BARE_NUMBER = re.compile(r"\b(64|128|256|512|1024)\b")

def _norm_storage_str(val_gb: int):
    if not val_gb: return ("", 0)
    if val_gb >= 1000:
        return ("1TB", 1024) if 1000 <= val_gb < 1200 else (f"{val_gb}GB", val_gb)
    return (f"{val_gb}GB", val_gb)

def storage_from_title(title: str):
    if not title: return ("", 0)
    t = title.strip()
    m = _RE_TB.search(t)
    if m:
        try:
            tb = float(m.group(1))
            if 0.9 <= tb <= 1.1: return ("1TB", 1024)
            gb = int(round(tb * 1024)); return (f"{gb}GB", gb)
        except: pass
    m = _RE_GB.search(t)
    if m:
        gb = int(m.group(1)); return _norm_storage_str(gb)
    m = re.search(r"\b1\s*테라\b", t)
    if m: return ("1TB", 1024)
    return ("", 0)

def storage_from_labeled_text(text: str):
    if not text: return ("", 0)
    t = text.strip()
    m = _RE_TB.search(t)
    if m:
        try:
            tb = float(m.group(1))
            if 0.9 <= tb <= 1.1: return ("1TB", 1024)
            gb = int(round(tb * 1024)); return (f"{gb}GB", gb)
        except: pass
    m = _RE_LABEL_CTX.search(t)
    if m:
        gb = int(m.group(1)); return _norm_storage_str(gb)
    if re.search(r"(용량|저장\s*공간|저장용량|메모리)", t, re.IGNORECASE):
        m2 = _RE_BARE_NUMBER.search(t)
        if m2 and int(m2.group(1)) in (64,128,256,512,1024):
            gb = int(m2.group(1)); return _norm_storage_str(gb)
    return ("", 0)

def storage_from_free_text(text: str):
    if not text: return ("", 0)
    t = text.strip()
    m = _RE_TB.search(t)
    if m:
        try:
            tb = float(m.group(1))
            if 0.9 <= tb <= 1.1: return ("1TB", 1024)
            gb = int(round(tb * 1024)); return (f"{gb}GB", gb)
        except: pass
    m = _RE_GB.search(t)
    if m:
        gb = int(m.group(1)); return _norm_storage_str(gb)
    m = re.search(r"\b1\s*테라\b", t)
    if m: return ("1TB", 1024)
    return ("", 0)

# ===== 컬러 추출 =====
def color_from_text(text: str) -> str:
    if not text: return ""
    t = text.lower()
    for pat, canon in COLOR_SYNONYMS:
        if re.search(pat, t, re.IGNORECASE):
            return canon
    for c in COLOR_WORDS:
        if re.search(re.escape(c), text, re.IGNORECASE):
            return c
    return ""

def norm_title(product_name: str, storage: str, color: str) -> str:
    parts = [product_name]
    if storage: parts.append(storage)
    if color and color in ALLOW_COLORS_SET: parts.append(color)
    return " ".join(parts)

def extract_post_id(url: str) -> str:
    m = re.search(r"/products/(\d+)", url or "")
    return m.group(1) if m else ""

# ===== 지역 파싱 =====
ALLOWED_GU = set("""
강남구 강동구 강북구 강서구 관악구 광진구 구로구 금천구 노원구 도봉구 동대문구 동작구 마포구 서대문구 서초구 성동구 성북구 송파구 양천구 영등포구 용산구 은평구 종로구 중구 중랑구
해운대구 수영구 동래구 연제구 남구 북구 부산진구 사하구 서구 동구 중구 영도구 사상구 금정구 강서구
수성구 달서구 동구 서구 남구 북구 중구
남동구 연수구 부평구 계양구 서구 미추홀구 동구 중구
서구 북구 동구 남구 광산구
서구 유성구 대덕구 중구 동구
남구 동구 북구 중구
팔달구 권선구 장안구 영통구 분당구 중원구 수정구 일산동구 일산서구 덕양구 동안구 만안구 상록구 단원구
기흥구 수지구 처인구
덕진구 완산구
흥덕구 상당구 청원구 서원구
남구 북구
""".split())

TOKEN_RE = re.compile(r"(?<![가-힣])([가-힣]{1,7}(?:구|군|시|동|읍|면))(?![가-힣])")
SIDO_LIST = [
    "서울특별시","부산광역시","대구광역시","인천광역시","광주광역시","대전광역시","울산광역시",
    "세종특별자치시","경기도","강원도","충청북도","충청남도","전라북도","전라남도","경상북도","경상남도",
    "제주특별자치도"
]
SIDO_RE = re.compile("|".join(map(re.escape, SIDO_LIST)))
DONG_RE = re.compile(r"(?<![가-힣])([가-힣]{1,7}(?:동|읍|면))(?![가-힣])")

def extract_region_value_text(soup: BeautifulSoup) -> str:
    lbl = soup.find(string=re.compile(r"^\s*직거래지역\s*$"))
    if lbl:
        for up in [lbl.parent, getattr(lbl.parent, "parent", None), getattr(lbl.parent, "parent", None) and lbl.parent.parent.parent]:
            if not up: continue
            val = up.find(attrs={"class": re.compile(r"Value")})
            if val: return clean_text(val.get_text(" ", strip=True))
            sib = up.find_next_sibling(attrs={"class": re.compile(r"Value")})
            if sib: return clean_text(sib.get_text(" ", strip=True))
    for v in soup.select('div[class*="ProductSummarystyle__Value"], span[class*="ProductSummarystyle__Value"]'):
        label = v.find_previous_sibling()
        if label and re.search(r"직거래지역", label.get_text(" ", strip=True)):
            return clean_text(v.get_text(" ", strip=True))
    around = soup.get_text(" ", strip=True)
    m = re.search(r"직거래지역\s*[:\-]?\s*([^\n]{1,150})", around)
    if m: return clean_text(m.group(0))
    return ""

def parse_region_from_value(value_text: str):
    if not value_text: return ("","","")
    m = SIDO_RE.search(value_text)
    if not m: return ("","","")
    sido = m.group(0)
    tail = value_text[m.end():]
    tokens = TOKEN_RE.findall(tail)
    gu_candidates = [t for t in tokens if t.endswith("구") and (t in ALLOWED_GU)]
    if gu_candidates:
        sigungu = gu_candidates[-1]
    else:
        any_gu = [t for t in tokens if t.endswith("구")]
        if any_gu:
            sigungu = any_gu[-1]
        else:
            city_county = [t for t in tokens if t.endswith("시") or t.endswith("군")]
            sigungu = city_county[-1] if city_county else ""
    m2 = DONG_RE.search(tail)
    dong = m2.group(1) if m2 else ""
    return (sido, sigungu, dong)

def jfind_region_text_any(data) -> str:
    best = ""
    def walk(x):
        nonlocal best
        if isinstance(x, dict):
            for k, v in x.items():
                if re.search(r"(address|area|region|location|address_name|regionName)", str(k), re.IGNORECASE):
                    if isinstance(v, (str, int, float)):
                        s = str(v)
                        if SIDO_RE.search(s) and len(s) > len(최고):
                            best = s
                if isinstance(v, (str, int, float)):
                    s = str(v)
                    if SIDO_RE.search(s) and len(s) > len(최고):
                        best = s
                else:
                    walk(v)
        elif isinstance(x, list):
            for v in x: walk(v)
    walk(data)
    return clean_text(최고)

def scripts_region_fallback(soup: BeautifulSoup) -> str:
    candidates = []
    for sc in soup.find_all("script"):
        txt = sc.string or sc.text or ""
        if not txt: continue
        m = SIDO_RE.search(txt)
        if m:
            start = max(0, m.start()-20)
            end = min(len(txt), m.end()+60)
            seg = clean_text(txt[start:end])
            candidates.append(seg)
    if not candidates: return ""
    candidates.sort(key=len, reverse=True)
    return candidates[0]

DELIVERY_ONLY_RE = re.compile(r"(택배\s*만|택배만|직거래\s*불가|직거래\s*X|대면\s*불가)", re.IGNORECASE)
def is_delivery_only(text: str) -> bool:
    if not text: return False
    return bool(DELIVERY_ONLY_RE.search(text))

# ====== 작성일(절대일자) 변환 ======
REL_RE = re.compile(r"(?P<num>\d+)\s*(?P<unit>분|시간|일|주|개월|달)\s*전")
DATE_RE = re.compile(r"([12]\d{3})[./-](\d{1,2})[./-](\d{1,2})")  # 2025-10-12 등

def parse_any_datetime(raw: str, now_dt: datetime) -> (str, str):
    if not raw:
        return ("", "")
    s = str(raw).strip()

    # epoch
    if re.fullmatch(r"\d{11,13}", s):  # ms
        dt = datetime.fromtimestamp(int(s)/1000, tz=KST)
        return (dt.strftime("%Y-%m-%d"), dt.isoformat())
    if re.fullmatch(r"\d{10}", s):     # sec
        dt = datetime.fromtimestamp(int(s), tz=KST)
        return (dt.strftime("%Y-%m-%d"), dt.isoformat())

    # ISO
    try:
        dt = datetime.fromisoformat(s.replace("Z","+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc).astimezone(KST)
        else:
            dt = dt.astimezone(KST)
        return (dt.strftime("%Y-%m-%d"), dt.isoformat())
    except Exception:
        pass

    # explicit date
    m = DATE_RE.search(s)
    if m:
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        try:
            dt = datetime(y, mo, d, tzinfo=KST)
            return (dt.strftime("%Y-%m-%d"), dt.isoformat())
        except Exception:
            pass

    # relative
    if "방금" in s or "지금" in s:
        dt = now_dt; return (dt.strftime("%Y-%m-%d"), dt.isoformat())
    if "오늘" in s:
        dt = now_dt; return (dt.strftime("%Y-%m-%d"), dt.isoformat())
    if "어제" in s:
        dt = (now_dt - timedelta(days=1)); return (dt.strftime("%Y-%m-%d"), dt.isoformat())

    m = REL_RE.search(s)
    if m:
        num = int(m.group("num")); unit = m.group("unit")
        delta = None
        if unit == "분": delta = timedelta(minutes=num)
        elif unit == "시간": delta = timedelta(hours=num)
        elif unit == "일": delta = timedelta(days=num)
        elif unit == "주": delta = timedelta(weeks=num)
        elif unit in ("개월","달"): delta = timedelta(days=30*num)
        if delta:
            dt = now_dt - delta
            return (dt.strftime("%Y-%m-%d"), dt.isoformat())

    return ("", "")

def find_labeled_datetime_text(soup: BeautifulSoup) -> str:
    label_pat = re.compile(r"(작성일|등록일|게시일|업로드\s*일|올린\s*날짜)")
    for val in soup.select('[class*="Value"], [class*="value"], [class*="content"]'):
        label = val.find_previous_sibling()
        if label and label_pat.search(label.get_text(" ", strip=True)):
            return clean_text(val.get_text(" ", strip=True))
    text = soup.get_text("\n", strip=True)
    m = re.search(r"(작성일|등록일|게시일)\s*[:\-]?\s*([^\n]{4,30})", text)
    if m:
        return clean_text(m.group(2))
    return ""

class Crawler:
    def __init__(self):
        opts = Options()
        if HEADLESS: opts.add_argument("--headless=new")
        opts.add_argument("--no-sandbox"); opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--lang=ko-KR")
        opts.add_experimental_option("excludeSwitches", ["enable-automation"])
        opts.add_experimental_option("useAutomationExtension", False)
        self.drv = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
        self.wait = WebDriverWait(self.drv, 6)

    def close(self):
        try: self.drv.quit()
        except: pass

    def collect_list(self):
        d = self.drv
        d.get(START_URL); rs(1.0,2.0)
        out, seen = [], set()
        last = d.execute_script("return document.body.scrollHeight"); stall = 0

        while len(out) < MAX_ITEMS:
            cards = d.find_elements(By.CSS_SELECTOR, "a[href*='/products/']")
            for a in cards:
                href = a.get_attribute("href")
                if not href or href in seen: continue
                if "/products/new" in href:
                    continue

                price_txt = ""
                try:
                    el = a.find_element(By.XPATH, ".//*[contains(normalize-space(.),'원')]")
                    price_txt = el.text.strip()
                except: pass

                m = PRICE_PATTERN.search(price_txt or "")
                if not m:
                    try:
                        container = a.find_element(By.XPATH, "./ancestor::div[1]")
                        ctext = clean_text(container.text)
                        m = PRICE_PATTERN.search(ctext)
                    except:
                        m = None
                price = to_int(m.group(1)) if m else 0

                if price > 0 and price <= MIN_PRICE:
                    seen.add(href); continue

                title_raw = clean_text(a.text)
                if (price_txt and price_txt in title_raw):
                    title_raw = clean_text(title_raw.replace(price_txt,""))
                title_raw = re.sub(r"\b\d+\s*(분|시간|일)\s*전\b","",title_raw)
                title = clean_text(title_raw)

                img = ""
                try:
                    imgel = a.find_element(By.CSS_SELECTOR, "img")
                    if imgel: img = imgel.get_attribute("src") or ""
                except: pass

                seen.add(href)
                out.append({"href": href, "title": title, "price": price, "img": img})
                if len(out) >= MAX_ITEMS: break

            d.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            rs(SCROLL_PAUSE, SCROLL_PAUSE+0.9)
            h = d.execute_script("return document.body.scrollHeight")
            if h == last:
                stall += 1
                if stall >= 3: break
            else:
                last, stall = h, 0

        return out

    @staticmethod
    def _jfind_first(data, keys):
        keys_lower = [k.lower() for k in keys]
        def walk(x):
            if isinstance(x, dict):
                for k,v in x.items():
                    if str(k).lower() in keys_lower and v not in (None,""):
                        return v
                for v in x.values():
                    r = walk(v)
                    if r is not None: return r
            elif isinstance(x, list):
                for v in x:
                    r = walk(v)
                    if r is not None: return r
            return None
        return walk(data)

    @staticmethod
    def _jfind_storage_any(data):
        found = []
        def walk(x):
            if isinstance(x, dict):
                for k, v in x.items():
                    key_hint = re.search(r"(capacity|storage|option|spec|attribute|variant|title|name|value|desc|description)", str(k), re.IGNORECASE)
                    if isinstance(v, (str, int, float)):
                        txt = str(v)
                        ss, sg = storage_from_labeled_text(txt)
                        if sg == 0:
                            ss, sg = storage_from_free_text(txt)
                        if sg > 0:
                            weight = 2 if key_hint else 1
                            found.append((weight, ss, sg))
                    else:
                        walk(v)
            elif isinstance(x, list):
                for v in x: walk(v)
        walk(x=data)
        if not found: return ("", 0)
        found.sort(key=lambda t: (t[0], t[2]), reverse=True)
        return (found[0][1], found[0][2])

    def enrich(self, row):
        href, list_title, list_price, list_img = row["href"], row["title"], row["price"], row["img"]

        if not is_target_phone(list_title):
            return None
        if list_price > 0 and list_price <= MIN_PRICE:
            return None

        stor_s, stor_g = storage_from_title(list_title)
        color = color_from_text(list_title)
        now_kst = datetime.now(tz=KST)

        rec = {
            "platform": PLATFORM_NAME,
            "post_id": extract_post_id(href),
            "product_id": "",
            "product_name": "아이폰 16",
            "storage": stor_s, "storage_gb": stor_g, "color": color,
            "price_krw": list_price,
            "title": "", "url": href, "image_url": list_img,
            "posted_at_raw": "", "posted_at": "", "posted_at_iso": "",
            "sido": "", "sigungu": "", "dong": "", "region_id": "",
            "scraped_at": now_kst.isoformat()
        }
        rec["product_id"] = rec["post_id"]

        try:
            self.drv.get(href)
            try:
                self.wait.until(
                    EC.any_of(
                        EC.presence_of_element_located((By.XPATH, "//*[contains(text(),'직거래지역')]")),
                        EC.presence_of_element_located((By.CSS_SELECTOR, "[class*='ProductSummarystyle__Value']")),
                        EC.presence_of_element_located((By.ID, "__NEXT_DATA__"))
                    )
                )
            except:
                rs(0.5, 0.8)

            rs(0.2,0.5)
            soup = BeautifulSoup(self.drv.page_source, "html.parser")
            page_txt = soup.get_text(" ", strip=True)

            # JSON (Next.js)
            s = soup.select_one("#__NEXT_DATA__")
            if s and s.text:
                js = json.loads(s.text)

                price_val = self._jfind_first(js, [
                    "price","finalPrice","sale_price","amount","productPrice","discountedPrice"
                ])
                if price_val and rec["price_krw"] == 0:
                    rec["price_krw"] = to_int(str(price_val))

                raw_ts = self._jfind_first(js, ["createdAt","updatedAt"])
                if raw_ts:
                    rec["posted_at_raw"] = str(raw_ts)
                    pd_, iso_ = parse_any_datetime(str(raw_ts), now_kst)
                    rec["posted_at"], rec["posted_at_iso"] = pd_, iso_

                ss_json, sg_json = self._jfind_storage_any(js)
                dt = self._jfind_first(js, ["title","name"])
                ss_t, sg_t = ("", 0)
                if dt:
                    dtc = clean_text(str(dt))
                    ss_t, sg_t = storage_from_title(dtc)
                    if not rec["color"]:
                        rec["color"] = color_from_text(dtc)

                try:
                    options_blob_obj = self._jfind_first(js, ["options","variants","attributes","specs"])
                    options_blob = json.dumps(options_blob_obj, ensure_ascii=False) if options_blob_obj else ""
                    ss_opt, sg_opt = storage_from_free_text(options_blob or "")
                    if not rec["color"]:
                        rec["color"] = color_from_text(options_blob or "")
                except:
                    ss_opt, sg_opt = ("", 0)

                cand = [(sg_t, ss_t, sg_t), (sg_json, ss_json, sg_json), (sg_opt, ss_opt, sg_opt)]
                cand = [c for c in cand if c[0] > 0]
                if cand:
                    cand.sort(key=lambda x: x[0], reverse=True)
                    rec["storage"], rec["storage_gb"] = cand[0][1], cand[0][2]

                region_json_text = jfind_region_text_any(js)
                if region_json_text and not rec["sido"]:
                    s1, g1, d1 = parse_region_from_value(region_json_text)
                    rec["sido"] = rec["sido"] or s1
                    rec["sigungu"] = rec["sigungu"] or g1
                    rec["dong"] = rec["dong"] or d1

            # DOM 지역
            vtxt = extract_region_value_text(soup)
            if vtxt:
                s2, g2, d2 = parse_region_from_value(vtxt)
                rec["sido"] = rec["sido"] or s2
                rec["sigungu"] = rec["sigungu"] or g2
                rec["dong"] = rec["dong"] or d2

            # 작성일 라벨 직접 탐색
            if not rec["posted_at"]:
                labeled = find_labeled_datetime_text(soup)
                if labeled:
                    rec["posted_at_raw"] = rec["posted_at_raw"] or labeled
                    pd_, iso_ = parse_any_datetime(labeled, now_kst)
                    rec["posted_at"], rec["posted_at_iso"] = pd_, iso_

            # 스크립트/텍스트 폴백 지역
            if not rec["sido"]:
                sc_txt = scripts_region_fallback(soup)
                if sc_txt:
                    s3, g3, d3 = parse_region_from_value(sc_txt)
                    rec["sido"] = rec["sido"] or s3
                    rec["sigungu"] = rec["sigungu"] or g3
                    rec["dong"] = rec["dong"] or d3

            if not rec["sido"]:
                m = SIDO_RE.search(page_txt)
                if m:
                    tail = clean_text(page_txt[m.start(): m.end()+80])
                    s4, g4, d4 = parse_region_from_value(tail)
                    rec["sido"] = rec["sido"] or s4
                    rec["sigungu"] = rec["sigungu"] or g4
                    rec["dong"] = rec["dong"] or d4

            # 컬러 보조
            if not rec["color"]:
                meta_texts = []
                for prop in ["og:title","og:description","twitter:title","twitter:description","description"]:
                    tag = soup.find("meta", attrs={"property":prop}) or soup.find("meta", attrs={"name":prop})
                    if tag and tag.get("content"):
                        meta_texts.append(tag["content"])
                rec["color"] = color_from_text(" | ".join(meta_texts))
            if not rec["color"]:
                rec["color"] = color_from_text(page_txt)

            # 용량 보조
            if rec["storage_gb"] == 0:
                meta_texts2 = []
                for prop in ["og:title","og:description","twitter:title","twitter:description","description"]:
                    tag = soup.find("meta", attrs={"property":prop}) or soup.find("meta", attrs={"name":prop})
                    if tag and tag.get("content"):
                        meta_texts2.append(tag["content"])
                ss_meta, sg_meta = storage_from_free_text(" | ".join(meta_texts2))
                if sg_meta > 0:
                    rec["storage"], rec["storage_gb"] = ss_meta, sg_meta
            if rec["storage_gb"] == 0:
                ss_free, sg_free = storage_from_free_text(page_txt)
                if sg_free > 0:
                    rec["storage"], rec["storage_gb"] = ss_free, sg_free

            # 가격/시간 폴백
            if rec["price_krw"] == 0:
                m = PRICE_PATTERN.search(page_txt)
                if m: rec["price_krw"] = to_int(m.group(1))

            if not rec["posted_at"]:
                m = re.search(r'(\d+\s*(?:분|시간|일|주|개월|달)\s*전|오늘|어제|[0-9]{4}[-/\.][0-9]{1,2}[-/\.][0-9]{1,2})', page_txt)
                if m:
                    rec["posted_at_raw"] = rec["posted_at_raw"] or m.group(1)
                    pd_, iso_ = parse_any_datetime(m.group(1), now_kst)
                    rec["posted_at"], rec["posted_at_iso"] = pd_, iso_

            if EXCLUDE_DELIVERY_ONLY and is_delivery_only(page_txt):
                return None

        except Exception:
            pass

        if rec["price_krw"] and rec["price_krw"] <= MIN_PRICE:
            return None
        if EXCLUDE_NO_REGION and not (rec["sido"] and (rec["sigungu"] or rec["dong"])):  # 필요시 활성화
            return None

        rec["title"] = norm_title(rec["product_name"], rec["storage"], rec["color"])
        rec["region_id"] = " ".join([p for p in [rec["sido"], rec["sigungu"], rec["dong"]] if p])

        return rec

    def run(self):
        try:
            lst = self.collect_list()
            print(f"[INFO] 목록 수집: {len(lst)}건")
            out = []
            for i, row in enumerate(lst, 1):
                print(f"[{i}/{len(lst)}] {row['href']}")
                rec = self.enrich(row)
                if rec:
                    out.append(rec)
                if i % 20 == 0:
                    self._save(out)
                rs(0.7, 1.3)
            self._save(out)
            print(f"[DONE] {len(out)} rows -> {OUT_CSV}")
        finally:
            self.close()

    @staticmethod
    def _save(rows):
        if not rows: 
            print("[SAVE] 0 rows (skip)")
            return

        df = pd.DataFrame(rows)

        # 한국어 헤더 매핑 및 컬럼 순서 지정
        cols_map = {
            "platform": "플랫폼명",
            "post_id": "게시글_ID",
            "price_krw": "가격",
            "url": "URL",
            "product_name": "모델명",
            "title": "제목",
            "storage": "용량",
            "sido": "시도",
            "sigungu": "시군구",
            "dong": "동읍면",
            "color": "색상",
            "posted_at": "작성일",
        }

        # 타입 보정
        if "post_id" in df.columns:
            df["post_id"] = df["post_id"].astype(str)

        base_cols = list(cols_map.keys())
        exist_cols = [c for c in base_cols if c in df.columns]
        df_out = df[exist_cols].rename(columns=cols_map)

        final_order = ["플랫폼명","게시글_ID","가격","URL","모델명","제목","용량","시도","시군구","동읍면","색상","작성일"]
        final_exist = [c for c in final_order if c in df_out.columns]
        df_out = df_out.reindex(columns=final_exist)

        df_out.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
        print(f"[SAVE] {len(df_out)} -> {OUT_CSV}")

if __name__ == "__main__":
    Crawler().run()
