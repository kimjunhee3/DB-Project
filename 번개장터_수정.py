import requests
import json
import time # API 요청 사이에 지연을 주기 위해 time 모듈 추가
from datetime import datetime # Unix timestamp 변환을 위해 추가
import csv  # <-- CSV 저장을 위해 추가
import os   # <-- 파일 존재 여부 확인을 위해 추가

#
# ========== Helper 함수 정의 ==========
#

def parse_capacity(name_str_lower):
    """상품명(소문자)에서 용량 키워드를 찾아 반환합니다."""
    if "128g" in name_str_lower or "128기가" in name_str_lower:
        return "128GB"
    if "256g" in name_str_lower or "256기가" in name_str_lower:
        return "256GB"
    if "512g" in name_str_lower or "512기가" in name_str_lower:
        return "512GB"
    if "1tb" in name_str_lower or "1테라" in name_str_lower:
        return "1TB"
    return "" # 찾지 못하면 빈칸

def parse_color(name_str_lower):
    """상품명(소문자)에서 색상 키워드를 찾아 표준 색상명으로 반환합니다."""
    # (탐지할 키워드 목록)
    colors = {
        "블랙": ["블랙", "black", "스페이스블랙"],
        "화이트": ["화이트", "white", "스타라이트"],
        "블루": ["블루", "blue", "시에라블루"],
        "그린": ["그린", "green", "알파인그린"],
        "레드": ["레드", "red", "프로덕트"],
        "핑크": ["핑크", "pink"],
        "옐로우": ["옐로우", "yellow"],
        "퍼플": ["퍼플", "purple", "딥퍼플"],
        "데저트": ["데저트", "desert", "티타늄", "네추럴", "natural"] # 이미지 예시 참고
    }
    for kor_color, terms in colors.items():
        for term in terms:
            if term in name_str_lower:
                return kor_color
    return "" # 찾지 못하면 빈칸

def parse_location(location_str):
    """위치 문자열을 '시도', '시군구', '동읍면'으로 분리합니다."""
    parts = location_str.split()
    sido = parts[0] if len(parts) > 0 else ""
    sigungu = parts[1] if len(parts) > 1 else ""
    dong = " ".join(parts[2:]) if len(parts) > 2 else "" # '방배 1동'처럼 띄어쓰기 가능성
    return sido, sigungu, dong

# ========== Helper 함수 정의 끝 ==========
#


# 1. API 기본 주소
base_url = "https://api.bunjang.co.kr/api/1/find_v2.json"

# 2. API에 전달할 파라미터들 (기본틀)
params = {
    'q': '아이폰 16',  # 검색어
    'order': 'date',
    'page': 0,         # 0이 첫 번째 페이지 (이 값은 반복문에서 변경됨)
    'n': 100,          # 한 페이지에 가져올 아이템 수
    'stat_device': 'w',
    'req_ref': 'search',
    'version': 5,
    'category_id': '600700001'
}

# 3. API 요청 헤더
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'
}

print("====== 번개장터 '아이폰 16' 크롤링 시작 (필터: '서울특별시' + '일반 모델') ======")

# 서울특별시 매물만 카운트하기 위한 변수
total_seoul_products_found = 0
# 어떤 CSV 파일이 생성되었는지 추적하기 위한 Set
created_csv_files = set()

# 4. 10페이지까지 반복 (0, 1, 2, ..., 9)
for page_num in range(10):
    
    # params 딕셔너리의 'page' 값을 현재 페이지 번호로 업데이트
    params['page'] = page_num
    
    print(f"\n======= '아이폰 16' {page_num + 1}번째 페이지 (page={page_num}) 요청 =======\n")
    
    # 이번 페이지에서 찾은 서울 매물 수
    page_seoul_products_count = 0

    try:
        # 5. API 요청 보내기
        response = requests.get(base_url, params=params, headers=headers)
        response.raise_for_status() # 오류가 있으면 예외 발생

        # 6. 응답 결과를 JSON 형태로 변환
        data = response.json()

        # 7. 데이터에서 상품 목록(list)만 추출
        if 'list' in data and data['list']:
            products = data['list']
            
            print(f"--- (상품 {len(products)}개 발견, '서울특별시' + '일반 모델' 필터링 시작) ---")

            # 8. *해당 페이지의* 모든 상품 정보 출력
            for product in products:
                
                # pid가 없는 광고성 항목은 건너뜀
                if product.get('pid', 'PID 없음') == 'PID 없음':
                    continue 

                # ******** 1. '서울특별시' 필터링 로직 ********
                location_string = product.get('location', '')
                if "서울특별시" not in location_string:
                    continue 
                # ******** 필터링 로직 끝 ********
                
                # ******** 2. '아이폰 16' (Pro/Max 제외) 필터링 로직 ********
                product_name_for_filter = product.get('name', '').lower() 
                if ("아이폰 16" not in product_name_for_filter and "아이폰16" not in product_name_for_filter):
                    continue 
                if ("프로" in product_name_for_filter or "pro" in product_name_for_filter):
                    continue
                if ("맥스" in product_name_for_filter or "max" in product_name_for_filter):
                    continue
                if "플러스" in product_name_for_filter or "plus" in product_name_for_filter:
                    continue
                # ******** 필터링 로직 끝 ********

                # (여기까지 코드가 왔다면 모든 필터를 통과한 매물)
                page_seoul_products_count += 1
                total_seoul_products_found += 1

                #
                # ******** 3. CSV 저장을 위한 데이터 가공 ********
                #
                
                # 3-1. 기본 정보 추출
                platform = "번개장터"
                product_id = product.get('pid')
                product_price = product.get('price', '0') # 가격 없으면 0
                product_name = product.get('name', '이름 없음')
                
                # 3-2. URL 생성
                url = f"https://m.bunjang.co.kr/product/{product_id}"
                
                # 3-3. 모델명 고정 (필터링 기준)
                model_name = "아이폰 16"
                
                # 3-4. 위치 파싱
                (sido, sigungu, dong) = parse_location(location_string)
                
                # 3-5. 용량/색상 파싱
                product_name_lower = product_name.lower()
                capacity = parse_capacity(product_name_lower)
                color = parse_color(product_name_lower)
                
                # 3-6. 날짜/시간 변환
                product_update_time_raw = product.get('update_time')
                csv_date_str = "날짜 정보 없음"
                csv_filename_date = "unknown_date"
                if product_update_time_raw:
                    try:
                        dt_object = datetime.fromtimestamp(int(product_update_time_raw))
                        # 이미지와 동일한 "YYYY-MM-DD" 형식
                        csv_date_str = dt_object.strftime("%Y-%m-%d") 
                        # 파일명에 사용할 형식 (슬래시 제외)
                        csv_filename_date = dt_object.strftime("%Y_%m_%d")
                    except (ValueError, TypeError, OSError):
                        pass # 기본값 사용
                
                # ******** 4. CSV 저장 로직 (신규 헤더/데이터 적용) ********
                #
                try:
                    # 1. 날짜로 파일명 만들기 (예: bunjang_2025_11_09.csv)
                    csv_filename = f"bunjang_{csv_filename_date}.csv"
                    
                    # 2. 파일이 존재하는지 확인 (헤더 중복 방지용)
                    file_exists = os.path.exists(csv_filename)
                    
                    # 3. CSV 파일 헤더 (이미지 기준)
                    header_row = [
                        '플랫폼', '게시글_ID', '가격', 'URL', '모델명', '제목', '용량', 
                        '시도', '시군구', '동읍면', '색상', '작성일'
                    ]
                    
                    # 4. CSV 파일 데이터 행 (리스트)
                    data_row = [
                        platform, product_id, product_price, url, model_name, product_name, capacity,
                        sido, sigungu, dong, color, csv_date_str
                    ]
                    
                    # 5. 파일 열기 (추가 모드 'a', 한글 인코딩 'utf-8-sig', 줄바꿈 newline='')
                    with open(csv_filename, 'a', newline='', encoding='utf-8-sig') as f:
                        writer = csv.writer(f)
                        
                        # 6. 파일이 방금 새로 생성됐다면 헤더를 쓴다
                        if not file_exists:
                            writer.writerow(header_row)
                        
                        # 7. 데이터 행을 쓴다
                        writer.writerow(data_row)
                    
                    created_csv_files.add(csv_filename)

                except (IOError, PermissionError) as e:
                    print(f"  [!! CSV 파일 저장 오류 !!] {csv_filename}을(를) 쓰는 중 오류 발생: {e}")
                # ******** CSV 로직 끝 ********
                #

                # (콘솔 출력은 그대로 유지 - 진행 상황 확인용)
                print(f"  [!! '아이폰 16' (일반) / 서울특별시 매물 발견 !!]")
                print(f"  상품명: {product_name}")
                print(f"  상품ID: {product_id}")
                print(f"  위  치: {location_string}") 
                print(f"  작성일: {csv_date_str} (-> {csv_filename}에 저장)") # 저장 파일명 표시
                print("  " + "-" * 20)
            
            if page_seoul_products_count == 0:
                 print(f"--- {page_num+1}페이지에서 필터에 맞는 매물을 찾지 못했습니다. ---")

        else:
            print("상품 목록이 비어있습니다. 크롤링을 종료합니다.")
            break 

        print("--- 1초 대기 ---")
        time.sleep(1) 

    except requests.exceptions.RequestException as e:
        print(f"API 요청 중 오류 발생: {e}")
        break 
    except json.JSONDecodeError:
        print("데이터를 JSON으로 변환하는 데 실패했습니다. 응답 내용을 확인하세요.")
        print("응답 내용:", response.text)
        break 
        
print(f"\n======= 모든 페이지 크롤링 완료 (총 {total_seoul_products_found}개의 '아이폰 16 (일반) / 서울특별시' 매물 발견) =======\n")
if created_csv_files:
    print("생성/업데이트된 CSV 파일 목록:")
    for f in sorted(list(created_csv_files)):
        print(f"- {f}")
else:
    print("조건에 맞는 매물이 없어 CSV 파일이 생성되지 않았습니다.")
print("\n=================================================================================\n")
