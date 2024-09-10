import pymysql
import time
import traceback
import json
import pandas as pd
from pandas import Timestamp
import cal_player_anal as anal
from datetime import datetime

# 타임스탬프 포함 메시지 출력 함수
def print_with_timestamp(message):
    """타임스탬프를 포함하여 메시지 출력"""
    current_time = datetime.now()
    timestamp = current_time.strftime('%Y-%m-%d %H:%M:%S.') + f'{current_time.microsecond:06d}'[:8]
    print(f"[{timestamp}] {message}")

# RDS 연결 정보
RDS_HOST = 'agroundrds.c4mjyhzyjllp.ap-northeast-2.rds.amazonaws.com'
USERNAME = 'ground'
PASSWORD = 'assist0907'
DB_NAME = 'agroundsDB'

# 분석할 anal_match_code 값
anal_match_code_num = 99

# ===============================================
# RDS에서 분석 정보 가져오기
# ===============================================
def fetch_match_info(anal_match_code_num, ground_name, start_time):
    # MySQL 연결
    connection = pymysql.connect(
        host=RDS_HOST,
        user=USERNAME,
        password=PASSWORD,
        database=DB_NAME,
        cursorclass=pymysql.cursors.DictCursor  # 결과를 딕셔너리 형태로 가져옴
    )

    try:
        with connection.cursor() as cursor:
            # 필요한 필드 선택
            sql = """
                SELECT ground_name, start, end, name, position, home, TXT
                FROM V2_anal_match 
                WHERE anal_match_code = %s 
                AND ground_name = %s 
                AND start = %s
            """
            cursor.execute(sql, (anal_match_code_num, ground_name, start_time))
            results = cursor.fetchall()
            return results  # 결과 반환
    finally:
        connection.close()

# ===============================================
# info 구조 생성 및 TXT 파일 생성
# ===============================================
def build_info_and_save_txt(results):
    if not results:
        return None  # 데이터가 없을 경우

    # 첫 번째 결과를 기반으로 info 구성
    first_result = results[0]
    
    info = {
        'ground_name': first_result['ground_name'],
        'quarter_info': []
    }

    # TXT 필드로부터 파일 작성
    txt_content = first_result.get('TXT')
    if txt_content:
        with open('edit.TXT', 'w') as txt_file:
            txt_file.write(txt_content)
        print("edit.TXT 파일로 저장되었습니다.")

    for result in results:
        quarter_info = {
            'name': result['name'],
            'start': Timestamp(result['start']),
            'end': Timestamp(result['end']),
            'position': result['position'],
            'home': result['home']
        }
        info['quarter_info'].append(quarter_info)

    return info

# ===============================================
# 선수 분석
# ===============================================   
def anal_player(file, info):
    start_time = time.time()
    results = None
    check = None
    error = None
    try:
        results, check = anal.player_anal(file, info)
    except Exception as e:
        error = traceback.format_exc()
        check = 'error'
    end = time.time()
    diff_time = end - start_time
    print("경기 분석시간 : ", diff_time)
    
    return results, check, error

# ===============================================
# 변환된 딕셔너리를 JSON 파일로 저장
# ===============================================
def convert_timestamps_to_str(obj):
    if isinstance(obj, dict):
        return {k: convert_timestamps_to_str(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_timestamps_to_str(i) for i in obj]
    elif isinstance(obj, pd.Timestamp):
        return obj.isoformat()  # Timestamp를 ISO 형식의 문자열로 변환
    else:
        return obj
# ===============================================
# 메인 실행 로직
# ===============================================

print_with_timestamp("Script started.")  # 스크립트 시작 알림

# 특정 ground_name과 start_time 설정
ground_name = "을미기 체육공원 축구장"
start_time = '2024-08-18 18:14:00.100000'

# RDS에서 정보 가져오기
db_results = fetch_match_info(anal_match_code_num, ground_name, start_time)

# 가져온 정보를 기반으로 info 구성 및 edit.TXT 파일 생성
info = build_info_and_save_txt(db_results)

# 정보가 있을 때만 분석 수행
if info:
    upload_file = "edit.TXT"  # edit.TXT 파일을 업로드 파일로 설정
    results, check, error = anal_player(upload_file, info)
    print(check, error)

    # JSON 파일로 저장
    converted_results = convert_timestamps_to_str(results)
    with open('data.json', 'w', encoding='utf-8') as json_file:
        json.dump(converted_results, json_file, ensure_ascii=False, indent=4)
    print("JSON 파일로 저장되었습니다.")
else:
    print("데이터를 찾을 수 없습니다.")

print_with_timestamp("Script finished.")  # 스크립트 종료 알림
