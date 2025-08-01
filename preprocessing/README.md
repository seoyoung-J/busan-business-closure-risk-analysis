## Preprocessing Scripts

이 디렉토리는 부산시 행정동 단위 데이터 분석을 위한 전처리 스크립트를 포함합니다.  

| 파일명 | 설명 |
|--------|------|
| 01_filter_mois_data.py | 인허가 데이터를 필터링하고, 개업·폐업일 기준으로 분석 기간 내 생존 점포/폐업 점포를 판별 |
| 02_clean_dongbaek_store_name.py | 동백전 가맹점 데이터에서 가맹점명을 정제 |
| 03_mois_dongbaek_affiliation_mapping.py | 행안부 인허가 데이터와 동백전 가맹 데이터를 unique_id와 가맹점명 기준으로 매핑하여 가맹 여부를 판단 |
| 04_mois_final_timeseries_table.py | 기준연월별로 사업장 정보를 집계하고, 평균 운영기간을 계산하여 최종 시계열 테이블을 생성 |
| API_mois_busan_coordinates_to_addr_vworld.py | 좌표(x, y)를 기반으로 VWorld API를 활용해 시군구·행정동 정보를 보완 | 

---