# Databricks notebook source
import requests
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from concurrent.futures import ThreadPoolExecutor, as_completed 
from config import VWORLD_API_KEY

# API 요청 함수
def get_address_data(api_key, x, y):
    url = "https://api.vworld.kr/req/address"
    params = {
        "service": "address",
        "request": "getAddress",
        "crs": "epsg:4326",
        "point": f"{x},{y}",
        "format": "json",
        "type": "parcel", # 지번주소 
        "key": api_key
    }
    try:
        res = requests.get(url, params=params, timeout=3)
        if res.status_code != 200:
            return None
        return res.json().get("response", {}).get("result", [None])[0]
    except:
        return None

# 단일 좌표 처리 함수
def process_address_data(api_key, uid, x, y):
    result = get_address_data(api_key, x, y)

    if result is None:
        return Row(
            uid=uid, x=x, y=y,
            text=None,
            level0=None, level1=None, level2=None, level3=None,
            level4L=None, level4LC=None, level4A=None, level4AC=None,
            level5=None, detail=None
        )

    structure = result.get("structure", {})
    return Row(
        uid=uid,
        x=x,
        y=y,
        text=result.get("text"),
        level0=structure.get("level0"),
        level1=structure.get("level1"),
        level2=structure.get("level2"),
        level3=structure.get("level3"),
        level4L=structure.get("level4L"),
        level4LC=structure.get("level4LC"),
        level4A=structure.get("level4A"),
        level4AC=structure.get("level4AC"),
        level5=structure.get("level5"),
        detail=structure.get("detail")
    )

# 병렬 처리 및 저장 함수 
def process_in_chunk(coordinate_list, api_key, chunk_size, table_name):
    total = len(coordinate_list)

    schema = StructType([
        StructField("uid", LongType(), True),
        StructField("x", DoubleType(), True),
        StructField("y", DoubleType(), True),
        StructField("text", StringType(), True),
        StructField("level0", StringType(), True),
        StructField("level1", StringType(), True),
        StructField("level2", StringType(), True),
        StructField("level3", StringType(), True),
        StructField("level4L", StringType(), True),
        StructField("level4LC", StringType(), True),
        StructField("level4A", StringType(), True),
        StructField("level4AC", StringType(), True),
        StructField("level5", StringType(), True),
        StructField("detail", StringType(), True)
    ])

    for start in range(0, total, chunk_size):
        chunk = coordinate_list[start:start + chunk_size]

        results = []
        with ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(process_address_data, api_key, uid, x, y)
                for uid, x, y in chunk
            ]
            for future in as_completed(futures):
                results.append(future.result())

        df = spark.createDataFrame(results, schema=schema)
        df.write.format("delta") \
            .mode("append") \
            .saveAsTable(table_name)

        print(f"저장 완료: {start} ~ {start + len(chunk)}")


# COMMAND ----------

# 좌표 컬럼 null 아닌것만 추출 
coordinate_pdf = spark.sql("""
SELECT unique_id as uid, 
       `좌표정보x_epsg4326_경도` as x, 
       `좌표정보y_epsg4326_위도` as y
FROM bronze.file_busan.`mois_busan_위경도`
WHERE `좌표정보x_epsg4326_경도` IS NOT NULL 
  AND `좌표정보y_epsg4326_위도` IS NOT NULL
""").toPandas()

# 튜플 리스트로 변환 (uid, x, y 순서)
coordinate_list = list(coordinate_pdf.itertuples(index=False, name=None))

api_key = VWORLD_API_KEY 
table_name = "bronze.api_busan.mois_busan_coordinates_to_addr_vworld"

# 실행
process_in_chunk(coordinate_list=coordinate_list, api_key=api_key, chunk_size=300, table_name=table_name)