# Databricks notebook source
# MAGIC %pip install geopandas shapely pyproj fiona 

import pandas as pd
import geopandas as gpd
import numpy as np
from shapely.geometry import Point

# 1. 데이터 로드
df_mois = spark.table("bronze.file_busan.`mois_busan_행정동매핑전체_20250614`").toPandas()


# 2. 분석 범위 필터링
df_mois["인허가일자"] = pd.to_datetime(df_mois["인허가일자"], errors="coerce") 
df_mois["폐업일자"] = pd.to_datetime(df_mois["폐업일자"], errors="coerce")

cutoff_start = pd.to_datetime("2022-01-01")
cutoff_end = pd.to_datetime("2023-09-30")

df_filtered = df_mois[
    (df_mois["인허가일자"] <= cutoff_end) & 
    (df_mois["폐업일자"].isnull() |      
        ((df_mois["폐업일자"] >= cutoff_start) & (df_mois["폐업일자"] <= cutoff_end)))
].copy()

df_filtered["폐업유무"] = df_filtered["폐업일자"].notnull().astype(int)


# 3. 좌표 기반 행정동/시군구 매핑이 안된 데이터 제거 
cols_to_check = [
    "좌표정보x_epsg5174_", "좌표정보y_epsg5174_",
    "x_경도", "y_위도", "전체주소",
    "시도", "시군구", "행정동", "행정동코드",
    "법정동", "법정동코드"
]
df_filtered = df_filtered.dropna(subset=cols_to_check).copy()


# 4. 행정동 매핑 (최빈값 기반)
df_filtered["행정동"] = df_filtered["행정동"].replace("", np.nan) 
df_filtered["행정동코드"] = df_filtered["행정동코드"].replace("", np.nan) 

# 유효한 행정동 정보가 있는 행만 사용해서 매핑 테이블 생성 
df_valid = df_filtered[df_filtered["행정동"].notna() & df_filtered["행정동코드"].notna()].copy()
df_valid["법정동코드"] = df_valid["법정동코드"].astype(str)

mode_map_dong = (
    df_valid.groupby("법정동코드")["행정동"]
    .agg(lambda x: x.mode().iloc[0])
    .reset_index()
)
mode_map_code = (
    df_valid.groupby("법정동코드")["행정동코드"]
    .agg(lambda x: x.mode().iloc[0])
    .reset_index()
)
mode_map = mode_map_dong.merge(mode_map_code, on="법정동코드")

# 매핑
df_filtered["법정동코드"] = df_filtered["법정동코드"].astype(str)
df_filtered = df_filtered.merge(mode_map, on="법정동코드", how="left", suffixes=("", "_mode"))
df_filtered["행정동"] = df_filtered["행정동"].fillna(df_filtered["행정동_mode"])
df_filtered["행정동코드"] = df_filtered["행정동코드"].fillna(df_filtered["행정동코드_mode"])
df_filtered.drop(columns=["행정동_mode", "행정동코드_mode"], inplace=True)


# 5. 역세권 파생변수 생성 
station_df = spark.table("bronze.file_busan.`역사정보_20210226_v2`").toPandas().copy()

station_gdf = gpd.GeoDataFrame(
    station_df,
    geometry=gpd.points_from_xy(station_df["역경도"], station_df["역위도"]),
    crs="EPSG:4326"
).to_crs("EPSG:5174")

mois_gdf = gpd.GeoDataFrame(
    df_filtered.copy(),
    geometry=gpd.points_from_xy(
        df_filtered["좌표정보x_epsg5174_"], df_filtered["좌표정보y_epsg5174_"]
    ),
    crs="EPSG:5174"
)

station_buffers = [geom.buffer(500) for geom in station_gdf.geometry]

mois_gdf["역세권_접근성"] = mois_gdf.geometry.apply(
    lambda pt: int(any(pt.within(buffer) for buffer in station_buffers))
)

final_df = mois_gdf.drop(columns="geometry")


# 6. 김해시 제외 및 컬럼 정리
final_df = final_df[final_df["시군구"] != "김해시"]
final_df["행정동코드"] = final_df["행정동코드"].fillna("").astype(str)

cols = [
    "unique_id", "시도", "시군구", "행정동", "행정동코드", "법정동", "법정동코드",
    "정제된사업장명", "개방서비스명", "전체주소", "x_경도", "y_위도",
    "좌표정보x_epsg5174_", "좌표정보y_epsg5174_", "역세권_접근성",
    "운영기간_범주", "운영기간", "폐업유무",
    "폐업일자", "인허가일자", "최종수정시점", "데이터갱신일자", "데이터갱신구분",
    "업태구분명", "위생업태명", "영업상태명", "영업상태구분코드", "상세영업상태명", "상세영업상태코드", "소재지면적"
]
final_df = final_df[cols].copy()

# Spark 저장
spark_df = spark.createDataFrame(final_df)
spark_df.write.format("delta").mode("overwrite").saveAsTable("silver.busan.preprocessed_mois_busan_202201_202309_v2")  


