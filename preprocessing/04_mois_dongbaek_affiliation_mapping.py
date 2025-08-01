# Databricks notebook source
# MAGIC %md
# MAGIC 문자열 유사도와 위경도 거리(50 ~ 100m 범위)를 함께 활용해 가맹여부를 판별
# MAGIC 유사도 0.7이상 기준으로 0.9 ~ 1이면 거리 100m, 0.7 ~ 0.9는 거리 50m


df_mois = spark.table("silver.busan.preprocessed_mois_busan_202201_202309_v2") # 행안부 데이터에서 분석 구간 필터링한 테이블 
df_dbj= spark.table("gold.busan.mois_dongbaek_clean_addr_v3") # 동백전 가맹점명 정제한 테이블에 위도 경도 정보 추가한 테이블 


from pyspark.sql.functions import col
df_mois_selected = df_mois.select(
    col("unique_id"),  
    col("정제된사업장명").alias("mois_store_name"),
    col("y_위도").alias("mois_latitude"),
    col("x_경도").alias("mois_longitude")
)

df_dbj_selected = df_dbj.select(
    col("정제된가맹점명").alias("dbj_store_name"),
    col("위도").alias("dbj_latitude"),
    col("경도").alias("dbj_longitude")
)


from pyspark.sql.functions import radians, sin, cos, atan2, sqrt, col, lit, expr
df_cross = df_mois_selected.crossJoin(df_dbj_selected)


from pyspark.sql.functions import expr, col, levenshtein, length, greatest

# 거리 계산 
df_with_distance = df_cross.withColumn(
    "distance_m",
    expr("""
        6371000 * 2 * ASIN(SQRT(
            POWER(SIN(RADIANS(dbj_latitude - mois_latitude) / 2), 2) +
            COS(RADIANS(mois_latitude)) * COS(RADIANS(dbj_latitude)) *
            POWER(SIN(RADIANS(dbj_longitude - mois_longitude) / 2), 2)
        ))
    """)
)

# 문자열 유사도 점수 계산 
df_with_similarity = df_with_distance.withColumn(
    "similarity_score",
    1 - (levenshtein(col("mois_store_name"), col("dbj_store_name")) /
         greatest(length(col("mois_store_name")), length(col("dbj_store_name"))))
)

df_matched = df_with_similarity.filter(
    ((col("similarity_score") >= 0.9) & (col("distance_m") <= 100)) |
    ((col("similarity_score") >= 0.7) & (col("similarity_score") < 0.9) & (col("distance_m") <= 50))
)


from pyspark.sql.functions import lit, row_number, col
from pyspark.sql.window import Window

df_matched_with_uid = (
    df_matched.drop("unique_id")  
    .join(
        df_mois_selected.select("unique_id", "mois_store_name", "mois_latitude", "mois_longitude"),
        on=["mois_store_name", "mois_latitude", "mois_longitude"],
        how="left"
    )
)

window_spec = Window.partitionBy("unique_id").orderBy(
    col("similarity_score").desc(), col("distance_m")
)
df_matched_dedup = df_matched_with_uid.withColumn("rn", row_number().over(window_spec)) \
    .filter("rn = 1") \
    .drop("rn") \
    .withColumn("가맹여부", lit(1))

df_mois_with_flag = df_mois_selected.join(
    df_matched_dedup.drop("mois_store_name", "mois_latitude", "mois_longitude"),
    on="unique_id",
    how="left"
).fillna({"가맹여부": 0})


df_mois_with_flag.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver.busan.mois_dongbaek_matched_temp_v5")


# 필요한 컬럼만 추출해서 저장 
df = spark.table("silver.busan.mois_dongbaek_matched_temp_v5") 
df_renamed = df.select(
    col("unique_id"),
    col("mois_store_name").alias("정제된사업장명"),
    col("가맹여부")
)

df_renamed.write.format("delta").mode("overwrite").saveAsTable("gold.busan.mois_dongbaek_store_final_v2") 
