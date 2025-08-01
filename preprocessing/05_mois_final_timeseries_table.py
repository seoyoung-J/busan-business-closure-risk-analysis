# Databricks notebook source 


# 1. 기준연월 목록 생성 
from pyspark.sql.functions import col, count, when

months = [f"{y}-{m:02d}" for y in range(2022, 2024) for m in range(1, 13) if f"{y}-{m:02d}" <= "2023-09"]
months_df = spark.createDataFrame([(m,) for m in months], ["기준연월"])


# 2. 원본 테이블 로드 및 crossJoin 
# 21*263274 = 5528754 행 
raw_df = spark.table("gold.busan.mois_busan_with_store_final_v2") # 가맹여부 컬럼 생성 및 업종 매핑 완료한 행안부 테이블 (263274행)
joined = months_df.crossJoin(raw_df)  


# 3. 생존 점포 필터링
alive = joined.filter(
    (col("인허가연월") <= col("기준연월")) & (col("폐업연월").isNull()|(col("폐업연월") > col("기준연월")))
)


# 4. 운영개월 계산 
# 2022.01 운영개월 계산해서 평균내고 누적 방식으로 해당 년월의 사업장의 평균 운영기간을 구하기 위함 

from pyspark.sql.functions import udf, col
from pyspark.sql.types import IntegerType

def calc_months(start, end):
    try:
        sy, sm = map(int, start.split("-"))
        ey, em = map(int, end.split("-"))
        return (ey - sy) * 12 + (em - sm)
    except:
        return None

calc_months_udf = udf(calc_months, IntegerType()) 

alive = alive.withColumn("운영개월", calc_months_udf(col("인허가연월"), col("기준연월"))) # 운영개월 컬럼 추가
 

# 5. 생존 점포 집계 
from pyspark.sql.functions import count, when, avg

agg_alive = alive.groupBy("기준연월", "시군구", "행정동", "대분류명").agg(
    count("*").alias("생존점포수"),
    count(when(col("가맹여부") == 1, True)).alias("지역화폐가맹점수"),
    count(when(col("역세권_접근성") == 1, True)).alias("역세권점포수"),
    avg("운영개월").alias("평균_운영개월") 
)


# 6. 폐업 점포 집계 
closed = raw_df.join(months_df, raw_df["폐업연월"] == months_df["기준연월"], "inner") \
    .groupBy("기준연월", "시군구", "행정동", "대분류명") \
    .agg(count("*").alias("폐업점포수"))


# 7. 병합 
# 전체 점포수 (생존점포수+폐업점포수) 계산
# 운영기간 계산 (평균 운영개월 / 12) 
final_df = agg_alive.join(
    closed,
    on=["기준연월", "시군구", "행정동", "대분류명"],
    how="outer"
).na.fill(0)

from pyspark.sql.functions import round as spark_round

final_df = final_df.withColumn(
    "전체점포수", col("생존점포수") + col("폐업점포수")
).withColumn(
    "평균_운영기간", spark_round(col("평균_운영개월") / 12.0, 2)
)

# 컬럼 순서 정리 
columns_order = [
    "기준연월", "시군구", "행정동", "대분류명",
    "생존점포수", "폐업점포수", "전체점포수",
    "지역화폐가맹점수", "역세권점포수",
    "평균_운영기간"
]
final_df = final_df.select(columns_order)

# 정렬
final_df = final_df.orderBy("기준연월", "시군구", "행정동") 


# 8. 테이블에 저장 
# 총 44393행 
final_df.write.mode("overwrite").saveAsTable("gold.busan.mois_busan_store_stats_v2_with_period")


# 9. 주요 파생지표 생성 
from pyspark.sql.functions import col, when, round

df_rates = final_df.withColumns({
    "생존률": round(when(col("전체점포수") > 0, col("생존점포수") / col("전체점포수")).otherwise(0), 2),
    "폐업률": round(when(col("전체점포수") > 0, col("폐업점포수") / col("전체점포수")).otherwise(0), 2),
    "역세권점포비율": round(when(col("전체점포수") > 0, col("역세권점포수") / col("전체점포수")).otherwise(0), 2),
    "지역화폐가맹률": round(when(col("전체점포수") > 0, col("지역화폐가맹점수") / col("전체점포수")).otherwise(0), 2)
})

# 필요한 컬럼만 필터링 
save_cols = [
    "기준연월", "시군구", "행정동", "대분류명",
    "전체점포수", "생존률", "폐업률", "역세권점포비율", "지역화폐가맹률", "평균_운영기간"
]
df_rates_selected = df_rates.select(save_cols)

# 테이블로 저장 
df_rates_selected.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold.busan.mois_busan_store_stats_v3_with_period") 

# 집계한 행안부 테이블 행정동명 정제 
# 정제 전 행 갯수 44393
# 정제 후 행 갯수 44173
from pyspark.sql.functions import regexp_replace, when, col

mois_df = spark.table("gold.busan.mois_busan_store_stats_v3_with_period")

# 대상 리스트
target_dongs = ["대저1동", "대저2동", "명지1동", "명지2동"]

mois_dong_cleaned = mois_df \
    .withColumn(
        "행정동",
        when(col("행정동").isin(target_dongs),
             regexp_replace(col("행정동"), r"([가-힣]+)(\d+)동$", r"$1제$2동")) # 대상리스트에 제 붙이기 
        .otherwise(col("행정동"))
    ) \
    .filter(col("행정동") != "신호동") # 신호동 행 제거 


# 행안부 데이터 행정동명 정제하고 동백전 거래 테이블이랑 행정동 일치하는지 확인 
dongbaek_dong_set = dongbaek_je_df.select("행정동").distinct()
mois_dong_set = mois_dong_cleaned.select("행정동").distinct() 

# mois에만 있는 행정동
mois_only = mois_dong_set.subtract(dongbaek_dong_set)
print("mois_dong_cleaned 에만 있는 행정동:")
mois_only.orderBy("행정동").show(100, truncate=False)

# dongbaek에만 있는 행정동
dongbaek_only = dongbaek_dong_set.subtract(mois_dong_set)
print("dongbaek_je_df 에만 있는 행정동:")
dongbaek_only.orderBy("행정동").show(100, truncate=False)

# 다시 테이블로 저장 
mois_dong_cleaned.write \
    .mode("overwrite") \
    .saveAsTable("gold.busan.mois_busan_store_stats_v3_with_period_re")

# MAGIC %md
# MAGIC ### 행정동명 정제 후 다시 조인 1 

# COMMAND ----------

# JOIN 1: 행안부 데이터를 기준으로 행정동 단위 업종별 지역화폐 거래 데이터 LEFT JOIN
from pyspark.sql.functions import col, expr, when

mois_df = spark.table("gold.busan.mois_busan_store_stats_v3_with_period_re")
dongbaek_df = spark.table("silver.busan.dongbaek_grouped_by_category") # 동백전 거래 데이터 전처리 및 업종 매핑 완료한 테이블 

# COMMAND ----------

# 동백전 지역화폐 거래 데이터 전처리 
# 기준연월 생성, 건당 평균 결제금액 파생변수 생성, 행정동명 정제 및 컬럼명을 통일(-제)
dongbaek_df = dongbaek_df.withColumn(
    "기준연월",
    expr("substring(year_month, 1, 4) || '-' || substring(year_month, 5, 2)")
)

from pyspark.sql.functions import round

dongbaek_df = dongbaek_df.withColumn(
    "건당_평균_결제금액",
    round(
        when(col("월별_총거래건수") > 0, col("월별_총거래금액") / col("월별_총거래건수"))
        .otherwise(0), 2
    )
)
from pyspark.sql.functions import regexp_replace, col

dongbaek_je_df = dongbaek_df.withColumn(
    "adn_dgnm",
    regexp_replace(
        col("adn_dgnm"),
        r"([가-힣]+)(\d+)동",
        r"$1제$2동"
    )
)
dongbaek_je_df = dongbaek_je_df.withColumnRenamed("adn_dgnm", "행정동")

# COMMAND ----------

# 데이터 조인 
dongbaek_selected = dongbaek_je_df.select(
    "기준연월", "행정동", "대분류명",
    "월별_총거래건수", "월별_총거래금액", "건당_평균_결제금액"
)

joined_df = mois_df.join(
    dongbaek_selected,
    on=["기준연월", "행정동", "대분류명"],
    how="left"
)

# COMMAND ----------

# 비가맹점이면서 지역화폐 거래 정보가 없는 경우: 정상 미사용 사례로0 처리 (14,095건)  
from pyspark.sql.functions import col, when, lit

joined_df = joined_df \
    .withColumn(
        "월별_총거래건수",
        when(col("지역화폐가맹률") == 0, lit(0))
        .otherwise(col("월별_총거래건수"))
    ).withColumn(
        "월별_총거래금액",
        when(col("지역화폐가맹률") == 0, lit(0))
        .otherwise(col("월별_총거래금액"))
    ).withColumn(
        "건당_평균_결제금액",
        when(col("지역화폐가맹률") == 0, lit(0))
        .otherwise(col("건당_평균_결제금액"))
    )

# COMMAND ----------

# 동백전 가맹점이지만 지역화폐 거래 정보가 누락인 경우: 일 기준연월·행정동·업종 평균값으로 보완 (3,453건) 
# 평균이 존재하는데 0이어서 0으로 채워진 행 841행 존재 
from pyspark.sql.functions import col, avg, when, lit
from pyspark.sql.window import Window

window_spec = Window.partitionBy("행정동", "대분류명")

df_avg = joined_df \
    .withColumn("거래건수_평균", avg("월별_총거래건수").over(window_spec)) \
    .withColumn("거래금액_평균", avg("월별_총거래금액").over(window_spec)) \
    .withColumn("건당금액_평균", avg("건당_평균_결제금액").over(window_spec))

df_filled = df_avg \
    .withColumn(
        "월별_총거래건수",
        when(col("월별_총거래건수").isNull() & (col("지역화폐가맹률") != 0), col("거래건수_평균"))
        .when(col("월별_총거래건수").isNull() & (col("지역화폐가맹률") == 0), lit(0))
        .otherwise(col("월별_총거래건수"))
    ).withColumn(
        "월별_총거래금액",
        when(col("월별_총거래금액").isNull() & (col("지역화폐가맹률") != 0), col("거래금액_평균"))
        .when(col("월별_총거래금액").isNull() & (col("지역화폐가맹률") == 0), lit(0))
        .otherwise(col("월별_총거래금액"))
    ).withColumn(
        "건당_평균_결제금액",
        when(col("건당_평균_결제금액").isNull() & (col("지역화폐가맹률") != 0), col("건당금액_평균"))
        .when(col("건당_평균_결제금액").isNull() & (col("지역화폐가맹률") == 0), lit(0))
        .otherwise(col("건당_평균_결제금액"))
    ).drop("거래건수_평균", "거래금액_평균", "건당금액_평균")


# COMMAND ----------

# 평균값이 부재인 경우: 0으로 채움(1926행) 
df_final = df_filled.fillna(0)

# COMMAND ----------

# 평균 0인 841행이랑 평균도 없어서 0으로 채운 1926행 모두 제외하고 저장 
from pyspark.sql.functions import col

# 평균이 0이었던 행 추출
zero_mean_filled = df_avg.filter(
    (col("월별_총거래건수").isNull()) &
    (col("지역화폐가맹률") != 0) &
    (col("거래건수_평균") == 0) &
    (col("거래건수_평균").isNotNull())
).select("기준연월", "행정동", "대분류명").distinct() 

# 위에서 뽑은 행을 제거한 데이터셋 
df_final_cleaned = df_final.join(
    zero_mean_filled,
    on=["기준연월", "행정동", "대분류명"],
    how="left_anti"
) 

# 조건에 맞는 1926개 찾기 (평균 자체가 없는 경우)
no_mean_rows = df_avg.filter(
    (col("월별_총거래건수").isNull()) &
    (col("지역화폐가맹률") != 0) &
    (col("거래건수_평균").isNull())
).select("기준연월", "행정동", "대분류명").distinct()

# 기존 cleaned 테이블에서 제거
df_final_cleaned_v2 = df_final_cleaned.join(
    no_mean_rows,
    on=["기준연월", "행정동", "대분류명"],
    how="left_anti"
)

# COMMAND ----------

# 컬럼 정렬 후 테이블에 저장 
ordered_cols = [
    "기준연월", "시군구", "행정동", "대분류명",
    "전체점포수","생존률", "폐업률", "지역화폐가맹률", "역세권점포비율", "평균_운영기간",
    "월별_총거래건수", "월별_총거래금액", "건당_평균_결제금액"
]
df_final_cleaned_v2 = df_final_cleaned_v2.select(*ordered_cols)

df_final_cleaned_v2.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver.busan.mois_with_dongbaek_joined_v2_with_period_cleaned_v2")  

# COMMAND ----------

# 인구 데이터 행정동명 정제 
# 인구 데이터 = 성별, 연령별, 시간대별 소비매출 데이터를 기준연월, 시군구, 행정동 기준으로 병합한 데이터
population_df = spark.table("gold.busan.age_gender_spending_final")

# 정제 대상
target_dongs = ["대저1동", "대저2동", "명지1동", "명지2동"] # 이거 앞에 제 붙임 

population_df_cleaned = population_df.withColumn(
    "행정동",
    when(col("행정동").isin(target_dongs),
         regexp_replace(col("행정동"), r"([가-힣]+)(\d+)동$", r"$1제$2동"))
    .otherwise(col("행정동"))
)
# 테이블로 다시 저장 
population_df_cleaned.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold.busan.age_gender_spending_final_v2") 

# COMMAND ----------

# 시군구+행정동 조합별 일치하는지 확인 
from pyspark.sql.functions import concat_ws

mois_dongbaek_df = spark.table("silver.busan.mois_with_dongbaek_joined_v2_with_period_cleaned_v2")  
mois_dongbaek_dong_set = mois_dongbaek_df.select(concat_ws("_", "시군구", "행정동").alias("sgg_dong")).distinct()
population_dong_set = population_df.select(concat_ws("_", "시군구", "행정동").alias("sgg_dong")).distinct()

# mois에만 있는 시군구+행정동 조합
mois_only = mois_dongbaek_dong_set.subtract(population_dong_set)
print("mois_dongbaek_df 에만 있는 시군구+행정동 조합:")
mois_only.orderBy("sgg_dong").show(100, truncate=False)

# population에만 있는 시군구+행정동 조합
population_only = population_dong_set.subtract(mois_dongbaek_dong_set)
print("population_df 에만 있는 시군구+행정동 조합:")
population_only.orderBy("sgg_dong").show(100, truncate=False)


# COMMAND ----------

# 인구 테이블 시군구 오류 수정 
# 서대신제1동, 서대신제3동, 서대신제4동: 금정구 -> 서구  
# 서제1동, 서제2동, 서제3동: 서구 -> 금정구  
from pyspark.sql.functions import when, col

population_df_fixed = population_df.withColumn(
    "시군구",
    when(col("행정동").isin(["서대신제1동", "서대신제3동", "서대신제4동"]), "서구")
    .when((col("행정동").isin("서제1동", "서제2동", "서제3동")) & (col("시군구") == "서구"), "금정구")
    .otherwise(col("시군구"))
)

population_df_fixed.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold.busan.age_gender_spending_final_v3")

# COMMAND ----------

# JOIN 1 결과 테이블 시군구 오류 수정 
# 안락제1동, 안락제2동: 연제구 -> 동래구 
mois_dongbaek_fixed = mois_dongbaek_df.withColumn(
    "시군구",
    when(col("행정동").isin(["안락제1동", "안락제2동"]), "동래구")
    .when((col("행정동") == "명장제2동") & (col("시군구") == "금정구"), "동래구")
    .otherwise(col("시군구"))
)

mois_dongbaek_fixed.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver.busan.mois_with_dongbaek_joined_v2_with_period_cleaned_v2")

# COMMAND ----------

# JOIN 2: 최종 JOIN 
mois_dongbaek_df = spark.table("silver.busan.mois_with_dongbaek_joined_v2_with_period_cleaned_v2")
population_df = spark.table("gold.busan.age_gender_spending_final_v3")

join_cols = ["기준연월", "시군구", "행정동"]

final_df_3 = mois_dongbaek_df.join(
    population_df,
    on=join_cols,
    how="left"
)

final_df_3.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold.busan.final_analysis_v3_cleaned_full")