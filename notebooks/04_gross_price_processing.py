# Databricks notebook source
# MAGIC %run "../Setup/utilities"

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df_bronze = spark.table("FMCG.bronze.gross_price")

display(df_bronze)

# COMMAND ----------

df_bronze.printSchema()

# COMMAND ----------

print("Total Records :", df_bronze.count())

# COMMAND ----------

valid_products = spark.table("FMCG.silver.products") \
    .select("product_id")

df_silver = df_bronze.join(
    valid_products,
    on="product_id",
    how="inner"
)

display(df_silver)

# COMMAND ----------

print("Before :", df_bronze.count())
print("After  :", df_silver.count())

# COMMAND ----------

from pyspark.sql.functions import col

df_silver = df_silver.filter(
    (~col("gross_price").isin("unknown", "not_available"))
)

# COMMAND ----------

df_silver = df_silver.withColumn(
    "gross_price",
    col("gross_price").cast("int")
)

# COMMAND ----------

df_silver = df_silver.filter(
    col("gross_price") > 0
)

# COMMAND ----------

df_bronze = spark.table("FMCG.bronze.gross_price")

# COMMAND ----------

valid_products = spark.table("FMCG.silver.products").select("product_id")

df_silver = df_bronze.join(
    valid_products,
    on="product_id",
    how="inner"
)

# COMMAND ----------

from pyspark.sql.functions import col

df_silver = df_silver.filter(
    col("gross_price").rlike(r"^-?\d+$")
)

# COMMAND ----------

df_silver = df_silver.withColumn(
    "gross_price",
    col("gross_price").cast("int")
)

# COMMAND ----------

df_silver = df_silver.filter(col("gross_price") > 0)

# COMMAND ----------

df_silver.printSchema()

# COMMAND ----------

from pyspark.sql.functions import *

valid_products = spark.table("FMCG.silver.products").select("product_id")

df_silver = (
    spark.table("FMCG.bronze.gross_price")
    .join(valid_products, on="product_id", how="inner")
)

# COMMAND ----------

df_silver = df_silver.withColumn(
    "gross_price",
    when(col("gross_price").rlike("^-?[0-9]+$"), col("gross_price"))
    .otherwise(None)
)

# COMMAND ----------

df_silver = df_silver.withColumn(
    "gross_price",
    col("gross_price").cast("int")
)

# COMMAND ----------

df_silver = df_silver.filter(
    col("gross_price").isNotNull()
)

# COMMAND ----------

df_silver = df_silver.filter(
    col("gross_price") > 0
)

# COMMAND ----------

display(df_silver)

# COMMAND ----------

from pyspark.sql.functions import col, coalesce, try_to_timestamp

df_silver = df_silver.withColumn(
    "month",
    coalesce(
        try_to_timestamp(col("month"), "yyyy-MM-dd"),
        try_to_timestamp(col("month"), "yyyy/MM/dd"),
        try_to_timestamp(col("month"), "dd/MM/yyyy"),
        try_to_timestamp(col("month"), "dd-MM-yyyy")
    ).cast("date")
)

# COMMAND ----------

spark.version

# COMMAND ----------

from pyspark.sql.functions import regexp_replace, col

df_silver = df_silver.withColumn(
    "month",
    regexp_replace(col("month"), "/", "-")
)

# COMMAND ----------

from pyspark.sql.functions import to_date

df1 = (
    df_silver
    .filter(col("month").rlike("^\\d{4}-"))
    .withColumn("month", to_date(col("month"), "yyyy-MM-dd"))
)

df2 = (
    df_silver
    .filter(col("month").rlike("^\\d{2}-"))
    .withColumn("month", to_date(col("month"), "dd-MM-yyyy"))
)

df_silver = df1.unionByName(df2)

# COMMAND ----------

from pyspark.sql.functions import *

valid_products = spark.table("FMCG.silver.products").select("product_id")

df_silver = (
    spark.table("FMCG.bronze.gross_price")
    .join(valid_products, on="product_id", how="inner")
)

# COMMAND ----------

df_silver = df_silver.withColumn(
    "gross_price",
    when(col("gross_price").rlike("^-?\\d+$"), col("gross_price"))
)

# COMMAND ----------

df_silver = df_silver.filter(col("gross_price").isNotNull())

# COMMAND ----------

df_silver = df_silver.withColumn(
    "gross_price",
    col("gross_price").cast("int")
)

# COMMAND ----------

df_silver = df_silver.filter(col("gross_price") > 0)

# COMMAND ----------

display(df_silver)

# COMMAND ----------

df_silver.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("FMCG.silver.gross_price")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN FMCG.silver;