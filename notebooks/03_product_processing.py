# Databricks notebook source
# MAGIC %run "../Setup/utilities"

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df_bronze = spark.table("FMCG.bronze.products")

display(df_bronze)

# COMMAND ----------

df_bronze.printSchema()

# COMMAND ----------

print("Total Records :", df_bronze.count())

# COMMAND ----------

duplicates = (
    df_bronze
    .groupBy("product_id")
    .count()
    .filter("count > 1")
)

display(duplicates)

# COMMAND ----------

df_silver = df_bronze.dropDuplicates(["product_id"])

# COMMAND ----------

print("Before :", df_bronze.count())
print("After  :", df_silver.count())

# COMMAND ----------

df_silver = df_silver.withColumn(
    "category",
    when(col("category") == "protien bars", "protein bars")
    .otherwise(col("category"))
)

# COMMAND ----------

df_silver = df_silver.filter(col("product_id").rlike("^[0-9]+$"))

# COMMAND ----------

display(df_silver)

# COMMAND ----------

df_silver.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("FMCG.silver.products")

# COMMAND ----------

df_silver = spark.table("FMCG.silver.products")

display(df_silver)

# COMMAND ----------

from pyspark.sql.functions import col

df_gold_product = (
    df_silver
    .withColumnRenamed("product_id", "product_code")
    .select(
        "product_code",
        "product_name",
        "category"
    )
)

display(df_gold_product)

# COMMAND ----------

df_gold_product.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("FMCG.gold.sb_dim_products")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN FMCG.gold;