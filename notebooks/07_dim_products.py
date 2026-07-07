# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

df_products = spark.table("FMCG.silver.products")

display(df_products)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS FMCG.gold.dim_products;

# COMMAND ----------

df_products.write \
.mode("overwrite") \
.format("delta") \
.saveAsTable("FMCG.gold.dim_products")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM FMCG.gold.dim_products;