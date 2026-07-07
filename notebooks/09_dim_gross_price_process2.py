# Databricks notebook source
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS FMCG.gold.dim_gross_price;

# COMMAND ----------

df_gp_gold = spark.table("FMCG.silver.gross_price")

df_gp_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("FMCG.gold.dim_gross_price")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM FMCG.gold.dim_gross_price LIMIT 10;