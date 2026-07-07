# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

df_customers = spark.table("FMCG.silver.customers")

display(df_customers)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE FMCG.gold.dim_customers;

# COMMAND ----------

df_customers.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS FMCG.gold.dim_customers;

# COMMAND ----------

df_customers.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("FMCG.gold.dim_customers")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM FMCG.gold.dim_customers;