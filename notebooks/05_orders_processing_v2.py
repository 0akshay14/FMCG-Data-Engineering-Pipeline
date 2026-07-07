# Databricks notebook source
# MAGIC %run "../Setup/utilities"

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# COMMAND ----------

df_orders = spark.table("FMCG.bronze.orders")

display(df_orders)

# COMMAND ----------

df_orders = df_orders.filter(
    col("customer_id").rlike("^[0-9]+$")
)

# COMMAND ----------

valid_products = spark.table("FMCG.silver.products").select("product_id")

df_orders = df_orders.join(
    valid_products,
    on="product_id",
    how="inner"
)

# COMMAND ----------

df_orders = df_orders.dropDuplicates()

# COMMAND ----------

df_orders = df_orders.filter(
    col("order_qty").isNotNull()
)

# COMMAND ----------

from pyspark.sql.functions import expr, coalesce

df_orders = df_orders.withColumn(
    "order_placement_date",
    coalesce(
        expr("try_to_timestamp(order_placement_date,'yyyy/MM/dd')"),
        expr("try_to_timestamp(order_placement_date,'dd-MM-yyyy')"),
        expr("try_to_timestamp(order_placement_date,'dd/MM/yyyy')"),
        expr("try_to_timestamp(order_placement_date,'EEEE, MMMM dd, yyyy')")
    ).cast("date")
)

# COMMAND ----------

df_orders = df_orders.filter(
    col("order_placement_date").isNotNull()
)

# COMMAND ----------

display(df_orders)

# COMMAND ----------

df_orders.write \
.mode("overwrite") \
.format("delta") \
.saveAsTable("FMCG.silver.orders")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN FMCG.silver;