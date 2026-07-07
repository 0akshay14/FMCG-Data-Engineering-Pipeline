# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

bronze_orders = spark.table("FMCG.bronze.orders")

display(bronze_orders)

# COMMAND ----------

orders = spark.table("FMCG.silver.orders")
customers = spark.table("FMCG.gold.dim_customers")
products = spark.table("FMCG.gold.dim_products")
gross_price = spark.table("FMCG.gold.dim_gross_price")
dates = spark.table("FMCG.gold.dim_date")

# COMMAND ----------

print("Orders:", orders.count())
print("Customers:", customers.count())
print("Products:", products.count())
print("Gross Price:", gross_price.count())
print("Date:", dates.count())

# COMMAND ----------

from pyspark.sql.functions import *

gross_price = gross_price.withColumn(
    "month",
    coalesce(
        to_date(col("month"), "yyyy-MM-dd"),
        to_date(col("month"), "yyyy/MM/dd"),
        to_date(col("month"), "dd/MM/yyyy"),
        to_date(col("month"), "dd-MM-yyyy")
    )
)

# COMMAND ----------

fact_orders = (
    orders
    .join(customers, orders.customer_id == customers.customer_id, "left")
    .join(products, orders.product_id == products.product_id, "left")
)

display(fact_orders)

# COMMAND ----------

print(fact_orders.count())

# COMMAND ----------

from pyspark.sql.functions import trunc

fact_orders = fact_orders.withColumn(
    "month",
    trunc(col("order_placement_date"), "month")
)

display(fact_orders)

# COMMAND ----------

gross_price.printSchema()

# COMMAND ----------

gross_price = spark.table("FMCG.gold.dim_gross_price")

gross_price.printSchema()

display(gross_price.limit(10))

# COMMAND ----------

fact_orders = fact_orders.join(
    spark.table("FMCG.gold.dim_gross_price")
         .select("product_id", "month", "gross_price"),
    on=["product_id", "month"],
    how="left"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS FMCG.gold.dim_gross_price;

# COMMAND ----------

df_gp = spark.table("FMCG.silver.gross_price")

display(df_gp)

# COMMAND ----------

display(df_gp.limit(10))

# COMMAND ----------

df_gp.printSchema()

# COMMAND ----------

from pyspark.sql.functions import *

df_gp = spark.table("FMCG.silver.gross_price")

df_gp = df_gp.withColumn(
    "month",
    when(col("month").rlike("^\\d{4}/\\d{2}/\\d{2}$"),
         regexp_replace(col("month"), "/", "-"))
    .when(col("month").rlike("^\\d{2}/\\d{2}/\\d{4}$"),
         concat_ws("-",
                   substring(col("month"), 7, 4),
                   substring(col("month"), 4, 2),
                   substring(col("month"), 1, 2)))
    .when(col("month").rlike("^\\d{2}-\\d{2}-\\d{4}$"),
         concat_ws("-",
                   substring(col("month"), 7, 4),
                   substring(col("month"), 4, 2),
                   substring(col("month"), 1, 2)))
    .otherwise(col("month"))
)

display(df_gp)

# COMMAND ----------

df_gp.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("FMCG.silver.gross_price")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS FMCG.gold.dim_gross_price;

# COMMAND ----------

df_gp = spark.table("FMCG.silver.gross_price")

df_gp = df_gp.withColumn("month", to_date(col("month"), "yyyy-MM-dd"))

df_gp.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("FMCG.gold.dim_gross_price")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS FMCG.silver.gross_price;

# COMMAND ----------

df_gp.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN FMCG.silver;

# COMMAND ----------

df_gp.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("FMCG.silver.gross_price")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN FMCG.bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN FMCG.silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN FMCG.gold;