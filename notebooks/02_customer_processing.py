# Databricks notebook source
# MAGIC %run "../Setup/utilities"

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df_bronze = spark.table("FMCG.bronze.customers")

display(df_bronze)

# COMMAND ----------

df_bronze.printSchema()

# COMMAND ----------

print("Total Records :", df_bronze.count())

# COMMAND ----------

duplicates = (
    df_bronze
    .groupBy("customer_id")
    .count()
    .filter("count > 1")
)

display(duplicates)

# COMMAND ----------

df_silver = df_bronze.dropDuplicates(["customer_id"])

# COMMAND ----------

print("Before :", df_bronze.count())
print("After  :", df_silver.count())

# COMMAND ----------

display(df_silver)

# COMMAND ----------

from pyspark.sql.functions import trim, col

df_silver = df_silver.withColumn(
    "customer_name",
    trim(col("customer_name"))
)

# COMMAND ----------

display(df_silver)

# COMMAND ----------

df_silver.select("city").distinct().show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import when, col

df_silver = df_silver.withColumn(
    "city",
    when(col("city") == "Bengalore", "Bengaluru")
    .when(col("city") == "Bengaluruu", "Bengaluru")
    .when(col("city") == "Hyderbad", "Hyderabad")
    .when(col("city") == "Hyderabadd", "Hyderabad")
    .when(col("city") == "NewDelhi", "New Delhi")
    .when(col("city") == "NewDelhee", "New Delhi")
    .when(col("city") == "NewDheli", "New Delhi")
    .otherwise(col("city"))
)

# COMMAND ----------

df_silver.select("city").distinct().show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import initcap, col

df_silver = df_silver.withColumn(
    "customer_name",
    initcap(col("customer_name"))
)

# COMMAND ----------

df_silver.select("customer_name").distinct().show(truncate=False)

# COMMAND ----------

df_silver.filter(col("city").isNull()).show(truncate=False)

# COMMAND ----------

customers = [
    "Sprintx Nutrition",
    "Zenathlete Foods",
    "Primefuel Nutrition",
    "Recovery Lane"
]

df_silver.filter(col("customer_name").isin(customers)).show(truncate=False)

# COMMAND ----------

fix_data = [
    (789403, "New Delhi"),
    (789420, "Bengaluru"),
    (789521, "Hyderabad"),
    (789603, "New Delhi")
]

fix_df = spark.createDataFrame(
    fix_data,
    ["customer_id", "fixed_city"]
)

display(fix_df)

# COMMAND ----------

from pyspark.sql.functions import coalesce

df_silver = (
    df_silver
    .join(fix_df, on="customer_id", how="left")
    .withColumn("city", coalesce(col("city"), col("fixed_city")))
    .drop("fixed_city")
)

# COMMAND ----------

df_silver.filter(col("city").isNull()).show()

# COMMAND ----------

from pyspark.sql.functions import col

df_silver = df_silver.withColumn(
    "customer_id",
    col("customer_id").cast("string")
)

# COMMAND ----------

df_silver.printSchema()

# COMMAND ----------

df_bronze = spark.table("FMCG.bronze.customers")

display(df_bronze)


# COMMAND ----------

from pyspark.sql.functions import *

df_silver = df_bronze.dropDuplicates(["customer_id"])

# COMMAND ----------

df_silver = df_silver.withColumn(
    "customer_name",
    trim(col("customer_name"))
)

# COMMAND ----------

df_silver = df_silver.withColumn(
    "customer_name",
    initcap(col("customer_name"))
)

# COMMAND ----------

df_silver = df_silver.withColumn(
    "city",
    when(col("city") == "Bengalore", "Bengaluru")
    .when(col("city") == "Bengaluruu", "Bengaluru")
    .when(col("city") == "Hyderbad", "Hyderabad")
    .when(col("city") == "Hyderabadd", "Hyderabad")
    .when(col("city") == "NewDelhi", "New Delhi")
    .when(col("city") == "NewDelhee", "New Delhi")
    .when(col("city") == "NewDheli", "New Delhi")
    .otherwise(col("city"))
)

# COMMAND ----------

fix_data = [
    (789403, "New Delhi"),
    (789420, "Bengaluru"),
    (789521, "Hyderabad"),
    (789603, "New Delhi")
]

fix_df = spark.createDataFrame(
    fix_data,
    ["customer_id", "fixed_city"]
)

df_silver = (
    df_silver
    .join(fix_df, on="customer_id", how="left")
    .withColumn("city", coalesce(col("city"), col("fixed_city")))
    .drop("fixed_city")
)

# COMMAND ----------

display(df_silver)

# COMMAND ----------

df_silver.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("FMCG.silver.customers")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN FMCG.silver;

# COMMAND ----------

display(spark.table("FMCG.silver.customers"))

# COMMAND ----------

df_silver = spark.table("FMCG.silver.customers")

display(df_silver)

# COMMAND ----------

df_silver.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("FMCG.silver.customers")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN FMCG.silver;

# COMMAND ----------

from pyspark.sql.functions import col, lit, concat_ws

df_gold_customer = (
    df_silver
    .withColumnRenamed("customer_id", "customer_code")
    .withColumn(
        "customer",
        concat_ws(" - ", col("customer_name"), col("city"))
    )
    .withColumn("market", lit("India"))
    .withColumn("platform", lit("Sports Bar"))
    .withColumn("channel", lit("Offline"))
    .select(
        "customer_code",
        "customer",
        "market",
        "platform",
        "channel"
    )
)

display(df_gold_customer)

# COMMAND ----------

df_gold_customer.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("FMCG.gold.sb_dim_customers")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN FMCG.gold;

# COMMAND ----------

display(spark.table("FMCG.gold.dim_customers"))

# COMMAND ----------

display(spark.table("FMCG.gold.sb_dim_customers"))

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE FMCG.gold.dim_customers_final AS
# MAGIC
# MAGIC SELECT * FROM FMCG.gold.dim_customers
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT * FROM FMCG.gold.sb_dim_customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM FMCG.gold.dim_customers_final;

# COMMAND ----------

display(spark.table("FMCG.gold.dim_customers_final"))