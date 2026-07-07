# Databricks notebook source
df_gp = spark.table("FMCG.silver.gross_price")

display(df_gp)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS FMCG.gold.dim_gross_price;

# COMMAND ----------

df_gp.write \
.mode("overwrite") \
.format("delta") \
.saveAsTable("FMCG.gold.dim_gross_price")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM FMCG.gold.dim_gross_price LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN FMCG.gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM FMCG.gold.dim_date LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     MIN(date) AS start_date,
# MAGIC     MAX(date) AS end_date,
# MAGIC     COUNT(*) AS total_days
# MAGIC FROM FMCG.gold.dim_date;

# COMMAND ----------

df_gp = spark.table("FMCG.bronze.gross_price")

display(df_gp)

# COMMAND ----------

df_gp.printSchema()

# COMMAND ----------

from pyspark.sql.functions import *

df_gp = spark.table("FMCG.bronze.gross_price")

df_gp = df_gp.filter(
    (col("product_id") >= 25891101) &
    (col("product_id") <= 25891603)
)

print("After Product ID Filter:", df_gp.count())
display(df_gp)

# COMMAND ----------

df_gp = df_gp.filter(
    (~col("gross_price").isin("unknown", "not_available")) &
    (col("gross_price").rlike("^-?[0-9]+$"))
)

print("After Removing Invalid Prices:", df_gp.count())
display(df_gp)

# COMMAND ----------

from pyspark.sql.functions import *

df_gp = spark.table("FMCG.bronze.gross_price")

print(df_gp.count())
df_gp.printSchema()

# COMMAND ----------

from pyspark.sql.functions import expr, col

df_clean = spark.table("FMCG.bronze.gross_price")

df_clean = df_clean.filter(
    (col("product_id") >= 25891101) &
    (col("product_id") <= 25891603)
)

df_clean = df_clean.withColumn(
    "gross_price",
    expr("try_cast(gross_price AS INT)")
)

df_clean = df_clean.filter(
    col("gross_price").isNotNull() &
    (col("gross_price") > 0)
)

display(df_clean)
print(df_clean.count())

# COMMAND ----------

from pyspark.sql.functions import *

df_clean = df_clean.withColumn(
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

display(df_clean)

# COMMAND ----------

from pyspark.sql.functions import to_date

df_clean = df_clean.withColumn(
    "month",
    to_date(col("month"), "yyyy-MM-dd")
)

df_clean.printSchema()
display(df_clean)

# COMMAND ----------

from pyspark.sql.functions import to_date, col

df_clean = df_clean.withColumn(
    "month",
    to_date(col("month"), "yyyy-MM-dd")
)

df_clean.printSchema()

# COMMAND ----------

df_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("FMCG.silver.gross_price")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM FMCG.silver.gross_price LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN FMCG.silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM FMCG.silver.gross_price LIMIT 10;