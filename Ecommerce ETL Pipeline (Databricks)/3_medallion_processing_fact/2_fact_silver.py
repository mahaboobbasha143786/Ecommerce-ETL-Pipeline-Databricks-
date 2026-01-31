# Databricks notebook source
catalog_name = "ecommerce"

df = spark.read.table(f"{catalog_name}.bronze.brz_order_items")
display(df)

# COMMAND ----------

import pyspark.sql.functions as F
df = df.dropDuplicates(["order_id", "item_seq"])

df = df.withColumn(
    "quantity",
    F.when(F.col("quantity") == "Two", 2).otherwise(F.col("quantity")).cast("int")

)

df = df.withColumn(
    "unit_price",
    F.regexp_replace(F.col("unit_price"), "[$]", "").cast("double")
)

df = df.withColumn(
    "discount_pct",
    F.regexp_replace(F.col("discount_pct"), "[%]", "").cast("double")
)

df = df.withColumn(
    "coupon_code", F.lower(F.trim(F.col("coupon_code")))
    
)

df = df.withColumn(
    "channel",
    F.when(F.col("channel") == "web", "Website")
    .when(F.col("channel") == "app", "Mobile")
    .otherwise(F.col("channel"))
    
)



# COMMAND ----------

display(df.limit(5))

# COMMAND ----------

# DBTITLE 1,Cell 4
df = df.withColumn(
    "dt",
    F.to_date("dt", "yyyy-MM-dd")
)

df = df.withColumn(
    "order_ts",
    F.coalesce(
        F.to_timestamp("order_ts", "yyyy-MM-dd HH:mm:ss"),
        F.to_timestamp("order_ts", "dd-MM-yyyy HH:mm") 
    )
)

df = df.withColumn(
    "item_seq",
    F.col("item_seq").cast("int")
)

df = df.withColumn(
    "tax_amount",
    F.regexp_replace(F.col("tax_amount"), r"[^0-9.\-]", "").cast("double")
)

df = df.withColumn(
    "processed_time", F.current_timestamp()
)

# COMMAND ----------

display(df.limit(5))

# COMMAND ----------

df.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.slv_order_items")

# COMMAND ----------

