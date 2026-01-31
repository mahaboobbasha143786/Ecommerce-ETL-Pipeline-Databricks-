# Databricks notebook source
# MAGIC %md
# MAGIC ### Silver to Gold: Building BI Ready Table's

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import StringType, IntegerType, DateType, TimestampType, FloatType

catalog_name = "ecommerce"

# COMMAND ----------

df = spark.read.table(f"{catalog_name}.silver.slv_order_items")

display(df.select("coupon_code").filter(F.col("coupon_code").isNotNull()))


# COMMAND ----------

df = df.withColumn(
    "gross_amount",
    F.round(F.col("unit_price") * F.col("quantity") 
))

df = df.withColumn(
    "discount_amount",
    F.ceil(F.col("gross_amount") * (F.col("discount_pct") / 100.0))
)

df = df.withColumn(
    "sale_amount", 
    F.col("gross_amount") - F.col("discount_amount") + F.col("tax_amount")
)

df = df.withColumn("date_id", F.date_format(F.col("dt"), "yyyyMMdd").cast(IntegerType()))

df = df.withColumn(
    "coupon_flag",
    F.when(F.col("coupon_code").isNotNull(), F.lit(1)).otherwise(F.lit(0))
)



df.limit(10).display()



# COMMAND ----------

# Define your fixed currency with current rates as per INR

fixed_rate = {
    "INR": 1.00,
    "USD": 91.80,
    "GBP": 125.94,
    "CAD": 67.67,
    "AUD": 64.16,
    "SGD": 72.24,
    "AED": 25.03
}

rates = [(k, float(v)) for k, v in fixed_rate.items()]
# DBTITLE 1,Define the function to convert the currency
rates_df = spark.createDataFrame(rates, ["currency", "inr_rate"])
rates_df.show()

# COMMAND ----------

# DBTITLE 1,Cell 6
df = (
    df
    .join(
        rates_df,
        rates_df.currency == F.upper(F.trim(F.col("unit_price_currency"))),
        "left"
    )
    .withColumn("sale_amount_inr", F.col("sale_amount") * F.col("inr_rate"))
    .withColumn("sale_amount_inr", F.ceil(F.col("sale_amount_inr")))
)


# COMMAND ----------

df.limit(10).display()


# COMMAND ----------

orders_gold_df = df.select(
    F.col("date_id"),
    F.col("dt").alias("transaction_date"),
    F.col("order_ts").alias("transaction_ts"),
    F.col("order_id").alias("transaction_id"),
    F.col("customer_id"),
    F.col("item_seq").alias("seq_no"),
    F.col("product_id"),
    F.col("channel"),
    F.col("coupon_code"),
    F.col("coupon_flag"),
    F.col("unit_price_currency"),
    F.col("quantity"),
    F.col("unit_price"),
    F.col("gross_amount"),
    F.col("discount_pct").alias("discount_percent"),
    F.col("discount_amount"),
    F.col("tax_amount"),
    F.col("sale_amount").alias("net_amount"),
    F.col("sale_amount_inr").alias("net_amount_inr")
)
    

# COMMAND ----------

orders_gold_df.limit(10).display()

# COMMAND ----------

orders_gold_df.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "True") \
    .saveAsTable(f"{catalog_name}.gold.gld_fact_item_orders")

# COMMAND ----------

