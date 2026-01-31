# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, IntegerType, DateType, TimestampType, FloatType
from pyspark.sql import Row

# COMMAND ----------

catalog_name = "ecommerce"

# COMMAND ----------

df_products = spark.read.table(f"{catalog_name}.silver.slv_products")
df_brands = spark.read.table(f"{catalog_name}.silver.slv_brands")
df_category = spark.read.table(f"{catalog_name}.silver.slv_category")



# COMMAND ----------

df_products.createOrReplaceTempView("v_products")
df_brands.createOrReplaceTempView("v_brands")
df_category.createOrReplaceTempView("v_category")

# COMMAND ----------

display(spark.sql("SELECT * FROM v_products LIMIT 5"))

# COMMAND ----------

display(spark.sql("SELECT * FROM v_category LIMIT 5"))

# COMMAND ----------

display(spark.sql("SELECT * FROM v_brands LIMIT 5"))


# COMMAND ----------

spark.sql(f"USE CATALOG {catalog_name}")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.gld_dim_products AS
# MAGIC
# MAGIC WITH brands_categories AS (
# MAGIC   SELECT
# MAGIC     b.brand_name,
# MAGIC     b.brand_code,
# MAGIC     c.category_name,
# MAGIC     c.category_code
# MAGIC   FROM v_brands b
# MAGIC   INNER JOIN v_category c
# MAGIC   ON c.category_code = b.category_code
# MAGIC )
# MAGIC SELECT 
# MAGIC   p.product_id,
# MAGIC   p.sku,
# MAGIC   p.category_code,
# MAGIC   COALESCE(bc.category_name, 'Not Available') as category_name,
# MAGIC   p.brand_code,
# MAGIC   COALESCE(bc.brand_name, 'Not Available') as brand_name,
# MAGIC   p.color,
# MAGIC   p.size,
# MAGIC   p.material,
# MAGIC   p.weight_grams,
# MAGIC   p.length_cm,
# MAGIC   p.width_cm,
# MAGIC   p.height_cm,
# MAGIC   p.rating_count,
# MAGIC   p._source_file,
# MAGIC   p.ingeted_at
# MAGIC FROM v_products p
# MAGIC LEFT JOIN brands_categories bc
# MAGIC ON p.brand_code = bc.brand_code;
# MAGIC
# MAGIC --Here is the error message:

# COMMAND ----------

# DBTITLE 1,Cell 10
# Indian States
india_region = {
    "MH" : "West", "GJ" : "West", "RJ" : "West",
    "KA" : "South", "TN" : "South", "TS" : "South", "AP" : "South", "KL" : "South",
    "UP" : "North", "WB" : "North", "DL" : "North"
}

# Australian States
australia_region = {
    "VIC" : "SouthEast", "WA" : "West", "NSW" : "East", "QLD" : "NorthEast"
}

# UK States
uk_region = {
    "ENG" : "England", "SCO" : "Scotland", "WAL" : "Wales", "NI" : "Northern Ireland"
}

# US States
us_region = {
    "MA" : "NorthEast", "FL" : "South", "NJ" : "NorthEast", "NY" : "NorthEast", "CA" : "West",
    "TX" : "South"
}

# UAE States
uae_region = {
    "AUH" : "Abu Dhabi", "DU" : "Dubai", "SHJ" : "Sharjah"
}

# Singapore States
singapore_region = {
    "SG" : "Singapore"
}

# Canada States
canada_region = {
    "ON" : "East", "QC" : "East", "BC" : "West", "AB" : "West", "NS" : "East", "IL" : "Other"
}

# Combine into a master dictionary
country_state_map = {
    "India" : india_region,
    "Australia" : australia_region,
    "United Kingdom" : uk_region,
    "United States" : us_region,
    "United Arab Emirates" : uae_region,
    "Singapore" : singapore_region,
    "Canada" : canada_region
}


# COMMAND ----------

country_state_map

# COMMAND ----------

# Flatten country_state_map into a Row

rows = []
for country, states in country_state_map.items():
    for state, region in states.items():
        rows.append(Row(country=country, state=state, region=region))
rows[:10]

# COMMAND ----------

df_region_mapping = spark.createDataFrame(rows)
df_region_mapping.show(truncate=False)

# COMMAND ----------

df_silver = spark.read.table(f"{catalog_name}.silver.slv_customers")
display(df_silver.limit(5))

# COMMAND ----------

df_gold = df_silver.join(df_region_mapping, on=["country", "state"], how="left")
df_gold = df_gold.fillna({"region" : "Other"})
display(df_gold.limit(5))


# COMMAND ----------

df_gold.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "True") \
    .saveAsTable(f"{catalog_name}.gold.gld_dim_customers")

# COMMAND ----------

catalog_name = "ecommerce"
df_silver = spark.read.table(f"{catalog_name}.silver.slv_calender")
display(df_silver.limit(5))

# COMMAND ----------

import pyspark.sql.functions as F

df_gold = df_silver.withColumn("date_id", F.date_format(F.col("date"), "yyyyMMdd").cast("int"))

df_gold = df_gold.withColumn("month_name", F.date_format(F.col("date"), "MMMM"))
df_gold = df_gold.withColumn(
    "is_weekend",
    F.when(F.col("day_name").isin(["Saturday", "Sunday"]), 1).otherwise(0)

)
display(df_gold.limit(5))

# COMMAND ----------

desired_columns = ["date_id", "date", "year","month_name", "day_name","is_weekend", "quarter", "week", "ingested_at", "_source_file"]
df_gold = df_gold.select(desired_columns)

display(df_gold.limit(5))


# COMMAND ----------

df_gold.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "True") \
    .saveAsTable(f"{catalog_name}.gold.gld_dim_date")

# COMMAND ----------

