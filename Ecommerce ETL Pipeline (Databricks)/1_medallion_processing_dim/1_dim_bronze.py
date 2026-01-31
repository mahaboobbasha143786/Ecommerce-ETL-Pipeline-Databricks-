# Databricks notebook source
# DBTITLE 1,Cell 1
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, FloatType, TimestampType
import pyspark.sql.functions as F

# COMMAND ----------

catalog_name = 'ecommerce'

brand_schema = StructType([
    StructField('brand_code', StringType(), False),
    StructField('brand_name', StringType(), True),
    StructField('category_code', StringType(), True)

])



# COMMAND ----------

raw_data_path = '/Volumes/ecommerce/source_data/raw/brands/*.csv'

df = spark.read.option("header", True).option('delimiter', ',').schema(brand_schema).csv(raw_data_path)
df = df.withColumn("_source_file", F.col("_metadata.file_path"))\
    .withColumn("ingeted_at", F.current_timestamp())
display(df.limit(5))

# COMMAND ----------

# DBTITLE 1,Untitled
df.write.format("delta")\
    .mode("overwrite")\
    .option("mergeschema", "True")\
    .saveAsTable(f"{catalog_name}.bronze.brz_brands")

# COMMAND ----------

category_schema = StructType([
    StructField('category_code', StringType(), False),
    StructField('category_name', StringType(), True)
    ])


raw_data_path = '/Volumes/ecommerce/source_data/raw/category/*.csv'

df = spark.read.option("header", True).option('delimiter', ',').schema(category_schema).csv(raw_data_path)
df = df.withColumn("_source_file", F.col("_metadata.file_path"))\
    .withColumn("ingeted_at", F.current_timestamp())
display(df.limit(5))
df.write.format("delta")\
    .mode("overwrite")\
    .option("mergeschema", "True")\
    .saveAsTable(f"{catalog_name}.bronze.brz_category")

    

# COMMAND ----------

products_schema = StructType([
    StructField('product_id', StringType(), False),
    StructField('sku', StringType(), True),
    StructField("category_code", StringType(), True),
    StructField('brand_code', StringType(), True),
    StructField('color', StringType(), True),
    StructField('size', StringType(), True),
    StructField('material', StringType(), True),
    StructField('weight_grams', StringType(), True),
    StructField('length_cm', StringType(), True),
    StructField('width_cm', FloatType(), True),
    StructField('height_cm', FloatType(), True),
    StructField('rating_count', IntegerType(), True),
    StructField('_source_file', StringType(), True),
    StructField('ingeted_at', TimestampType(), False)
])
raw_data_path = '/Volumes/ecommerce/source_data/raw/products/*.csv'

df = spark.read.option("header", True).option('delimiter', ',').schema(products_schema).csv(raw_data_path) \
    .withColumn("_source_file", F.col("_metadata.file_path"))\
    .withColumn("ingeted_at", F.current_timestamp())
display(df.limit(5))
df.write.format("delta") \
    .mode("overwrite") \
    .option("mergeschema", "True") \
    .saveAsTable(f"{catalog_name}.bronze.brz_products")

   
   
    

# COMMAND ----------

# DBTITLE 1,Cell 7
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import pyspark.sql.functions as F
catalog_name = "ecommerce"

date_schema = StructType([
    StructField('date', StringType(), True),
    StructField('year', IntegerType(), True),
    StructField('day_name', StringType(), True),
    StructField('quarter', IntegerType(), True),
    StructField('week_of_year', IntegerType(), True)
    ])
raw_data_path = '/Volumes/ecommerce/source_data/raw/date/*.csv'

df = spark.read.option("header", True).option('delimiter', ',').schema(date_schema).csv(raw_data_path) \
    .withColumn("_source_file", F.col("_metadata.file_path"))\
    .withColumn("ingested_at", F.current_timestamp())
display(df.limit(5))
df = df.withColumn("date", F.col("date").cast("string"))
df.write.format("delta") \
    .mode("overwrite") \
    .option("mergeschema", "True") \
    .saveAsTable(f"{catalog_name}.bronze.brz_calender")

# COMMAND ----------

from pyspark.sql.types import *
import pyspark.sql.functions as F
customers_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("phone", StringType(), True),
    StructField("country_code", StringType(), True),
    StructField("country", StringType(), True),
    StructField("state", StringType(), True)

])

raw_data_path = "/Volumes/ecommerce/source_data/raw/customers/*.csv"

df = spark.read.option("header", True).option('delimiter', ',')\
    .schema(customers_schema).csv(raw_data_path).withColumn("_source_file", F.col("_metadata.file_path"))\
    .withColumn("ingest_timestamp", F.current_timestamp())
df.write.format("delta")\
    .mode("overwrite")\
    .option("mergeSchema", True)\
    .saveAsTable(f"{catalog_name}.bronze.brz_customers")

# COMMAND ----------

