# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, IntegerType, DateType, TimestampType, FloatType


catalog_name = 'ecommerce'

# COMMAND ----------

df_bronze = spark.table(f"{catalog_name}.bronze.brz_brands")
df_bronze.show(10)

# COMMAND ----------

df_silver = df_bronze.withColumn("brand_code", F.regexp_replace(F.col("brand_code"), r'[^A-Za-z0-9 ]', ''))
df_silver.show(10)

# COMMAND ----------

df_silver.select("category_code").distinct().display()

# COMMAND ----------

anomalies = {
    "BOOKS": "BKS",
    "GROCERY": "GRCY",
    "TOYS": "TOY"
}
df_silver = df_silver.replace(anomalies, subset="category_code")
df_silver.select("category_code").distinct().show()


# COMMAND ----------

df_silver.write.format("delta")\
    .mode("overwrite")\
    .option("mergeSchema", "True")\
    .saveAsTable(f"{catalog_name}.silver.slv_brands")

# COMMAND ----------

df_bronze = spark.table(f"{catalog_name}.bronze.brz_category")
df_bronze.show(10)


# COMMAND ----------

df_duplicates = df_bronze.groupBy("category_code").count().filter(F.col("count") > 1)
df_duplicates.show()

# COMMAND ----------

df_silver = df_bronze.dropDuplicates(["category_code"])
df_silver.show()

# COMMAND ----------

df_silver = df_silver.withColumn("category_code", F.upper(F.col("category_code")))
display(df_silver)

# COMMAND ----------

df_silver.write.format("delta")\
    .mode("overwrite")\
    .option("mergeSchema", "True")\
    .saveAsTable(f"{catalog_name}.silver.slv_category")

# COMMAND ----------

catalog_name = "ecommerce"
df_bronze = spark.table(f"{catalog_name}.bronze.brz_products")

row_count, column_count = df_bronze.count(), len(df_bronze.columns)
print(f"Row count: {row_count}\ncolumn count: {column_count}")


# COMMAND ----------

df_bronze.display(5, truncate=False)

# COMMAND ----------

df_silver = df_bronze.withColumn("weight_grams", F.regexp_replace(F.col("weight_grams"), "g", "").cast(IntegerType()))
df_silver.display(5, truncate=False)

# COMMAND ----------

df_silver = df_silver.withColumn("length_cm", \
    F.regexp_replace(F.col("length_cm"), ",", ".").cast(FloatType()))
df_silver.select("length_cm").display(5, truncate=False)

# COMMAND ----------

df_silver = df_silver.withColumn("category_code", F.upper(F.col("category_code")))
df_silver.select("category_code").display(5, truncate=False)
df_silver = df_silver.withColumn("brand_code", F.upper(F.col("brand_code")))
df_silver.select("brand_code").display(5, truncate=False)

# COMMAND ----------

df_silver.select("material").display(5, truncate=False)


# COMMAND ----------

df_silver = df_silver.drop()
df_silver.display()

# COMMAND ----------

df_silver = df_silver.withColumn(
    "material",
    F.when(F.col("material") == "Coton", "Cotton") 
    .when(F.col("material") == "Alumium", "Aluminum") 
    .when(F.col("material") == "Ruber", "Rubber")
    .otherwise(F.col("material"))
)

df_silver.select("material").display(5, truncate=False)

# COMMAND ----------

df_silver.filter(F.col('rating_count') < 0).display()


# COMMAND ----------

df_silver = df_silver.withColumn("rating_count", 
                F.when(F.col("rating_count").isNotNull(),F.abs(F.col("rating_count")))
                .otherwise(F.lit(0)))
df_silver.filter(F.col('rating_count') < 0).display()


# COMMAND ----------

# DBTITLE 1,Cell 23
df_silver = df_silver.drop("file_name")
df_silver.display()

# COMMAND ----------

df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.slv_products")

# COMMAND ----------

# DBTITLE 1,Untitled
df_bronze = spark.table(f"{catalog_name}.bronze.brz_customers")

row_count, column_count = df_bronze.count(), len(df_bronze.columns)

print(f"Row count: {row_count}")
print(f"Column count: {column_count}")
df_bronze = df_bronze.withColumn("_source_file_path", F.col("_source_file"))
df_bronze = df_bronze.drop("_source_file")



df_bronze.display()

# COMMAND ----------

df_silver = df_bronze.drop("date", "year", "day_name", "quarter", "week_of_year", "ingeted_at")
df_silver.display()

# COMMAND ----------

df_nullcount = df_silver.filter(F.col("customer_id").isNull()).count()
df_nullcount

# COMMAND ----------

df_silver = df_silver.dropna(subset=["customer_id"])

raw_count = df_silver.count()

print(f"raw count after droping null values {raw_count}")

# COMMAND ----------

null_phone = df_silver.filter(F.col("phone").isNull()).count()

print(f"Number of nulls in phone :{null_phone}")

# COMMAND ----------

df_silver = df_silver.fillna("Not Available", subset=["phone"])

df_silver.filter(F.col("phone").isNull()).show()

# COMMAND ----------

df_silver.write.format("delta") \
    .mode("overwrite")\
    .option("mergeSchema", True)\
    .saveAsTable(f"{catalog_name}.silver.slv_customers")

# COMMAND ----------

catalog_name = "ecommerce"
df_bronze = spark.read.table(f"{catalog_name}.bronze.brz_calender")

row_count, column_count = df_bronze.count(), len(df_bronze.columns)

print(f"Row count: {row_count}")
print(f"Column count: {column_count}")

df_bronze.show(3)

# COMMAND ----------

# DBTITLE 1,Cell 32

from pyspark.sql.functions import to_date

df_silver = df_bronze.withColumn("date", to_date(F.col("date"), "dd-MM-yyyy"))





# COMMAND ----------

print(df_silver.printSchema())

df_silver.show(3)

# COMMAND ----------

duplicates = df_silver.groupBy("date").count().filter("count > 1")

print(f"Duplicates: {duplicates.count()}")
duplicates.display()

# COMMAND ----------

df_silver = df_silver.dropDuplicates(["date"])

row_count = df_silver.count()

print(f"Row count after droping duplicates: {row_count}")



# COMMAND ----------

df_silver = df_silver.withColumn("day_name", F.initcap(F.col("day_name")))

df_silver.display()

# COMMAND ----------

df_silver = df_silver.withColumn("week_of_year", F.abs(F.col("week_of_year")))

df_silver.display()


# COMMAND ----------

df_silver = df_silver.withColumn("quarter", F.concat_ws("", F.concat(F.lit("Q"), F.col("quarter"), F.lit("-"), F.col("year"))))

df_silver = df_silver.withColumn("week_of_year", F.concat_ws("-", F.concat(F.lit("Week"), F.col("week_of_year"), F.lit("-"), F.col("year"))))

df_silver.display()

# COMMAND ----------

df_silver = df_silver.withColumnRenamed("week_of_year", "week")
df_silver.show(5)

# COMMAND ----------



# COMMAND ----------

df_silver = df_silver.drop("ingeted_at")
df_silver.display()
df_silver.write.format("delta")\
    .mode("overwrite")\
    .option("mergeSchema", False)\
    .saveAsTable(f"{catalog_name}.silver.slv_calender")

# COMMAND ----------



# COMMAND ----------

