# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS ecommerce;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG ecommerce;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS ecommerce.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS ecommerce.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS ecommerce.gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW DATABASES FROM ecommerce;

# COMMAND ----------

