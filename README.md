# 🛒 Ecommerce ETL Pipeline using Databricks

This project implements an end-to-end **Ecommerce ETL Pipeline** using the **Medallion Architecture (Bronze, Silver, Gold)** on **Databricks / Apache Spark**.

---

## 🚀 Architecture Overview

**Medallion Architecture**
- **Bronze Layer**: Raw ingestion
- **Silver Layer**: Cleaned & transformed data
- **Gold Layer**: Analytics-ready fact & dimension tables

---

## 🧱 Layers Explained

### 🟤 Bronze Layer
- Raw data ingestion
- Minimal transformations
- Schema enforcement

Scripts:
- `1_dim_bronze.py`
- `1_fact_bronze.py`

---

### ⚪ Silver Layer
- Data cleansing
- Deduplication
- Business logic applied

Scripts:
- `2_dim_silver.py`
- `2_fact_silver.py`

---

### 🟡 Gold Layer
- Star schema modeling
- Aggregations
- Analytics-ready tables

Scripts:
- `3_dim_gold.py`
- `3_fact_gold.py`

---

## 🛠 Tech Stack
- Databricks
- Apache Spark (PySpark)
- Delta Lake
- Python
- Git & GitHub

---

## 📊 Data Model
Star Schema with:
- Fact Tables: Orders, Sales
- Dimension Tables: Customers, Products, Date

---

## ▶️ How to Run
1. Upload notebooks to Databricks
2. Run Bronze → Silver → Gold sequentially
3. Validate tables in Delta Lake

---

## 📌 Future Enhancements
- Airflow orchestration
- CDC using Auto Loader
- Data quality checks
- Unity Catalog integration

---

## 👤 Author
**Shaik Mahaboob Basha**

