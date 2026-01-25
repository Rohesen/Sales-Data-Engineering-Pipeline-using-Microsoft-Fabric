# 📊 End-to-End Data Engineering Pipeline using Microsoft Fabric

## 🚀 Project Overview

This project demonstrates an **end-to-end data engineering pipeline** built using **Microsoft Fabric**, **Apache Spark**, and **Delta Lake** following a **Medallion Architecture (Raw → Landing → Bronze → Silver → Gold)**.

The pipeline ingests raw CSV sales data, applies data quality checks and transformations, and produces **analytics-ready fact and dimension tables**. The entire flow is orchestrated using **Fabric Pipelines** with success and failure notifications.

---

## 🏗️ Architecture

**Medallion Layers:**

```
Raw Files
   ↓
Landing (Partitioned by Processing_Date)
   ↓
Bronze (Delta | Deduplicated & Incremental)
   ↓
Silver (Business Transformations)
   ↓
Gold (Star Schema: Dimensions + Fact)
```

**Technologies Used**

* Microsoft Fabric
* Apache Spark (PySpark & Spark SQL)
* Delta Lake
* OneLake (ABFSS)
* Fabric Pipelines (Orchestration)
* Office 365 Email (Notifications)

---

## 🔄 Pipeline Orchestration

The Fabric pipeline executes the following steps sequentially:

1. **Raw → Landing**
2. **Landing → Bronze**
3. **Bronze → Silver**
4. **Silver → Gold**
5. **Email Notification (Success / Failure)**

✔ Fully automated
✔ Idempotent
✔ Production-ready

---

## 📁 Notebooks Breakdown

---

### 1️⃣ Raw → Landing

**Purpose**

* Read raw CSV files from OneLake
* Add `Processing_Date`
* Store data in a partitioned Landing zone

**Key Features**

* Schema inference
* Partitioning by `Processing_Date`
* Data availability check

```python
df_new = df.withColumn('Processing_Date', lit(processed_date))

df_new.write \
    .format('csv') \
    .option("header", "true") \
    .mode("append") \
    .partitionBy("Processing_Date") \
    .save(".../Files/Landing")
```

---

### 2️⃣ Landing → Bronze

**Purpose**

* Enforce schema
* Deduplicate records
* Perform incremental UPSERT into Delta Bronze table

**Key Features**

* Explicit schema definition
* Deterministic deduplication using `ROW_NUMBER`
* Delta `MERGE` (SCD Type 1)

```sql
MERGE INTO tblsales_bronze
USING t_new_data_dedup
ON Order_ID AND Customer_ID
```

✔ Handles late-arriving data
✔ Prevents duplicate key conflicts

---

### 3️⃣ Bronze → Silver (Transformations)

**Purpose**

* Apply business logic
* Improve data quality
* Create enriched metrics

**Transformations**

* Remove null keys
* Remove duplicates
* Calculate delivery days
* Calculate profit margin

```python
df_days = df.withColumn(
    'Delivery_Days',
    (col('Ship_Date') - col('Order_Date')).cast('int')
)

df_final = df_days.withColumn(
    'Profit_Margin',
    col('Profit') / col('Sales')
)
```

**Silver Table**

* `tblsales_silver`
* Delta format
* Incremental MERGE

---

### 4️⃣ Silver → Gold (Star Schema)

**Gold Tables Created**

#### 🟡 Dimension Tables

* `Dim_Customer`
* `Dim_Product`

✔ Deduplicated by business key
✔ SCD Type 1
✔ Deterministic logic using Window functions

```python
Window.partitionBy("Customer_ID")
```

---

#### 🔵 Fact Table

* `Fact_Sales`

✔ Incremental MERGE
✔ Analytics-ready
✔ Fully conformed dimensions

---

### 5️⃣ Monitoring & Metrics

After every MERGE:

* Rows inserted
* Rows updated
* Rows deleted
* Total affected rows

```python
history_df = delta_table.history(1)
```

---

## 📈 Key Design Decisions

* **Delta Lake MERGE** for incremental processing
* **Window-based deduplication** to avoid ambiguous MERGE errors
* **Partition pruning** using `processing_date`
* **Idempotent pipelines**
* **Separation of concerns** (Raw vs Bronze vs Silver vs Gold)

---

## 📬 Notifications

* ✅ Success Email on successful pipeline run
* ❌ Failure Email on pipeline failure

Implemented using **Office 365 Email activity** in Fabric Pipelines.

---

## 🧪 Data Model (Gold Layer)

**Star Schema**

```
            Dim_Customer
                  |
                  |
Dim_Product — Fact_Sales
```

Optimized for:

* BI tools
* Reporting
* Analytics workloads

---

## 🧠 Learnings & Highlights

* Real-world Delta MERGE pitfalls and fixes
* Source deduplication before MERGE
* Dimension vs Fact modeling rules
* Fabric OneLake + Delta best practices
* End-to-end orchestration in Microsoft Fabric

---

## 📌 Future Enhancements

* SCD Type 2 for dimensions
* Data Quality framework
* Parameterized pipelines
* Incremental file ingestion
* Power BI semantic model

---

## 👤 Author

**Rohesen Maurya**
Data Engineer | Microsoft Fabric | Spark | Delta Lake
