-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Data profiling in Databricks
-- MAGIC 1. Profile Data using UI
-- MAGIC 1. Profile Data using DBUTILS Package (dbutils.data.summarize method)
-- MAGIC 1. Profile Data Manually 
-- MAGIC    - COUNT
-- MAGIC    - COUNT_IF
-- MAGIC    - MIN
-- MAGIC    - MAX
-- MAGIC    - WHERE Clause

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1. Profile Data using User Interface

-- COMMAND ----------

SELECT * FROM gizmobox.bronze.v_customers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2. Profile Data using DBUTILS Package (dbutils.data.summarize method)

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC df = spark.table('gizmobox.bronze.v_customers')
-- MAGIC dbutils.data.summarize(df)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3. Profile Data Manually 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 3.1 COUNT Function

-- COMMAND ----------

SELECT COUNT(*), COUNT(customer_id), COUNT(email), COUNT(telephone)
FROM gizmobox.bronze.v_customers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 3.1 COUNT_IF Function

-- COMMAND ----------

SELECT COUNT(*), COUNT_IF(customer_id IS NULL), COUNT_IF(email IS NULL), COUNT_IF(telephone IS NULL)
FROM gizmobox.bronze.v_customers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 3.2 WHERE Clause

-- COMMAND ----------

SELECT COUNT(*)
FROM gizmobox.bronze.v_customers
WHERE customer_id IS NULL;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 3.4 DISTINCT Keyword

-- COMMAND ----------

SELECT COUNT(*) total_number_of_records,
       COUNT(DISTINCT customer_id) unique_customer_ids
  FROM gizmobox.bronze.v_customers
 WHERE customer_id IS NOT NULL;
