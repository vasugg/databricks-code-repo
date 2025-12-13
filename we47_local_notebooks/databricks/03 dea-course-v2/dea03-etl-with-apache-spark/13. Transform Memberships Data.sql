-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Transform Memberships Data
-- MAGIC 1. Extract customer_id from the file path 
-- MAGIC 1. Write transformed data to the Silver schema  

-- COMMAND ----------

SELECT * FROM gizmobox.bronze.v_memberships;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1. Extract customer_id from the file path
-- MAGIC > [Documentation for regexp_extract Function](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/functions/regexp_extract)  
-- MAGIC > [Documentation for Regex Pattern](https://docs.oracle.com/javase/7/docs/api/java/util/regex/Pattern.html)

-- COMMAND ----------

SELECT regexp_extract(path, '.*/([0-9]+)\\.png$', 1) AS customer_id,
       content AS membership_card
  FROM gizmobox.bronze.v_memberships;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2. Write transformed data to the Silver schema 

-- COMMAND ----------

CREATE TABLE gizmobox.silver.memberships
AS
SELECT regexp_extract(path, '.*/([0-9]+)\\.png$', 1) AS customer_id,
       content AS membership_card
  FROM gizmobox.bronze.v_memberships;

-- COMMAND ----------

SELECT * FROM gizmobox.silver.memberships;
