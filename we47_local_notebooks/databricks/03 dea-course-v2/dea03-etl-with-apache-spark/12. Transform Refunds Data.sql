-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Transform Refunds Data
-- MAGIC 1. Extract specific portion of the string from refund_reason using split function
-- MAGIC 1. Extract specific portion of the string from refund_reason using regexp_extract function
-- MAGIC 1. Extract date and time from the refund_timestamp
-- MAGIC 1. Write transformed data to the Silver schema in hive metastore [Default Location: /user/hive/warehouse]

-- COMMAND ----------

SELECT refund_id,
       payment_id,
       refund_timestamp, 
       refund_amount,
       refund_reason
  FROM hive_metastore.bronze.refunds;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1. Extract specific portion of the string from refund_reason using split function
-- MAGIC > [Documentation for split Function](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/functions/split)

-- COMMAND ----------

SELECT refund_id,
       payment_id,
       refund_timestamp, 
       refund_amount,
       SPLIT(refund_reason, ':')[0] AS refund_reason,
       SPLIT(refund_reason, ':')[1] AS refund_
  FROM hive_metastore.bronze.refunds;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2. Extract specific portion of the string from refund_reason using regexp_extract function
-- MAGIC > [Documentation for regexp_extract Function](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/functions/regexp_extract)  
-- MAGIC > [Regex Pattern](https://docs.oracle.com/javase/7/docs/api/java/util/regex/Pattern.html)

-- COMMAND ----------

SELECT refund_id,
       payment_id,
       CAST(date_format(refund_timestamp, 'yyyy-MM-dd') AS DATE) AS refund_date,
       date_format(refund_timestamp, 'HH:mm:ss') AS refund_time,
       refund_amount,
       regexp_extract(refund_reason, '^([^:]+):', 1) AS refund_reason,
       regexp_extract(refund_reason, '^[^:]+:(.*)$', 1) AS refund_source
  FROM hive_metastore.bronze.refunds;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3. Write transformed data to the Silver schema  

-- COMMAND ----------

CREATE SCHEMA hive_metastore.silver;

-- COMMAND ----------

CREATE TABLE hive_metastore.silver.refunds
AS
SELECT refund_id,
       payment_id,
       CAST(date_format(refund_timestamp, 'yyyy-MM-dd') AS DATE) AS refund_date,
       date_format(refund_timestamp, 'HH:mm:ss') AS refund_time,
       refund_amount,
       regexp_extract(refund_reason, '^([^:]+):', 1) AS refund_reason,
       regexp_extract(refund_reason, '^[^:]+:(.*)$', 1) AS refund_source
  FROM hive_metastore.bronze.refunds;

-- COMMAND ----------

SELECT * FROM hive_metastore.silver.refunds;

-- COMMAND ----------

DESC EXTENDED hive_metastore.silver.refunds;
