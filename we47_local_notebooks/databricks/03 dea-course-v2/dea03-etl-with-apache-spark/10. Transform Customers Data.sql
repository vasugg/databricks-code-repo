-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Transform Customer Data
-- MAGIC 1. Remove records with NULL customer_id 
-- MAGIC 1. Remove exact duplicate records
-- MAGIC 1. Remove duplicate records based on created_timestamp
-- MAGIC 1. CAST the columns to the correct Data Type
-- MAGIC 1. Write transformed data to the Silver schema

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1. Remove records with NULL customer_id

-- COMMAND ----------

SELECT * 
 FROM gizmobox.bronze.v_customers
WHERE customer_id IS NOT NULL; 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2. Remove exact duplicate records

-- COMMAND ----------

SELECT * 
 FROM gizmobox.bronze.v_customers
WHERE customer_id IS NOT NULL
ORDER BY customer_id; 

-- COMMAND ----------

SELECT DISTINCT * 
 FROM gizmobox.bronze.v_customers
WHERE customer_id IS NOT NULL
ORDER BY customer_id; 

-- COMMAND ----------

SELECT customer_id,
      MAX(created_timestamp),
      MAX(customer_name),
      MAX(date_of_birth),
      MAX(email),
      MAX(member_since),
      MAX(telephone)
 FROM gizmobox.bronze.v_customers
WHERE customer_id IS NOT NULL
GROUP BY customer_id
ORDER BY customer_id; 

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW v_customers_distinct
AS
SELECT DISTINCT * 
 FROM gizmobox.bronze.v_customers
WHERE customer_id IS NOT NULL
ORDER BY customer_id; 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3. Remove duplicate records based on created_timestamp

-- COMMAND ----------

SELECT customer_id,
       MAX(created_timestamp) AS max_created_timestamp
 FROM v_customers_distinct
GROUP BY customer_id;

-- COMMAND ----------

WITH cte_max AS 
(
  SELECT customer_id,
       MAX(created_timestamp) AS max_created_timestamp
  FROM v_customers_distinct
  GROUP BY customer_id
)
SELECT t.*
  FROM v_customers_distinct t
  JOIN cte_max m 
    ON t.customer_id = m.customer_id 
    AND t.created_timestamp = m.max_created_timestamp;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 4. CAST the column values to the correct data type

-- COMMAND ----------

WITH cte_max AS 
(
  SELECT customer_id,
       MAX(created_timestamp) AS max_created_timestamp
  FROM v_customers_distinct
  GROUP BY customer_id
)
SELECT CAST(t.created_timestamp AS TIMESTAMP) AS created_timestamp,
       t.customer_id,
       t.customer_name,
       CAST(t.date_of_birth AS DATE) AS date_of_birth,
       t.email,
       CAST(t.member_since AS DATE) AS member_since,
       t.telephone
  FROM v_customers_distinct t
  JOIN cte_max m 
    ON t.customer_id = m.customer_id 
    AND t.created_timestamp = m.max_created_timestamp;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 5. Write Data to a Delta Table

-- COMMAND ----------

CREATE TABLE gizmobox.silver.customers
AS
WITH cte_max AS 
(
  SELECT customer_id,
       MAX(created_timestamp) AS max_created_timestamp
  FROM v_customers_distinct
  GROUP BY customer_id
)
SELECT CAST(t.created_timestamp AS TIMESTAMP) AS created_timestamp,
       t.customer_id,
       t.customer_name,
       CAST(t.date_of_birth AS DATE) AS date_of_birth,
       t.email,
       CAST(t.member_since AS DATE) AS member_since,
       t.telephone
  FROM v_customers_distinct t
  JOIN cte_max m 
    ON t.customer_id = m.customer_id 
    AND t.created_timestamp = m.max_created_timestamp;

-- COMMAND ----------

SELECT * FROM gizmobox.silver.customers;

-- COMMAND ----------

DESCRIBE EXTENDED gizmobox.silver.customers;
