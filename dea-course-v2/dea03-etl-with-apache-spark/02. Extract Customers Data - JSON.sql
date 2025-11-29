-- Databricks notebook source
-- MAGIC %md 
-- MAGIC ## Extract Data From the Customers JSON File
-- MAGIC 1. Query Single File
-- MAGIC 1. Query List of Files using wildcard Characters
-- MAGIC 1. Query all the files in a Folder
-- MAGIC 1. Select File Metadata
-- MAGIC 1. Register Files in Unity Catalog using Views
-- MAGIC 1. Create Temporary View
-- MAGIC 1. Create Global Temporary View

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1. Query Single JSON File

-- COMMAND ----------

-- MAGIC %fs ls /Volumes/gizmobox/landing/operational_data/customers/

-- COMMAND ----------

SELECT * FROM json.`/Volumes/gizmobox/landing/operational_data/customers/customers_2024_10.json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2. Query Multiple JSON Files

-- COMMAND ----------

SELECT * FROM json.`/Volumes/gizmobox/landing/operational_data/customers/customers_2024_*.json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3. Query all the files in a folder

-- COMMAND ----------

SELECT * FROM json.`/Volumes/gizmobox/landing/operational_data/customers`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 4. Select File Metadata

-- COMMAND ----------

SELECT input_file_name() AS file_path, -- Deprecated from Databricks Runtime 13.3 LTS onwards,
       _metadata.file_path AS file_path,
       * 
  FROM json.`/Volumes/gizmobox/landing/operational_data/customers`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 5. Register Files in Unity Catalog using Views

-- COMMAND ----------

CREATE OR REPLACE VIEW gizmobox.bronze.v_customers
AS
SELECT *,
       _metadata.file_path AS file_path 
  FROM json.`/Volumes/gizmobox/landing/operational_data/customers`

-- COMMAND ----------

SELECT * FROM gizmobox.bronze.v_customers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 6. Create Temporary View
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW tv_customers
AS
SELECT *,
       _metadata.file_path AS file_path 
  FROM json.`/Volumes/gizmobox/landing/operational_data/customers`

-- COMMAND ----------

SELECT * FROM tv_customers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 7. Create Global Temporary View

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMPORARY VIEW gtv_customers
AS
SELECT *,
       _metadata.file_path AS file_path 
  FROM json.`/Volumes/gizmobox/landing/operational_data/customers`

-- COMMAND ----------

SELECT * FROM global_temp.gtv_customers
