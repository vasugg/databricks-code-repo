-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Extract Data From the Address Files
-- MAGIC 1. Demonstrate Limitations of CSV file format in SELECT statement
-- MAGIC 1. Use read_files function to overcome the limitations
-- MAGIC 1. Create Addresses view in the Bronze Layer

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1. Demonstrate Limitations of CSV file format in SELECT statement

-- COMMAND ----------

SELECT * FROM csv.`/Volumes/gizmobox/landing/operational_data/addresses`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2. Use read_files function to overcome the limitations
-- MAGIC [Function Documentaion](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/functions/read_files)

-- COMMAND ----------

SELECT * 
  FROM read_files('/Volumes/gizmobox/landing/operational_data/addresses',
                  format => 'csv',
                  delimiter => '\t',
                  header => true);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3. Create Addresses view in the Bronze Layer

-- COMMAND ----------

CREATE OR REPLACE VIEW gizmobox.bronze.v_addresses
AS
SELECT * 
  FROM read_files('/Volumes/gizmobox/landing/operational_data/addresses',
                  format => 'csv',
                  delimiter => '\t',
                  header => true);

-- COMMAND ----------

SELECT * FROM gizmobox.bronze.v_addresses;
