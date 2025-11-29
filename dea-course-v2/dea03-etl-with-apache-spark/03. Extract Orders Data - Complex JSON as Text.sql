-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Extract Data From the Orders JSON File
-- MAGIC 1. Query Orders File using JSON Format
-- MAGIC 1. Query Orders File using TEXT Format
-- MAGIC 1. Create Orders View in Bronze Schema

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1. Query Orders File using JSON Format

-- COMMAND ----------

SELECT * FROM json.`/Volumes/gizmobox/landing/operational_data/orders`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2. Query Orders File using TEXT Format

-- COMMAND ----------

SELECT * FROM text.`/Volumes/gizmobox/landing/operational_data/orders`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3. Create Orders View in Bronze Schema

-- COMMAND ----------

CREATE OR REPLACE VIEW gizmobox.bronze.v_orders
AS
SELECT * FROM text.`/Volumes/gizmobox/landing/operational_data/orders`;

-- COMMAND ----------

SELECT * FROM gizmobox.bronze.v_orders;
