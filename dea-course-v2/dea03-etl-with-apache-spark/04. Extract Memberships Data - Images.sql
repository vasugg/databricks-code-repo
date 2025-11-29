-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Extract Data From the Memberships - Image Files
-- MAGIC 1. Query Memberships File using binaryFile Format
-- MAGIC 1. Create Memberships View in Bronze Schema

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1. Query Memberships Folder using binaryFile Format

-- COMMAND ----------

-- MAGIC %fs ls '/Volumes/gizmobox/landing/operational_data/memberships'

-- COMMAND ----------

SELECT * FROM binaryFile.`/Volumes/gizmobox/landing/operational_data/memberships/2024-10/*.png`

-- COMMAND ----------

SELECT * FROM binaryFile.`/Volumes/gizmobox/landing/operational_data/memberships/*/*.png`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2. Create Memberships View in Bronze Schema

-- COMMAND ----------

CREATE OR REPLACE VIEW gizmobox.bronze.v_memberships
AS
SELECT * FROM binaryFile.`/Volumes/gizmobox/landing/operational_data/memberships/*/*.png`;

-- COMMAND ----------

SELECT * FROM gizmobox.bronze.v_memberships;
