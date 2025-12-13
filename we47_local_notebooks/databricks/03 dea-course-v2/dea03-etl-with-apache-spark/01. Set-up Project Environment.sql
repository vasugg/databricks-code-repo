-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Set-up the project environment for GizmoBox Data Lakehouse

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3. Create the catalog - gizmobox

-- COMMAND ----------

SHOW CATALOGS;

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS gizmobox
      
      COMMENT 'This is the catalog for the Gizmobox Data Lakehouse' ;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 4. Create Schemas
-- MAGIC 1. Landing
-- MAGIC 1. Bronze
-- MAGIC 1. Silver
-- MAGIC 1. Gold

-- COMMAND ----------

SELECT current_catalog();

-- COMMAND ----------

USE CATALOG gizmobox;


CREATE SCHEMA IF NOT EXISTS landing;
     
CREATE SCHEMA IF NOT EXISTS bronze;
      
CREATE SCHEMA IF NOT EXISTS silver;
      
CREATE SCHEMA IF NOT EXISTS gold;
                

-- COMMAND ----------

SHOW SCHEMAS;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 5. Create Volume

-- COMMAND ----------

USE CATALOG gizmobox;
USE SCHEMA landing;

CREATE VOLUME IF NOT EXISTS operational_data;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Instead of using the abfss (External azure path), we can use the volumes path as shown below

-- COMMAND ----------

-- MAGIC %fs ls /Volumes/gizmobox/landing/operational_data/addresses

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Creating a new folder inside the volume using python code
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC     dbutils.fs.mkdirs("/Volumes/gizmobox/landing/operational_data/test")
-- MAGIC     
