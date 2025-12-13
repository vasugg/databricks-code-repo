-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Configure Access to Cloud Storage via Unity Catalog

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Access Cloud Storage

-- COMMAND ----------

-- MAGIC %fs ls 'abfss://demo@deacourseextdl.dfs.core.windows.net/'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create External Location

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS dea_course_ext_dl_demo
    URL 'abfss://demo@deacourseextdl.dfs.core.windows.net/'
    WITH (STORAGE CREDENTIAL dea_course_ext_sc)
    COMMENT 'External Location for Demo Purposes'
