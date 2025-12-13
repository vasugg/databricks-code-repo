-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Set-up the project environment for CircuitBox Data Lakehouse
-- MAGIC 1. Create external location - dea_course_ext_dl_circuitbox
-- MAGIC 1. Create Catalog - circuitbox
-- MAGIC 1. Create Schemas
-- MAGIC     - landing
-- MAGIC     - lakehouse
-- MAGIC 1. Create Volume - operational_data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ![](images/dea06-01-set-up-project-environment.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1. Create External Location
-- MAGIC **External Location Name:** dea_course_ext_dl_circuitbox  
-- MAGIC _ADLS Path:_ [abfss://circuitbox@deacourseextdl.dfs.core.windows.net/](abfss://circuitbox@deacourseextdl.dfs.core.windows.net/)  
-- MAGIC _Storage Credential:_ dea_course_ext_sc

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS dea_course_ext_dl_circuitbox
  URL 'abfss://circuitbox@deacourseextdl.dfs.core.windows.net/'
  WITH (STORAGE CREDENTIAL dea_course_ext_sc)
  COMMENT 'External Location for the circuitbox data lakehouse';

-- COMMAND ----------

-- MAGIC %fs ls 'abfss://circuitbox@deacourseextdl.dfs.core.windows.net/'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2. Create catalog 
-- MAGIC **Catalog Name:** circuitbox  
-- MAGIC _Managed Location:_ [abfss://circuitbox@deacourseextdl.dfs.core.windows.net/](abfss://circuitbox@prepdeacourseextdl.dfs.core.windows.net/)

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS circuitbox
  MANAGED LOCATION 'abfss://circuitbox@deacourseextdl.dfs.core.windows.net/'
  COMMENT 'Catalog for the circuitbox data lakehouse';

-- COMMAND ----------

SHOW CATALOGS;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3. Create Schemas
-- MAGIC 1. **Schema Name:** landing  
-- MAGIC    _Managed Location:_ [abfss://circuitbox@deacourseextdl.dfs.core.windows.net/landing](abfss://circuitbox@deacourseextdl.dfs.core.windows.net/landing)
-- MAGIC 1. **Schema Name:** lakehouse  
-- MAGIC    _Managed Location:_ [abfss://circuitbox@deacourseextdl.dfs.core.windows.net/lakehouse](abfss://circuitbox@deacourseextdl.dfs.core.windows.net/)

-- COMMAND ----------

USE CATALOG circuitbox;

CREATE SCHEMA IF NOT EXISTS landing
   MANAGED LOCATION 'abfss://circuitbox@deacourseextdl.dfs.core.windows.net/landing';

CREATE SCHEMA IF NOT EXISTS lakehouse
   MANAGED LOCATION 'abfss://circuitbox@deacourseextdl.dfs.core.windows.net/lakehouse';

-- COMMAND ----------

SHOW SCHEMAS;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 4. Create Volume
-- MAGIC **Volume Name:** operational_data  
-- MAGIC _ADLS Path:_ [abfss://circuitbox@deacourseextdl.dfs.core.windows.net/landing/operational_data/](abfss://circuitbox@prepdeacourseextdl.dfs.core.windows.net/landing/operational_data/)

-- COMMAND ----------

USE CATALOG circuitbox;
USE SCHEMA landing;

CREATE EXTERNAL VOLUME IF NOT EXISTS operational_data
      LOCATION 'abfss://circuitbox@deacourseextdl.dfs.core.windows.net/landing/operational_data';

-- COMMAND ----------

-- MAGIC %fs ls /Volumes/circuitbox/landing/operational_data
