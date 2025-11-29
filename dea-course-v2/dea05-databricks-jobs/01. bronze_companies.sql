-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Ingest companies data into bronze layer
-- MAGIC 1. Create the bronze schema if doesn't already exists
-- MAGIC 1. Create the bronze companies table with the data from landing folder. 

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS demo.bronze
     MANAGED LOCATION 'abfss://demo@deacourseextdl.dfs.core.windows.net/bronze';  

-- COMMAND ----------

DROP TABLE IF EXISTS demo.bronze.companies;

CREATE TABLE demo.bronze.companies
AS
SELECT company_name, founded_date, country
  FROM read_files('abfss://demo@deacourseextdl.dfs.core.windows.net/landing/companies',
                  format => 'csv',
                  header => true);
