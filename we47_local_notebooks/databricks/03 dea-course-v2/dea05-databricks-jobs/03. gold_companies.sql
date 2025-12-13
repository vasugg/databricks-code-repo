-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Create company summary in the gold layer
-- MAGIC 1. Create the gold schema if doesn't already exists
-- MAGIC 1. Create the gold company_summary with the number of companies per country. 

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS demo.gold
     MANAGED LOCATION 'abfss://demo@deacourseextdl.dfs.core.windows.net/gold';  

-- COMMAND ----------

DROP TABLE IF EXISTS demo.gold.company_summary;

CREATE TABLE demo.gold.company_summary
AS
SELECT country,
       COUNT(*) AS num_companies
  FROM demo.silver.companies
 GROUP BY country;    
