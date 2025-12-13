-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Transform the companies data and insert into silver layer
-- MAGIC 1. Create the silver schema if doesn't already exists
-- MAGIC 1. Create the silver companies table with the data bronze layer and generate the columns company_id and founded_year. 

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS demo.silver
     MANAGED LOCATION 'abfss://demo@deacourseextdl.dfs.core.windows.net/silver';  

-- COMMAND ----------

DROP TABLE IF EXISTS demo.silver.companies;

CREATE TABLE demo.silver.companies
  (company_id   BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
   company_name STRING,
   founded_date DATE,
   founded_year INT GENERATED ALWAYS AS (YEAR(founded_date)),
   country      STRING);

INSERT INTO demo.silver.companies 
(company_name, founded_date, country)
SELECT company_name,
       founded_date,
       country
  FROM demo.bronze.companies;    
