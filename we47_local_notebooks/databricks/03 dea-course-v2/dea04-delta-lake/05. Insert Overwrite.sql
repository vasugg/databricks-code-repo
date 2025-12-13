-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Insert Overwrite
-- MAGIC 1. Replace all the data in a table
-- MAGIC 1. Replace all the data from a specific partition
-- MAGIC 1. How to handle schema changes

-- COMMAND ----------

-- MAGIC %md
-- MAGIC INSERT OVERWITE - Overwrites the existing data in a table or a specific partition with the new data. 
-- MAGIC
-- MAGIC INSERT INTO - Appends new data
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1. Replace all the data in a table

-- COMMAND ----------

DROP TABLE IF EXISTS demo.delta_lake.gold_companies;

CREATE TABLE demo.delta_lake.gold_companies
  (company_name STRING,
   founded_date DATE,
   country      STRING);

INSERT INTO demo.delta_lake.gold_companies 
(company_name, founded_date, country)
VALUES ("Apple", "1976-04-01", "USA"),  
       ("Tencent", "1998-11-11", "China"); 

SELECT * FROM demo.delta_lake.gold_companies;        

-- COMMAND ----------

DROP TABLE IF EXISTS demo.delta_lake.bronze_companies;

CREATE TABLE demo.delta_lake.bronze_companies
  (company_name STRING,
   founded_date DATE,
   country      STRING);

INSERT INTO demo.delta_lake.bronze_companies 
(company_name, founded_date, country)
VALUES ("Apple", "1976-04-01", "USA"),
       ("Microsoft", "1975-04-04", "USA"),
       ("Google", "1998-09-04", "USA"),
       ("Amazon", "1994-07-05", "USA"),
       ("Tencent", "1998-11-11", "China");   

SELECT * FROM demo.delta_lake.bronze_companies;       

-- COMMAND ----------

INSERT OVERWRITE TABLE demo.delta_lake.gold_companies
SELECT *
  FROM demo.delta_lake.bronze_companies;

-- COMMAND ----------

SELECT * FROM demo.delta_lake.gold_companies;

-- COMMAND ----------

DESC HISTORY demo.delta_lake.gold_companies;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2. Replace all the data from a specific partition

-- COMMAND ----------

DROP TABLE IF EXISTS demo.delta_lake.gold_companies_partitioned;

CREATE TABLE demo.delta_lake.gold_companies_partitioned
  (company_name STRING,
   founded_date DATE,
   country      STRING)
PARTITIONED BY (country);

INSERT INTO demo.delta_lake.gold_companies_partitioned 
(company_name, founded_date, country)
VALUES ("Apple", "1976-04-01", "USA"),  
       ("Tencent", "1998-11-11", "China"); 

SELECT * FROM demo.delta_lake.gold_companies_partitioned;        

-- COMMAND ----------

DESC EXTENDED demo.delta_lake.gold_companies_partitioned

-- COMMAND ----------

DROP TABLE IF EXISTS demo.delta_lake.bronze_companies_usa;

CREATE TABLE demo.delta_lake.bronze_companies_usa
  (company_name STRING,
   founded_date DATE,
   country      STRING);

INSERT INTO demo.delta_lake.bronze_companies_usa 
(company_name, founded_date, country)
VALUES ("Apple", "1976-04-01", "USA"),
       ("Microsoft", "1975-04-04", "USA"),
       ("Google", "1998-09-04", "USA"),
       ("Amazon", "1994-07-05", "USA");   

SELECT * FROM demo.delta_lake.bronze_companies_usa;       

-- COMMAND ----------

INSERT OVERWRITE TABLE demo.delta_lake.gold_companies_partitioned
PARTITION (country = "USA")
SELECT company_name,
       founded_date
  FROM demo.delta_lake.bronze_companies_usa;     

-- COMMAND ----------

SELECT * FROM demo.delta_lake.gold_companies_partitioned;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3. How to handle schema changes

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Insert Overwrite -> Use to overwrite the data in a table or a partition when there are no schema changes.  
-- MAGIC Create or replace table -> Use when there are schema changes. 
-- MAGIC
