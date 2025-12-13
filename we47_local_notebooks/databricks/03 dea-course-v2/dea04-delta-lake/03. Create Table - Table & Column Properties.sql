-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Create Table - Table & Column Properties
-- MAGIC Demonstrate adding Table and Column Properties to the CREATE TABLE statement. 
-- MAGIC
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1. Table Properties
-- MAGIC       1.1. COMMENT - allows you to document the purpose of the table. 
-- MAGIC       1.2. TBLPROPERTIES - used to specify table level metadata or configuration settings
-- MAGIC

-- COMMAND ----------

DROP TABLE IF EXISTS demo.delta_lake.companies;
CREATE TABLE demo.delta_lake.companies
  (company_name STRING,
   founded_date DATE,
   country      STRING)
COMMENT 'This table contains information about some of the successful tech companies'   
TBLPROPERTIES ('sensitive' = 'true', 'delta.enableDeletionVectors' = 'false');

-- COMMAND ----------

DESC EXTENDED demo.delta_lake.companies;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2. Column Properties
-- MAGIC       2.1 NOT NULL Constraints - enforces data integrity and quality by ensuring that a specific column cannot contain NULL values
-- MAGIC       2.2 COMMENT - documents the purpose or context of individual columns in a table

-- COMMAND ----------

DROP TABLE IF EXISTS demo.delta_lake.companies;
CREATE TABLE demo.delta_lake.companies
  (company_name STRING NOT NULL,
   founded_date DATE COMMENT 'The date the company was founded',
   country      STRING)
COMMENT 'This table contains information about some of the successful tech companies'   
TBLPROPERTIES ('sensitive' = 'true', 'delta.enableDeletionVectors' = 'false');

-- COMMAND ----------

DESC EXTENDED demo.delta_lake.companies;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2. Column Properties
-- MAGIC       2.3 Generated Columns - derived or computed columns, whose values are computed at the time of inserting a new records
-- MAGIC           2.3.1. Generated Identity Columns - used to generate an identity for example a primary key value
-- MAGIC           2.3.2. Generated Computed Columns - automatically calculate and store derived values based on other columns in the same table.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 2.3.1. Generated Identity Columns
-- MAGIC `GENERATED { ALWAYS | BY DEFAULT } AS IDENTITY [ ( [ START WITH start ] [ INCREMENT BY step ] ) ]`

-- COMMAND ----------

DROP TABLE IF EXISTS demo.delta_lake.companies;
CREATE TABLE demo.delta_lake.companies
  (company_id BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
   company_name STRING NOT NULL,
   founded_date DATE COMMENT 'The date the company was founded',
   country      STRING)
COMMENT 'This table contains information about some of the successful tech companies'   
TBLPROPERTIES ('sensitive' = 'true', 'delta.enableDeletionVectors' = 'false');

-- COMMAND ----------

INSERT INTO demo.delta_lake.companies 
(company_name, founded_date, country)
VALUES ("Apple", "1976-04-01", "USA"),
       ("Microsoft", "1975-04-04", "USA"),
       ("Google", "1998-09-04", "USA"),
       ("Amazon", "1994-07-05", "USA");

-- COMMAND ----------

SELECT * FROM demo.delta_lake.companies ;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 2.3.2. Generated Computed Columns
-- MAGIC GENERATED ALWAYS AS ( `expr` )
-- MAGIC
-- MAGIC `expr` may be composed of literals, column identifiers within the table, and deterministic, built-in SQL functions or operators except:
-- MAGIC - Aggregate functions
-- MAGIC - Analytic window functions
-- MAGIC - Ranking window functions
-- MAGIC - Table valued generator functions
-- MAGIC
-- MAGIC Also `expr` must not contain any subquery.

-- COMMAND ----------

DROP TABLE IF EXISTS demo.delta_lake.companies;
CREATE TABLE demo.delta_lake.companies
  (company_id BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
   company_name STRING NOT NULL,
   founded_date DATE COMMENT 'The date the company was founded',
   founded_year INT GENERATED ALWAYS AS (YEAR(founded_date)),
   country      STRING)
COMMENT 'This table contains information about some of the successful tech companies'   
TBLPROPERTIES ('sensitive' = 'true', 'delta.enableDeletionVectors' = 'false');

-- COMMAND ----------

INSERT INTO demo.delta_lake.companies 
(company_name, founded_date, country)
VALUES ("Apple", "1976-04-01", "USA"),
       ("Microsoft", "1975-04-04", "USA"),
       ("Google", "1998-09-04", "USA"),
       ("Amazon", "1994-07-05", "USA");

-- COMMAND ----------

SELECT * FROM demo.delta_lake.companies;
