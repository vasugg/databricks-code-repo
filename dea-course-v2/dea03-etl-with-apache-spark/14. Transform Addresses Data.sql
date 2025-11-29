-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Transform Addresses Data
-- MAGIC 1. Create one record for each customer with 2 sets of address columns, 1 for shipping and 1 for billing address 
-- MAGIC 1. Write transformed data to the Silver schema  

-- COMMAND ----------

SELECT customer_id,
       address_type,
       address_line_1,
       city,
       state,
       postcode
  FROM gizmo_box.bronze.v_addresses;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1. Create one record for each customer with both addresses, one for each address_type
-- MAGIC > [Documentation for PIVOT clause](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-syntax-qry-select-pivot)

-- COMMAND ----------

SELECT *
 FROM (SELECT customer_id,
            address_type,
            address_line_1,
            city,
            state,
            postcode
        FROM gizmobox.bronze.v_addresses)
PIVOT (MAX(address_line_1) AS address_line_1,
       MAX(city) AS city,
       MAX(state) AS state,
       MAX(postcode) AS postcode
       FOR address_type IN ('shipping', 'billing')
       );

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2. Write transformed data to the Silver schema 

-- COMMAND ----------

CREATE TABLE gizmobox.silver.addresses
AS
SELECT *
 FROM (SELECT customer_id,
            address_type,
            address_line_1,
            city,
            state,
            postcode
        FROM gizmobox.bronze.v_addresses)
PIVOT (MAX(address_line_1) AS address_line_1,
       MAX(city) AS city,
       MAX(state) AS state,
       MAX(postcode) AS postcode
       FOR address_type IN ('shipping', 'billing')
       );

-- COMMAND ----------

SELECT * FROM gizmobox.silver.addresses;
