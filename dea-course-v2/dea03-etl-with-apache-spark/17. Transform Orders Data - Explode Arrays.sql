-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Transform Orders Data - Explode Arrays
-- MAGIC 1. Access elements from the JSON object
-- MAGIC 1. Deduplicate Array Elements
-- MAGIC 1. Explode Arrays
-- MAGIC 1. Write the Transformed Data to Silver Schema

-- COMMAND ----------

 SELECT *  
   FROM gizmobox.silver.orders_json;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1. Access elements from the JSON object
-- MAGIC `<column_name.object>`

-- COMMAND ----------

 SELECT json_value.order_id,
        json_value.order_status,
        json_value.payment_method,
        json_value.total_amount,
        json_value.transaction_timestamp,
        json_value.customer_id,
        json_value.items
FROM gizmobox.silver.orders_json;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2. Deduplicate Array Elements
-- MAGIC Function [array_distinct](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/functions/array_distinct)

-- COMMAND ----------

SELECT json_value.order_id,
        json_value.order_status,
        json_value.payment_method,
        json_value.total_amount,
        json_value.transaction_timestamp,
        json_value.customer_id,
        array_distinct(json_value.items) AS items
FROM gizmobox.silver.orders_json;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### 3. Explode Arrays
-- MAGIC Function [explode](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/functions/explode)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW tv_orders_exploded
AS
SELECT json_value.order_id,
        json_value.order_status,
        json_value.payment_method,
        json_value.total_amount,
        json_value.transaction_timestamp,
        json_value.customer_id,
        explode(array_distinct(json_value.items)) AS item
FROM gizmobox.silver.orders_json;

-- COMMAND ----------

SELECT order_id,
       order_status,
       payment_method,
       total_amount,
       transaction_timestamp,
       customer_id,
       item.item_id,
       item.name,
       item.price,
       item.quantity,
       item.category,
       item.details.brand,
       item.details.color
  FROM tv_orders_exploded;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 4. Write the Transformed Data to Silver Schema

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS gizmobox.silver.orders
AS
SELECT order_id,
       order_status,
       payment_method,
       total_amount,
       transaction_timestamp,
       customer_id,
       item.item_id,
       item.name,
       item.price,
       item.quantity,
       item.category,
       item.details.brand,
       item.details.color
  FROM tv_orders_exploded;

-- COMMAND ----------

SELECT * FROM gizmobox.silver.orders;
