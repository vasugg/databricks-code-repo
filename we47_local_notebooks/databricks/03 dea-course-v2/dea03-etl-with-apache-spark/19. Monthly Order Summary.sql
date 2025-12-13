-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Monthly Order Summary
-- MAGIC For each of the customer, produce the following summary per month
-- MAGIC 1. total orders
-- MAGIC 1. total items bought
-- MAGIC 1. total amount spent

-- COMMAND ----------

SELECT * FROM gizmobox.silver.orders WHERE customer_id = 5816 ;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS gizmobox.gold.order_summary_monthly
AS
SELECT date_format(transaction_timestamp, 'yyyy-MM') AS order_month,
       customer_id, 
       COUNT(DISTINCT order_id) AS total_orders,
       SUM(quantity) AS total_items_bought,
       SUM(price * quantity) AS total_amount
 FROM gizmobox.silver.orders
 GROUP BY order_month, customer_id ;

-- COMMAND ----------

SELECT * FROM gizmobox.gold.order_summary_monthly;
