-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Extract Data From the Payments Files
-- MAGIC 1. List the files from Payment folder
-- MAGIC 1. Create External Table
-- MAGIC 1. Demonstrate the effect of Adding/ Updating / Deleting files. 
-- MAGIC 1. Demonstrate the effect of Dropping the Table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1. List the files from payment folder

-- COMMAND ----------

-- MAGIC %fs ls '/Volumes/gizmobox/landing/external_data/payments/'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2. Create External Table 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Cant able to create the table with the csv files stored in internal location, 
-- MAGIC With unity catalog we need to refer the external location csv file to create a file. 
-- MAGIC So used python code instead of SQL. 

-- COMMAND ----------

DROP TABLE IF EXISTS gizmobox.bronze.payments;
CREATE TABLE IF NOT EXISTS gizmobox.bronze.payments
 (payment_id INTEGER, order_id INTEGER, payment_timestamp TIMESTAMP, payment_status INTEGER, payment_method STRING)
USING CSV 
OPTIONS (
  path "/Volumes/gizmobox/landing/external_data/payments",
  header = "true",
  delimiter = ","
)  




-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Read the CSV
-- MAGIC df = spark.read.format("csv").options(
-- MAGIC     header="true",
-- MAGIC     delimiter=","
-- MAGIC ).load("dbfs:/Volumes/gizmobox/landing/external_data/payments")
-- MAGIC
-- MAGIC # Rename columns to valid Delta Lake column names
-- MAGIC new_columns = [
-- MAGIC     "payment_id",
-- MAGIC     "order_id",
-- MAGIC     "payment_timestamp",
-- MAGIC     "payment_status",
-- MAGIC     "payment_method"
-- MAGIC ]
-- MAGIC df = df.toDF(*new_columns)
-- MAGIC
-- MAGIC # Save as table
-- MAGIC df.write.saveAsTable("gizmobox.bronze.payments")

-- COMMAND ----------

SELECT * FROM gizmobox.bronze.payments;

-- COMMAND ----------

DESCRIBE EXTENDED gizmobox.bronze.payments;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3. Demonstrate the effect of adding/ updating / deleting files. 

-- COMMAND ----------

SELECT * FROM gizmobox.bronze.payments;

-- COMMAND ----------

REFRESH TABLE gizmobox.bronze.payments;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 4. Demonstrate the effect of Dropping the Table

-- COMMAND ----------

DROP TABLE IF EXISTS gizmobox.bronze.payments;
