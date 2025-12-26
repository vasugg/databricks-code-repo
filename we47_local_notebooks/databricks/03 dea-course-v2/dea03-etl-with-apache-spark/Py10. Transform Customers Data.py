# Databricks notebook source
# MAGIC %md
# MAGIC ## Transform Customer Data
# MAGIC 1. Remove records with NULL customer_id 
# MAGIC 1. Remove exact duplicate records
# MAGIC 1. Remove duplicate records based on created_timestamp
# MAGIC 1. CAST the columns to the correct Data Type
# MAGIC 1. Write transformed data to the Silver schema

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Remove records with NULL customer_id

# COMMAND ----------

df = spark.sql('SELECT * FROM gizmobox.bronze.py_customers')
display(df)

# COMMAND ----------

df = spark.table('gizmobox.bronze.py_customers')
display(df)

# COMMAND ----------

df = spark.table('gizmobox.bronze.py_customers')
df_filtered = df.filter('customer_id is not null')
display(df_filtered)


# COMMAND ----------

df_filtered = df.filter(df.customer_id.isNotNull())
display(df_filtered)

# COMMAND ----------

df_filtered = df.where(df.customer_id.isNotNull())
display(df_filtered)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Remove exact duplicate records

# COMMAND ----------

df_distinct = df_filtered.distinct()
display(df_distinct)

# COMMAND ----------

#dropDuplicates is more versatile than distinct() as it can be used to pick distinct values from specific columns as well. When no argument is passed to dropDuplicates(), it performs the same task as distinct(), checking the entire row. However, you can also specify a subset of columns to consider when looking for duplicates.

df_distinct = df_filtered.dropDuplicates()

#dropDuplicates(subset)
#df_distinct = df_filtered.dropDuplicates(['customer_id'])
display(df_distinct)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Remove duplicate records based on created_timestamp

# COMMAND ----------

from pyspark.sql import functions as F

df_max_ts = df_distinct.groupBy('customer_id')\
                       .agg(F.max('created_timestamp').alias('max_created_timestamp'))
display(df_max_ts)


# COMMAND ----------

df_distinct_customer = (df_distinct.join(df_max_ts, 
                        (df_distinct.customer_id == df_max_ts.customer_id) & 
                        (df_distinct.created_timestamp == df_max_ts.max_created_timestamp),
                        'inner')
                        .select(df_distinct['*']))

display(df_distinct_customer)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. CAST the column values to the correct data type

# COMMAND ----------

df_casted_customer = (
                        df_distinct_customer
                        .select(df_distinct_customer.created_timestamp.cast('timestamp'),
                                df_distinct_customer.customer_id,
                                df_distinct_customer.customer_name,
                                df_distinct_customer.date_of_birth.cast('date'),
                                df_distinct_customer.email,
                                df_distinct_customer.member_since.cast('date'),
                                df_distinct_customer.telephone)
)
display(df_casted_customer)


# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Write Data to a Delta Table

# COMMAND ----------

df_casted_customer.writeTo('gizmobox.silver.py_customers').createOrReplace()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gizmobox.silver.py_customers;
