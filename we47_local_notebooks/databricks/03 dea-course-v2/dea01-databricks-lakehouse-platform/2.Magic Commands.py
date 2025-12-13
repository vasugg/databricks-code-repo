# Databricks notebook source
# MAGIC %md
# MAGIC ## Databricks Magic Commands
# MAGIC - **%python, %scala, %sql, %r** : Switch to a different language for a specific cell
# MAGIC - **%md**  : Markdown for documenting notebooks
# MAGIC - **%fs**  : Run file system commands
# MAGIC - **%sh**  : Run shell commands (Driver Node only)
# MAGIC - **%pip** : Install Python libraries
# MAGIC - **%run** : Include/ Import another notebook into the current notebook
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### %fs  : Run file system commands

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/

# COMMAND ----------

# MAGIC %md
# MAGIC ### %sh  : Run shell commands (Driver Node only)

# COMMAND ----------

# MAGIC %sh ps

# COMMAND ----------

# MAGIC %md
# MAGIC ### %pip : Install Python libraries

# COMMAND ----------

# MAGIC %pip list

# COMMAND ----------

# MAGIC %pip install faker

# COMMAND ----------

# MAGIC %md
# MAGIC ### %run : Include/ Import another notebook into the current notebook

# COMMAND ----------

# MAGIC %run "./2.1 Environment Variables and Functions"

# COMMAND ----------

env

# COMMAND ----------

print_env_info()

# COMMAND ----------


