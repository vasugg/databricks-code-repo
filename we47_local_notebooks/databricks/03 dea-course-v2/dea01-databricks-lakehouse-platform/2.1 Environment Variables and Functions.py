# Databricks notebook source
env = 'dev'

# COMMAND ----------

import os
import platform

def print_env_info():
    # Print Python Version
    print(f"Python Version: {platform.python_version()}")
    # Print Databricks Runtime Version
    runtime_version = os.environ.get("DATABRICKS_RUNTIME_VERSION", "Unknown")
    print(f"Databricks Runtime Version: {runtime_version}")
