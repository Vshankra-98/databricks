# Databricks notebook source
dbutils.notebook.run("qualifying_result", 0, arguments={"formatted_date": "20240417"})

# COMMAND ----------

dbutils.notebook.run("Result_table", 0, arguments={"formatted_date": "20240417"})
