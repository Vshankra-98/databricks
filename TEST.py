# Databricks notebook source
display(dbutils.fs.ls('/mnt/formula-one/bronze'))

# COMMAND ----------

display(spark.read.json('dbfs:/mnt/formula-one/bronze/pitstops/'))

# COMMAND ----------


