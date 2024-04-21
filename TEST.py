# Databricks notebook source
display(dbutils.fs.ls('/mnt/dataricks-formula-one/silver/'))

# COMMAND ----------

df = (spark.read.parquet('dbfs:/mnt/dataricks-formula-one/silver/pit/'))

display(df)
df.count()

# COMMAND ----------


