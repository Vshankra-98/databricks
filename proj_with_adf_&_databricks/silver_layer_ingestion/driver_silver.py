# Databricks notebook source
display(dbutils.fs.ls('/mnt/formula-one/bronze/'))

# COMMAND ----------

df = spark.read.json('dbfs:/mnt/formula-one/bronze/drivers/')
display(df)

# COMMAND ----------


