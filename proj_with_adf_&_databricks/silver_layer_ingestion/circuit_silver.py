# Databricks notebook source
display(dbutils.fs.ls('/mnt/formula-one/bronze'))

# COMMAND ----------

from pyspark.sql.functions import col

df = spark.read.csv('dbfs:/mnt/formula-one/bronze/circuits/',header=True, inferSchema=True)
df = df.select(col('circuitId').alias('circuit_Id'), col('circuitRef').alias('circuit_Ref'), col('name'), col('location'), col('country'))
display(df)

df.write.mode('overwrite').format('delta').save('dbfs:/mnt/formula-one/silver/circuits/')

# COMMAND ----------


