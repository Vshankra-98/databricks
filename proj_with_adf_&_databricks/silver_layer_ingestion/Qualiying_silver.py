# Databricks notebook source
display(dbutils.fs.ls('/mnt/formula-one/bronze/'))

# COMMAND ----------

df = spark.read.json('/mnt/formula-one/bronze/qualifying/', multiLine = True)
display(df)
df.write.mode('overwrite').format('delta').save('/mnt/formula-one/silver/qualifying')

# COMMAND ----------


