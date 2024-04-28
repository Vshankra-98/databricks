# Databricks notebook source
display(dbutils.fs.ls('mnt/formula-one/silver/'))

# COMMAND ----------

result_df = spark.read.format('delta').load('dbfs:/mnt/formula-one/silver/results/')


# COMMAND ----------

from pyspark.sql.functions import col, concat, lit, sum
from pyspark.sql.window import Window

result_df1 = result_df.select( col('Results_Constructor_name').alias('Team'),   col('Results_points').alias('Points'), col('Results_position').alias('position'), 'season')
result_df1 = result_df1.withColumn('wins',  sum(col('position')).over(Window.partitionBy(col('Team'), col('season')))).filter(col('season') == 2024).distinct()

constructor_standing_api = result_df1.select('Team', 'wins', 'Points')

display(constructor_standing_api)


# COMMAND ----------

constructor_standing_api.write.mode('overwrite').parquet('dbfs:/mnt/formula-one/gold/constructor_standing_api')

# COMMAND ----------


