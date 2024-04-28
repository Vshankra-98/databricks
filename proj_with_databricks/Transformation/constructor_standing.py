# Databricks notebook source
# MAGIC %run ./race_result/

# COMMAND ----------

display(dbutils.fs.ls('/mnt/dataricks-formula-one/gold/'))

# COMMAND ----------

from pyspark.sql.functions import col,row_number,desc, sum
from pyspark.sql.window import Window

constructor_standing = final_df.select( 'Team', 'position' ,'points', 'year').filter(col('position') == 1).withColumn('wins', sum(col('position')).over(Window.partitionBy(col('Team'),col('year')).orderBy(desc(col("points"))))).select([ 'Team', 'wins' , 'points' ]).distinct()

print(constructor_standing.columns)
constructor_standing.display()

# COMMAND ----------

constructor_standing.write.mode('overwrite').parquet('dbfs:/mnt/dataricks-formula-one/gold/constructor_standing/')

# COMMAND ----------


