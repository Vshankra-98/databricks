# Databricks notebook source
# MAGIC %run ./race_result/

# COMMAND ----------

display(dbutils.fs.ls('/mnt/dataricks-formula-one/silver'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimization Techniques -->Columanar pronuning-->
# MAGIC  reduce amount of data before join opration to reduce the shuffling overhead which optimizes the performance using selecting important columns

# COMMAND ----------

from pyspark.sql.functions import col,row_number,desc, sum
from pyspark.sql.window import Window

drivers_standing = final_df.select('Driver', 'Team', 'position' ,'points', 'year').filter(col('position') == 1).withColumn('wins', sum(col('position')).over(Window.partitionBy(col('Driver'),col('year')).orderBy(desc(col("points"))))).select(['Driver', 'Team', 'wins' , 'points' ])

# drivers_standing = drivers_standing.withColumn('wins', sum(col('rank')).over(Window.partitionBy(col('year')))).select('Driver', 'Team', 'wins', 'points', 'year').distinct()

print(drivers_standing.columns)
drivers_standing.display()

# COMMAND ----------

drivers_standing.write.mode('overwrite').parquet('dbfs:/mnt/dataricks-formula-one/gold/drivers_standing/')

# COMMAND ----------


