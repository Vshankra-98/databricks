# Databricks notebook source
# MAGIC %run ../Utils/common_functions

# COMMAND ----------

dbutils.fs.ls('/mnt/silver')

# COMMAND ----------

race_df = spark.read.parquet('dbfs:/mnt/silver/race/')
construnctor_df = spark.read.parquet('/mnt/silver/constructors')
driver_df = spark.read.parquet('dbfs:/mnt/silver/drivers/')
result_df = spark.read.parquet('dbfs:/mnt/silver/results/')
pitspot_df = spark.read.parquet('dbfs:/mnt/silver/pitsopts/')


# COMMAND ----------

driver_df1 = driver_df.withColumn('Driver' , concat(col('forename'), lit('  '), col('surname'))).select('nationality','driver_Id', 'Driver', 'number')

construnctor_df1 = construnctor_df.select('constructor_Id', col('constructor_Ref').alias('Team'))

result_df1 = result_df.select('race_Id', 'constructor_id','driver_Id', 'grid', 'position', 'points', col('fastestLap').alias('Fastest Lap'))

pitspot_df1 = pitspot_df.select('driver_id', col('stop').alias('Pits'), col('time').alias('Race Time'))



# COMMAND ----------

from pyspark.sql.functions import desc

join_df = result_df1.join(construnctor_df1, 'constructor_Id', 'left').join(driver_df1, 'driver_id', 'left').join(pitspot_df1, 'driver_id', 'left')

output = join_df.select('nationality','Driver', 'Number', 'Team', 'Grid', 'Pits', 'Fastest Lap', 'Race Time', 'Points')
output.orderBy(desc(col('Points'))).display()

# COMMAND ----------

output.write.parquet('/mnt/gold/race_result', mode='overwrite')

# COMMAND ----------

 
