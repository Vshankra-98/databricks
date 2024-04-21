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

drivers_standing = final_df.select('Driver', 'Team','grid', 'circuit_Id', 'points', 'year', 'round', 'name', 'date').withColumn('rank', row_number().over(Window.partitionBy(col('year'), col('circuit_Id')).orderBy(desc(col("points"))))).filter(col('rank') == 1)

drivers_standing = drivers_standing.withColumn('wins', sum(col('rank')).over(Window.partitionBy(col('year')))).select('Driver', 'Team', 'wins', 'points', 'year', 'name').distinct()

drivers_standing.display()

# COMMAND ----------

from pyspark.sql.functions import col,concat, lit, desc


driver_df = spark.read.parquet("dbfs:/mnt/dataricks-formula-one/silver/drivers/").withColumn('Driver',concat(col('Forename'),lit('  '), col('surname'))).select(['driver_Id', 'driver_Ref', col('number'), 'nationality', 'Driver'])

constructor_df = spark.read.parquet('dbfs:/mnt/dataricks-formula-one/silver/constructors/').select('constructor_Id', col('name').alias('Team'))

race_df = spark.read.parquet('dbfs:/mnt/dataricks-formula-one/silver/race/').select(['race_id', 'year', 'round', 'circuit_Id', 'name', 'date'])


circuit_df = spark.read.parquet('dbfs:/mnt/dataricks-formula-one/silver/circuts/').select(["country", "location",  "circuit_ref", "circuit_id"])



result_df = spark.read.parquet('dbfs:/mnt/dataricks-formula-one/silver/results/').select(['result_Id', 'race_Id', 'driver_Id', 'grid', 'points', col('time').alias('Race Time'), col('fastestLapTime').alias('Fastest Lap'), 'constructor_Id'])


# pitsots_df = spark.read.parquet('dbfs:/mnt/dataricks-formula-one/silver/pit_stops/').select(['driverId', 'duration', 'raceId', col('stop').alias('Pits')])
