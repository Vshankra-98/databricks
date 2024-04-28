# Databricks notebook source
# ##--> races result

# 2024 Australian Grand Prix

# ##Melbourne Grand Prix Circuit, 24 March 2024
# nationality, driver, number, team, grid, pits, fastest lap, race time, points

# result -->grid, race_id, driver_id, consructor_id,points,fastest_lap_time,
# pitspots --> spot(pits), race_id, driver_id, time

# race --> year, race_name, race_id, time(race_time)
# driver --> driver_name, nationality, driver_id,number,

# custructor --> constructor_id, team

# circuit --> circuit_id, circuit_name, country

# COMMAND ----------

display(dbutils.fs.ls('/mnt/dataricks-formula-one/silver'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimization Techniques -->Columanar pronuning-->
# MAGIC  reduce amount of data before join opration to reduce the shuffling overhead which optimizes the performance using selecting important columns
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col,count, sum




# COMMAND ----------

from pyspark.sql.functions import col,concat, lit, desc



circuit_df = spark.read.parquet('dbfs:/mnt/dataricks-formula-one/silver/circuts/').select(["country", "location",  "circuit_ref", "circuit_id"])

driver_df = spark.read.parquet("dbfs:/mnt/dataricks-formula-one/silver/drivers/").withColumn('Driver',concat(col('Forename'),lit('  '), col('surname'))).select(['driver_Id', 'driver_Ref', col('number'), 'nationality', 'Driver'])

constructor_df = spark.read.parquet('dbfs:/mnt/dataricks-formula-one/silver/constructors/').select('constructor_Id', col('name').alias('Team'))

race_df = spark.read.parquet('dbfs:/mnt/dataricks-formula-one/silver/race/').select(['race_id', 'year', 'round', 'circuit_Id', 'name', 'date'])

result_df = spark.read.parquet('dbfs:/mnt/dataricks-formula-one/silver/results/').select(['result_Id', 'race_Id', 'driver_Id', 'grid', 'points', col('time').alias('Race Time'), col('fastestLapTime').alias('Fastest Lap'), 'constructor_Id', "position"])

pitsots_df = spark.read.parquet('dbfs:/mnt/dataricks-formula-one/silver/pit_stops/').select(['driverId', 'duration', 'raceId', col('stop').alias('Pits')])


# COMMAND ----------

# MAGIC %md
# MAGIC ## Combining different datasets -->joining dataframes

# COMMAND ----------


# drivers columns --> ['driver_Id', 'driver_Ref', 'number', 'nationality', 'Forename', 'surname']
# result_columns --> ['result_Id', 'race_Id', 'driver_Id', 'number', 'grid', 'points', 'time', 'fastestLapTime']
# race columns --> ['race_id', 'year', 'round', 'circuit_Id', 'name', 'date']

# circuit columns --> [country, location, name, circuit_ref, circuit_id]
# pitspots_columns --> ['driverId', 'duration', 'raceId', 'stop', 'time']

# COMMAND ----------

final_df = driver_df.join(result_df, 'driver_Id', 'left').join(race_df,'race_id', 'left').join(circuit_df,'circuit_Id', 'left').join(pitsots_df, driver_df.driver_Id == pitsots_df.driverId, 'left').join(constructor_df,'constructor_Id', 'left')


final_df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Austrian_Grand_Prix_2015_06_2015

# COMMAND ----------

from pyspark.sql.functions import row_number
from pyspark.sql.window import Window


columns =["nationality", "Driver", "number", "Team", "grid", "Pits", "Fastest Lap", 'Race Time', "points"]

Austrian_Grand_Prix_2015 = final_df.filter(col('date') == '2015-06-21').select(columns).filter((col('name') == 'Austrian Grand Prix') & (col('year') == '2015') ).withColumn('points', col('points').cast('int')).distinct()


display(Austrian_Grand_Prix_2015)

Austrian_Grand_Prix_2015.write.parquet('/mnt/dataricks-formula-one/gold/race_result/Austrian_Grand_Prix_2015_06_2015',  mode='overwrite')





# COMMAND ----------


