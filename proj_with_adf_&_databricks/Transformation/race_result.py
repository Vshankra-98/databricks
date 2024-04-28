# Databricks notebook source
# ##--> races result

# 2024 Australian Grand Prix

# ##Melbourne Grand Prix Circuit, 24 March 2024
# nationality, driver, number, team, grid, pits, fastest lap, race time, points

# result -->grid, race_id, driver_id, consructor_id,points,fastest_lap_time,
# pitspots --> spot(pits), race_id, driver_id, time

# driver --> driver_name, nationality, driver_id,number,

# custructor --> constructor_id, team

# circuit --> circuit_id, circuit_name, country

# COMMAND ----------



# COMMAND ----------

display(dbutils.fs.ls('/mnt/formula-one/silver'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimization Techniques -->Columanar pronuning-->
# MAGIC  reduce amount of data before join opration to reduce the shuffling overhead which optimizes the performance using selecting important columns
# MAGIC

# COMMAND ----------

## race --> [Driver, Number, Team, Grid,Pits, Fastest Lap, Race Time, Points]


# COMMAND ----------

from pyspark.sql.functions import col,count, sum, concat, lit, lower

result_df = spark.read.format('delta').load('dbfs:/mnt/formula-one/silver/results/')
# print(result_df.columns)

result_df = result_df.select(  col('Results_Driver_nationality').alias("nationality"), 
  concat(col('Results_Driver_familyName'), lit('  '), col('Results_Driver_givenName')).alias("Driver"),  col('Results_number').alias("Number"),   col('Results_Constructor_name').alias('Team'),   col('Results_grid').alias("grid"),  col('Results_FastestLap_Time_time').alias('Fastest Lap'), col('Results_Time_time').alias("Race_Time") ,   col('Results_points').alias("Points"),
   'date', 'raceName',  col('Circuit_circuitName'),   'Results_FastestLap_rank', 'Results_Time_millis', col('Results_Driver_givenName'))


pit_spots_df = spark.read.format('delta').load('dbfs:/mnt/formula-one/silver/pitstops/')


# driver_df = spark.read.format('delta').load('dbfs:/mnt/formula-one/silver/drivers/')

# display(driver_df)

final_race_result = result_df.join(pit_spots_df, lower(result_df["Results_Driver_givenName"]) == lower(pit_spots_df.driver_Id), 'left')

display(final_race_result)
 


# COMMAND ----------

final_race_result.write.mode('overwrite').format('parquet').save('dbfs:/mnt/formula-one/gold/final_race_result')

# COMMAND ----------


