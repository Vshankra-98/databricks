# Databricks notebook source
# MAGIC %run ../Utils/common_functions

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank,desc

# COMMAND ----------

display(dbutils.fs.ls("/mnt/silver"))

# COMMAND ----------

# MAGIC %md
# MAGIC # 1) Get a dirver name who has maximum point per year
# MAGIC

# COMMAND ----------

race_df = spark.read.parquet('dbfs:/mnt/silver/race/').select('race_id', 'date')
reulst_df = spark.read.parquet('dbfs:/mnt/silver/results/').select('race_id', 'driver_id', 'points')
driver_df = spark.read.parquet("dbfs:/mnt/silver/drivers/").select('driver_id', concat(col('forename'), lit('  '), col('surname')).alias('Driver'))

joined_df = driver_df.join(reulst_df, 'driver_id', 'inner').join(race_df, 'race_id', 'inner')



final_ouput = joined_df.withColumn('rank', dense_rank().over(Window.partitionBy(col('date')).orderBy(desc(col('points'))))).filter(col('rank') == 1).select("Driver", "date", "points")

display(final_ouput)

# COMMAND ----------

# MAGIC %md
# MAGIC #  2) Get a team name who has scored max point in a year 
# MAGIC

# COMMAND ----------

race_df = spark.read.parquet('dbfs:/mnt/silver/race/').select('race_id', 'date')
reulst_df = spark.read.parquet('dbfs:/mnt/silver/results/').select('race_id', 'constructor_id', 'points')
constructor_df = spark.read.parquet('dbfs:/mnt/silver/constructors/').select('constructor_id', col('name').alias('Team'))

join_df2 = race_df.join(reulst_df, 'race_id', 'inner').join(constructor_df, 'constructor_id',  'inner')
display(join_df2)

final_ouput2 = join_df2.withColumn('rank', dense_rank().over(Window.partitionBy("date").orderBy(desc(col("points"))))).filter(col('rank') ==  1).select("Team", "date", "points")

display(final_ouput2)

# COMMAND ----------

# MAGIC %md
# MAGIC # 3) Get a driver name and his team name who qualifying 	one (q1) in minimum time.

# COMMAND ----------

from pyspark.sql.functions import regexp_replace

qualifying = spark.read.parquet('dbfs:/mnt/silver/qualifying/').withColumn('q1', regexp_replace('q1', '\\\\N', '')).select('driver_id', 'constructor_id', col('q1').alias('Qualifying_1'))

driver_df = spark.read.parquet("dbfs:/mnt/silver/drivers/").select('driver_id', concat(col('forename'), lit('  '), col('surname')).alias('Driver'))

constructor_df = spark.read.parquet('dbfs:/mnt/silver/constructors/').select('constructor_id', col('name').alias('Team'))




# COMMAND ----------


from pyspark.sql.functions import split 

qualifying1 = qualifying.withColumn('minute', (split(col('Qualifying_1'), ":")[0].cast('int'))).withColumn('Seconds', split(split(col('Qualifying_1'), ':')[1], "\.")[0].cast("int")).withColumn('milisecon', split(split(col('Qualifying_1'), ':')[1], '\.')[1].cast("int"))

qalifying2 = qualifying1.withColumn("q1_time(second)", (col('minute')*60) + col("Seconds") + (col("milisecon")/1000)).select('q1_time(second)', "driver_id", "constructor_id")


qalifying2.display()

# COMMAND ----------

from pyspark.sql.functions import asc_nulls_last


join_df4 = driver_df.join(qalifying2, "driver_id", "inner").join(constructor_df, "constructor_id", "inner").select('Team', 'Driver', "q1_time(second)").orderBy(asc_nulls_last(col('q1_time(second)')))


print(join_df4.first())




# COMMAND ----------


