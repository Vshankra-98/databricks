# Databricks notebook source
# MAGIC %run ../Utils/common_functions

# COMMAND ----------

# define schema 

input_schema = StructType([
    StructField('resultId', IntegerType()),
    StructField('raceId', IntegerType()),
    StructField('driverId', IntegerType()),
    StructField('number', IntegerType()),
    StructField('grid', IntegerType()),
    StructField('position', IntegerType()),
    StructField('positionText', StringType()),
    StructField('positionOrder', IntegerType()),
    StructField('points', FloatType()),
    StructField('laps', IntegerType()),
    StructField('time', StringType()),
    StructField('milliseconds', IntegerType()),
    StructField('fastestLap', IntegerType()),
    StructField('rank', IntegerType()),
    StructField('fastestLapTime', StringType()),
    StructField('fastestLapSpeed', StringType()),
    StructField('statusId', IntegerType()),

])

# COMMAND ----------

dbutils.fs.ls('/mnt/bronze/')

# COMMAND ----------

df = spark.read.json('dbfs:/mnt/bronze/results.json', schema=input_schema)

df.printSchema()

# COMMAND ----------

df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Few Trasformation
# MAGIC

# COMMAND ----------

current_dt = datetime.today().strftime('%Y-%m-%d')

df = df.withColumnRenamed('resultId', 'result_Id').withColumnRenamed('raceId', 'race_Id').withColumnRenamed('driverId', 'driver_Id').withColumnRenamed('statusId', 'status_Id').withColumn('ingest_dt', lit(current_dt))

display(df)


# COMMAND ----------

df.write.parquet('/mnt/silver/results', mode ='overwrite')

# COMMAND ----------


