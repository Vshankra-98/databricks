# Databricks notebook source
# MAGIC %run ../Utils/common_functions

# COMMAND ----------

# define Schema
from pyspark.sql.types import TimestampType

input_scema = StructType([
            StructField('raceId', IntegerType()),
            StructField('driverId', IntegerType()),
            StructField('stop', IntegerType()),
            StructField('time', TimestampType()),
            StructField('duration', IntegerType()),
            StructField('milliseconds', IntegerType())
])

# COMMAND ----------

dbutils.fs.ls('/mnt/bronze/')

# COMMAND ----------

df = spark.read.json('dbfs:/mnt/bronze/pit_stops.json', multiLine=True, schema=input_scema)
display(df)
df.printSchema()

# COMMAND ----------

df.write.parquet('/mnt/silver/pitsopts', mode='overwrite')

# COMMAND ----------


