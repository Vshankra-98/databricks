# Databricks notebook source
# MAGIC %run ../Utils/common_functions

# COMMAND ----------

# define schema

input_schema = StructType([
    StructField('raceId', IntegerType()),
    StructField('driverId', IntegerType()),
    StructField('lap', IntegerType()),
    StructField('position', IntegerType()),
    StructField('time', StringType()),
    StructField('milliseconds', IntegerType()),
])

# COMMAND ----------

dbutils.fs.ls('/mnt/bronze/')

# COMMAND ----------

df = spark.read.csv('dbfs:/mnt/bronze/lap_times/', schema=input_schema)
df.display()
df.count()
df.printSchema()

# COMMAND ----------


current_dt = datetime.today().strftime('%Y-%m-%d')

df = df.withColumnRenamed('raceId', 'race_Id').withColumnRenamed('driverId','driver_Id').withColumn('ingest_dt', lit(current_dt))
df.display()

# COMMAND ----------

df.write.parquet('/mnt/silver/lap_times', mode = 'overwrite')

# COMMAND ----------


