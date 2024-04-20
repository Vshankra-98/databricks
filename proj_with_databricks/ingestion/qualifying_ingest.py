# Databricks notebook source
# MAGIC %run ../Utils/common_functions

# COMMAND ----------

# define schema

input_schema = StructType([
    StructField('qualifyId', IntegerType()),
    StructField('raceId', IntegerType()),
    StructField('driverId', IntegerType()),
    StructField('constructorId', IntegerType()),
    StructField('position', IntegerType()),
    StructField('q1', StringType()),
    StructField('q2', StringType()),
    StructField('q3', StringType())
])

# COMMAND ----------

dbutils.fs.ls('/mnt/bronze/')

# COMMAND ----------

df = spark.read.json('dbfs:/mnt/bronze/qualifying/', multiLine=True, schema = input_schema)
df.count()
display(df)
df.printSchema()

# COMMAND ----------

current_dt = datetime.today().strftime('%Y-%m-%d')

df = df.withColumnRenamed('qualifyId',  'qualify_Id').withColumnRenamed('raceId',  'race_Id').withColumnRenamed('driverId',  'driver_Id').withColumnRenamed('constructorId',  'constructor_Id').withColumn('ingest_dt', lit(current_dt))

display(df)

# COMMAND ----------

df.write.parquet('/mnt/silver/qualifying', mode = 'overwrite')

# COMMAND ----------


