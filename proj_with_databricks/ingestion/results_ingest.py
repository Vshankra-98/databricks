# Databricks notebook source
# MAGIC %run ../Utils/common_functions

# COMMAND ----------

df =(spark.read.json('dbfs:/mnt/dataricks-formula-one/bronze/results.json'))

print(len(df.columns))

# COMMAND ----------

# define schema 

input_schema = StructType([
    StructField('resultId', IntegerType()),
    StructField('raceId', IntegerType()),
    StructField('driverId', IntegerType()),
    StructField('constructorId', IntegerType()),
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

display(dbutils.fs.ls('/mnt/dataricks-formula-one/bronze/'))

# COMMAND ----------

df = spark.read.json('dbfs:/mnt/dataricks-formula-one/bronze/results.json', schema=input_schema)

df.printSchema()

# COMMAND ----------

df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Few Trasformation
# MAGIC

# COMMAND ----------

current_dt = datetime.today().strftime('%Y-%m-%d')

df = df.withColumnRenamed('resultId', 'result_Id').withColumnRenamed('raceId', 'race_Id').withColumnRenamed('driverId', 'driver_Id').withColumnRenamed('statusId', 'status_Id').withColumn('ingest_dt', lit(current_dt)).withColumnRenamed('constructorId', 'constructor_Id')

display(df)


# COMMAND ----------

from pyspark.sql.functions import regexp_replace,when,col


display(df.withColumn('milliseconds', when(col('milliseconds').isNull(), '').otherwise(col('milliseconds'))))


# COMMAND ----------

from pyspark.sql.functions import regexp_replace,isnull,when

columns = df.columns
df1 = df
for  i in columns:
    df1 = df1.withColumn(i, regexp_replace(i,'\\\\N','')).withColumn(i, when(col(i).isNull(), '').otherwise(col(i)))
    
display(df1)
df1.write.parquet('/mnt/dataricks-formula-one/silver/results', mode ='overwrite')

# COMMAND ----------

df1.write.parquet('/mnt/dataricks-formula-one/silver/results', mode ='overwrite')

# COMMAND ----------


