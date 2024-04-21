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

dbutils.fs.ls('/mnt/dataricks-formula-one/bronze/')

# COMMAND ----------

df = spark.read.csv('dbfs:/mnt/dataricks-formula-one/bronze/lap_times/', schema=input_schema)
df.display()
df.count()
df.printSchema()

# COMMAND ----------


current_dt = datetime.today().strftime('%Y-%m-%d')

df = df.withColumnRenamed('raceId', 'race_Id').withColumnRenamed('driverId','driver_Id').withColumn('ingest_dt', lit(current_dt))
df.display()

# COMMAND ----------

from pyspark.sql.functions import regexp_replace,isnull, when

columns = df.columns
df1 = df
for  i in columns:
    df1 = df1.withColumn(i, regexp_replace(i,'\\\\N','')).withColumn(i, when(col(i).isNull(), '').otherwise(col(i)))
    
display(df1)

df1.write.parquet('/mnt/dataricks-formula-one/silver/lap_times', mode = 'overwrite')

# COMMAND ----------


