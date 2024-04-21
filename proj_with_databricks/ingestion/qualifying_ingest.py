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

dbutils.fs.ls('/mnt/dataricks-formula-one/bronze/')

# COMMAND ----------

df = spark.read.json('dbfs:/mnt/dataricks-formula-one/bronze/qualifying/', multiLine=True)
df.count()
display(df)
df.printSchema()

# COMMAND ----------

current_dt = datetime.today().strftime('%Y-%m-%d')

df = df.withColumnRenamed('qualifyId',  'qualify_Id').withColumnRenamed('raceId',  'race_Id').withColumnRenamed('driverId',  'driver_Id').withColumnRenamed('constructorId',  'constructor_Id').withColumn('ingest_dt', lit(current_dt))

display(df)

# COMMAND ----------

from pyspark.sql.functions import regexp_replace,isnull, when

columns = df.columns
df1 = df
for  i in columns:
    df1 = df1.withColumn(i, regexp_replace(i,'\\\\N','')).withColumn(i, when(col(i).isNull(), '').otherwise(col(i)))
    
display(df1)
df1.write.parquet('/mnt/dataricks-formula-one/silver/qualifying', mode ='overwrite')

# COMMAND ----------


