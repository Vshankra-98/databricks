# Databricks notebook source
# MAGIC %run ../Utils/common_functions

# COMMAND ----------

dbutils.fs.ls('/mnt/bronze/')

# COMMAND ----------

from pyspark.sql.types import DateType, ArrayType

input_schema = StructType([
    StructField('driverId', IntegerType()),
    StructField('driverRef', StringType()),
    StructField('number', IntegerType()),
    StructField('code', StringType()),
    StructField('name',
        StructType([
            StructField('forename', StringType()),
            StructField('surname', StringType())
        ])),
    StructField('dob', DateType()),  
    StructField('nationality', StringType()), 
    StructField('url', StringType())
])

# COMMAND ----------

df = spark.read.json('dbfs:/mnt/bronze/drivers.json', schema=input_schema)
df.display()
df.printSchema()

# COMMAND ----------

## converting nested column into single column

df = df.withColumn('Forename', col('name').forename).withColumn('surname', col('name').surname).drop('name')
df.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## few Trasfomation

# COMMAND ----------

current_dt = datetime.today().strftime('%Y-%m-%d')

df = df.withColumnRenamed('driverId', 'driver_Id').withColumnRenamed('driverRef','driver_Ref').withColumn('ingest_dt', lit(current_dt))

df.display()

# COMMAND ----------

df.write.parquet('/mnt/silver/drivers',mode ='overwrite')

# COMMAND ----------


