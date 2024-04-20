# Databricks notebook source
# MAGIC %run ../Utils/common_functions

# COMMAND ----------

from datetime import datetime
from pyspark.sql.types import IntegerType,StructField, StructType,StringType,FloatType
from pyspark.sql.functions import lit,col

# COMMAND ----------

# define Schema

input_schema = StructType([
    StructField('constructorId', IntegerType()),
    StructField('constructorRef', StringType()),
    StructField('name', StringType()),
    StructField('nationality', StringType()),
    StructField('url', StringType())
])

# COMMAND ----------

dbutils.fs.ls('/mnt/bronze')

# COMMAND ----------

df = spark.read.json('dbfs:/mnt/bronze/constructors.json', schema=input_schema)
display(df)
df.printSchema()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Few Trasformations
# MAGIC

# COMMAND ----------

current_dt = datetime.today().strftime('%Y-%m-%d')

# COMMAND ----------

df = df.withColumnRenamed('constructorId', 'constructor_Id').withColumnRenamed('constructorRef', 'constructor_Ref').withColumn('ingest_dt', lit(current_dt))

display(df)

# COMMAND ----------

df.write.parquet('/mnt/silver/constructors',mode ='overwrite')

# COMMAND ----------


