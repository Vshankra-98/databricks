# Databricks notebook source
# MAGIC %run ../Utils/common_functions

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType,StringType,FloatType,DateType,TimestampType

from datetime import datetime 
from pyspark.sql.functions import lit

input_schema = StructType(
    [
    StructField('raceId', IntegerType()),
    StructField('year', IntegerType()),
    StructField('round', IntegerType()),
    StructField('circuitId', IntegerType()),
    StructField('name', StringType()),
    StructField('date', DateType()),
    StructField('time', TimestampType()),
    StructField('url', StringType()),
    StructField('fp1_date', DateType()),
    StructField('fp1_time', TimestampType()),
    StructField('fp2_date', DateType()),
    StructField('f2_time', TimestampType()),
    StructField('f3_date', DateType()),
    StructField('f3_time', TimestampType()),            
    StructField('quali_date', DateType()),
    StructField('quali_time', TimestampType()),
    StructField('sprint_date', DateType()),
    StructField('sprint_time', TimestampType())
    ]
    )



# COMMAND ----------

dbutils.fs.ls('mnt/dataricks-formula-one/bronze/')

# COMMAND ----------

df = create_csv_df('dbfs:/mnt/dataricks-formula-one/bronze/races.csv',input_schema )
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Few Trasformations

# COMMAND ----------

current_dt = datetime.today().strftime('%Y-%m-%m')

df = df.withColumnRenamed('raceId', 'race_id').withColumnRenamed('circuitId', 'circuit_Id').withColumn('ingest_dt',lit(current_dt))

display(df)

# COMMAND ----------

from pyspark.sql.functions import regexp_replace,isnull,when

columns = df.columns
df1 = df
for  i in columns:
    df1 = df1.withColumn(i, regexp_replace(i,'\\\\N','')).withColumn(i, when(col(i).isNull(), '').otherwise(col(i)))
    
display(df1)
df1.write.parquet('/mnt/dataricks-formula-one/silver/race', mode ='overwrite')

# COMMAND ----------


