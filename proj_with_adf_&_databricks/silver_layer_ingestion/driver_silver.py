# Databricks notebook source
display(dbutils.fs.ls('/mnt/formula-one/bronze/'))

# COMMAND ----------

display(spark.read.json('dbfs:/mnt/formula-one/bronze/drivers/'))

# COMMAND ----------

from pyspark.sql.functions import explode,col

df = spark.read.json('dbfs:/mnt/formula-one/bronze/drivers/').select(col('MRData.DriverTable.Drivers.driverId').alias('driver_Id'), col('MRData.DriverTable.Drivers.nationality'), col('MRData.DriverTable.Drivers.familyName').alias('surname'), col('MRData.DriverTable.Drivers.givenName').alias('first_name'))

for i in df.columns:
    df = df.withColumn(i, explode(i))

display(df)


#[ 'driver_Ref', 'number',  'Forename', 'surname']


# COMMAND ----------

from pyspark.sql.functions import col,explode

df = spark.read.json('dbfs:/mnt/formula-one/bronze/drivers/').select(explode(col('MRData.DriverTable.Drivers')))
df.write.json('dbfs:/mnt/formula-one/test/test_silver/', mode='overwrite')

df_flat = spark.read.json('dbfs:/mnt/formula-one/test/test_silver', multiLine=True)

#df = spark.read.json('dbfs:/mnt/formula-one/bronze/drivers/', multiLine= True)
display(df_flat)

# COMMAND ----------

df = spark.read.json('dbfs:/mnt/formula-one/bronze/pitstops/')

display(df)

# COMMAND ----------


