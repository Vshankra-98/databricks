# Databricks notebook source
display(dbutils.fs.ls('/mnt/formula-one/bronze/'))

# COMMAND ----------

 from pyspark.sql.functions import col, concat, lit
 
 df = (spark.read.json('dbfs:/mnt/formula-one/bronze/drivers/'))

df = df.select( col('nationality'),  col('driverId').alias('driver_Id'), concat( col('givenName'), lit('  '), col('familyName')).alias("Driver"))

display(df)

df.write.mode('overwrite').format('delta').save('dbfs:/mnt/formula-one/silver/drivers')


# COMMAND ----------



# COMMAND ----------

# from pyspark.sql.functions import explode,col

# df = spark.read.json('dbfs:/mnt/formula-one/bronze/drivers/').select(col('MRData.DriverTable.Drivers.driverId').alias('driver_Id'), col('MRData.DriverTable.Drivers.nationality'), col('MRData.DriverTable.Drivers.familyName').alias('surname'), col('MRData.DriverTable.Drivers.givenName').alias('first_name'))

# for i in df.columns:
#     df = df.withColumn(i, explode(i))

# display(df)


# #[ 'driver_Ref', 'number',  'Forename', 'surname']

