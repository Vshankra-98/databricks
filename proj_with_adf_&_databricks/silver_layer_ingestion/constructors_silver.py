# Databricks notebook source
dbutils.widgets.text('layer','')
dbutils.widgets.text('dataset_name', '')



# COMMAND ----------

layer = dbutils.widgets.get('layer')
dataset_name = dbutils.widgets.get('dataset_name')

# COMMAND ----------

display(dbutils.fs.ls('/mnt/formula-one/bronze/'))

# COMMAND ----------

 from pyspark.sql.functions import col, concat, lit
 
 df = (spark.read.json('dbfs:/mnt/formula-one/bronze/constructors/'))

df = df.select( col('constructorId').alias('constructor_Id'),  col('constructorRef').alias('constructor_Ref'), col('name').alias('Team'), col('nationality'))

display(df)

df.write.mode('overwrite').format('delta').save(f'dbfs:/mnt/formula-one/{layer}/{dataset_name}')


# COMMAND ----------



# COMMAND ----------

# from pyspark.sql.functions import explode,col

# df = spark.read.json('dbfs:/mnt/formula-one/bronze/drivers/').select(col('MRData.DriverTable.Drivers.driverId').alias('driver_Id'), col('MRData.DriverTable.Drivers.nationality'), col('MRData.DriverTable.Drivers.familyName').alias('surname'), col('MRData.DriverTable.Drivers.givenName').alias('first_name'))

# for i in df.columns:
#     df = df.withColumn(i, explode(i))

# display(df)


# #[ 'driver_Ref', 'number',  'Forename', 'surname']

