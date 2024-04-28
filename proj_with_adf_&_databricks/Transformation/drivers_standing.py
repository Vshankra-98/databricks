# Databricks notebook source
display(dbutils.fs.ls('/mnt/formula-one/silver'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimization Techniques -->Columanar pronuning-->
# MAGIC  reduce amount of data before join opration to reduce the shuffling overhead which optimizes the performance using selecting important columns

# COMMAND ----------

display(spark.read.format('delta').load('dbfs:/mnt/formula-one/silver/results'))

# COMMAND ----------

from pyspark.sql.functions import col,row_number,desc, sum,lit,concat
from pyspark.sql.window import Window




result_df  = spark.read.format('delta').load('dbfs:/mnt/formula-one/silver/results').select( col('Results_Driver_nationality').alias('nationality'),  concat(col('Results_Driver_familyName'), lit('   '), col('Results_Driver_givenName')).alias('Driver'), 'Results_position',col('Results_Constructor_name').alias('Team'), col('Results_points').alias('Points'), "season").filter((col('Results_position') ==1) &  (col('season')  ==2024))







result_df = result_df.withColumn('wins', sum(col('Results_position')).over(Window.partitionBy(col('Driver')))).drop('Results_position', 'season')

resul_standing_api = result_df.select(['nationality', 'Driver', 'Team',  'wins' , 'Points'])


display(resul_standing_api)



# COMMAND ----------

resul_standing_api.write.mode('overwrite').format('parquet').save('dbfs:/mnt/formula-one/gold/resul_standing_api')

# COMMAND ----------


