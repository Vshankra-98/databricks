# Databricks notebook source
# MAGIC %run ../utils/common_functions

# COMMAND ----------

dbutils.widgets.text("formatted_date","20240417")
formatted_date = dbutils.widgets.get("formatted_date")

# COMMAND ----------

# Create dataframe on top of json file
df = spark.read.json(f"/mnt/formulaone/bronze/drivers/{formatted_date}/")
# df.display()

# COMMAND ----------

# Rename the columns as per requirement
# new_df = df.withColumnRenamed(existing_column_name, new_column_name)
df = (
    df.withColumnRenamed("permanentNumber", "Number")
    .withColumnRenamed("driverid", "driver_id")
    .withColumn("Driver", concat(col("givenName"),lit(' '),col("familyName")))
)
df = df.drop("familyName", "givenName")

# COMMAND ----------

# df.display()

# COMMAND ----------

# Save the data as delta table in the silver layer
df.distinct().write.mode("overwrite").format("delta").save(f"/mnt/formulaone/silver/drivers/{formatted_date}/")
