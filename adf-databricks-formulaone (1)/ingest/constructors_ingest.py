# Databricks notebook source
# MAGIC %run ../utils/common_functions

# COMMAND ----------

dbutils.widgets.text("formatted_date","20240417")
formatted_date = dbutils.widgets.get("formatted_date")

# COMMAND ----------

# Create the dataframe based on json file.
df = spark.read.json(f"/mnt/formulaone/bronze/constructors/{formatted_date}/")


# COMMAND ----------

# Assuming you have columns 'old_name1', 'old_name2', etc.
column_mapping = {"constructorid": "constructor_id", "constructorRef": "constructor_ref"}

df = df.withColumnsRenamed(column_mapping)

# df.display()

# COMMAND ----------

df.distinct().write.mode("overwrite").format("delta").save(f"/mnt/formulaone/silver/constructors/{formatted_date}/")
