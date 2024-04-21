# Databricks notebook source
df = spark.read.json(path = f"/mnt/formulaone/bronze/results/20240411/")

# COMMAND ----------

from pyspark.sql.functions import col, explode
df.select("MRData.RaceTable.Races.Circuit.Location.country").withColumn("country",explode(col("country"))).display()

# COMMAND ----------

df1 = df.select("MRData.RaceTable.Races")
df1.display()

# COMMAND ----------

df1 = df1.withColumn("Races",explode("Races"))


# COMMAND ----------

df1.select("Races.Circuit.Location.locality").display()

# COMMAND ----------

df = spark.read.json(path = f"/mnt/formulaone/bronze/results/20240411/")
df.display()

# COMMAND ----------

df1 = df.select("MRData.RaceTable.Races.Results.Constructor.nationality")

# COMMAND ----------

df1.withColumn("nationality",explode("nationality")).withColumn("nationality",explode("nationality")).display()

# COMMAND ----------

dbutils.secrets.list("aws-scope")

# COMMAND ----------

username = dbutils.secrets.get("aws-scope","username")
print(username)
