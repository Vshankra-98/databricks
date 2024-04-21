# Databricks notebook source
# MAGIC %run ../utils/common_functions

# COMMAND ----------

dbutils.widgets.text("formatted_date","20240417")
formatted_date = dbutils.widgets.get("formatted_date")

# COMMAND ----------

dbutils.fs.ls("/mnt/formulaone/bronze/qualifying/")

# COMMAND ----------

df = spark.read.json(f"/mnt/formulaone/bronze/qualifying/{formatted_date}/")
# df.display()

# COMMAND ----------

df = df.select("MRData.RaceTable.season","MRData.RaceTable.Races").withColumn("Races",explode(col("Races")))
# df.display()

# COMMAND ----------

df = (
    df.select("season","Races.Circuit.circuitId","Races.QualifyingResults")
    .withColumn("QualifyingResults", explode(col("QualifyingResults")))
    .select(
        col("season"),
        col("circuitId").alias("circuit_id"),
        col("QualifyingResults.Constructor.constructorId").alias("constructor_id"),
        col("QualifyingResults.Constructor.name").alias("Team"),
        col("QualifyingResults.Driver.driverId").alias("driver_id"),
        col("QualifyingResults.Driver.givenName"),
        col("QualifyingResults.Driver.familyName"),
        col("QualifyingResults.Driver.nationality"),
        col("QualifyingResults.Driver.permanentNumber"),
        col("QualifyingResults.Q1"),
        col("QualifyingResults.Q2"),
        col("QualifyingResults.Q3"),
        col("QualifyingResults.position"),
    )
)
# df.display()

# COMMAND ----------

df = df.withColumn("Driver",concat(col("givenName"),lit(' '),col("familyName"))).drop(col("givenName"),col("familyName"))
# df.display()

# COMMAND ----------

df = df.distinct()

# COMMAND ----------

# Save the dataframe as delta file in the silver layer
df.write.mode("overwrite").format("delta").save(f"/mnt/formulaone/silver/qualifying/{formatted_date}/")

# COMMAND ----------

# # Reading delta table
# spark.read.format("delta").load(f"/mnt/formulaone/silver/qualifying/{formatted_date}/").display()
