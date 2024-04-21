# Databricks notebook source
# MAGIC %run ../utils/common_functions

# COMMAND ----------

dbutils.widgets.text("formatted_date","20240417")
formatted_date = dbutils.widgets.get("formatted_date")

# COMMAND ----------

# Creating a dataframe on the top of parquet file in silver layer
drivers_df = spark.read.format("delta").load(f"/mnt/formulaone/silver/drivers/{formatted_date}/")
# Choosing driver_id, Driver, nationality
drivers_df = drivers_df.select("driver_id","Driver","Number")
# drivers_df.display()

# COMMAND ----------

# Create dataframe based on locations
qualifying_df = spark.read.format("delta").load(f"/mnt/formulaone/silver/qualifying/{formatted_date}/")

# COMMAND ----------

# Replace '\N' from Q1, Q2, Q3 column with empty string
qualifying_df = qualifying_df.withColumn("Q1", regexp_replace(col("Q1"), r"[\\,N]", "")).withColumn("Q2", regexp_replace(col("Q2"), r"[\\,N]", "")).withColumn("Q3", regexp_replace(col("Q3"), r"[\\,N]", ""))

# qualifying_df.display()

# COMMAND ----------

final_df = qualifying_df.join(drivers_df,['driver_id'],'left').select("nationality",qualifying_df["Driver"],col("permanentNumber").alias("Number"),"Team",col("Q1").alias("Qualifying_1"),col("Q2").alias("Qualifying_2"),col("Q3").alias("Qualifying_3"),"position","driver_id","season","circuit_id","constructor_id")

# final_df.display()

# COMMAND ----------

final = final_df.withColumn("min_q", when((col("Qualifying_1")=='') & (col("Qualifying_2")=='') & (col("Qualifying_3")==''),'30:00.000').when((col("Qualifying_1")!='') & (col("Qualifying_2")=='') & (col("Qualifying_3")==''),col("Qualifying_1")).when((col("Qualifying_1")!='') & (col("Qualifying_2")!='') & (col("Qualifying_3")==''),least(col("Qualifying_1"),col("Qualifying_2"))).otherwise(least(col("Qualifying_1"),col("Qualifying_2"),col("Qualifying_3"))))

# COMMAND ----------

# Save dataframe as delta table
final.write.mode("overwrite").format("delta").save(f"/mnt/formulaone/gold/qualifying/{formatted_date}/")

# COMMAND ----------

qualifying_standing = final.select(col("nationality"),col("Driver"),col("Number"),col("Team"),col("Qualifying_1"),col("Qualifying_2"),col("Qualifying_3"),col("min_q").alias("Best_time"),col("season"))
# qualifying_standing.display()

# COMMAND ----------

# Write the result as qualifying standing
qualifying_standing = qualifying_standing.withColumn("Ranking", row_number().over(Window.partitionBy(col("season")).orderBy(col("Best_time").asc())))
qualifying_standing = qualifying_standing.filter(col("Best_time")!='30:00.000').distinct()

# COMMAND ----------

qualifying_standing.write.mode("overwrite").format("delta").save(f"/mnt/formulaone/gold/qualifying_standing/{formatted_date}/")
