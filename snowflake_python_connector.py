# Databricks notebook source
 
pip install snowflake-connector-python

# COMMAND ----------

dbutils.library.restartPython()


# https://ifa34379.east-us-2.azure.snowflakecomputing.com
# OMA98420

# COMMAND ----------

import snowflake.connector

# COMMAND ----------

ctx = snowflake.connector.connect(
    user='shobha12233',
    password='Shobha@12233',
    account='LHLUGBV-OMA98420',
    warehouse='COMPUTE_WH',
    database='mydb'
)

# COMMAND ----------

cur = ctx.cursor()

# COMMAND ----------


sfOptions = {
"sfURL"       : "https://ifa34379.east-us-2.azure.snowflakecomputing.com",
"sfAccount"   : "LHLUGBV-OMA98420",
"sfUser"      : "shobha12233",
"sfPassword"  : "Shobha@12233",
"sfDatabase"  : "MYDB",
"sfSchema"    : "DYNAMIC",
"sfWarehouse" : "COMPUTE_WH",
"sfRole"      : "ACCOUNTADMIN",
}

# COMMAND ----------

# df = spark.read.format("snowflake").options(**snowflake_connection).load("<table>")


df = spark.read.format('snowflake').options(**sfOptions).option('dbtable', 'employee_table_scdType1').load()
df.display()

# COMMAND ----------

# from pyspark.sql import SparkSession

# spark = SparkSession.builder.getOrCreate()

# # Define Snowflake connection details
# snowflake_connection = {
#     "account": "<your_account>",
#     "user": "<your_username>",
#     "password": "<your_password>",
#     "warehouse": "<your_warehouse>",
#     "database": "<your_database>",
#     "schema": "<your_schema>"
# }

# # Read data from a Snowflake table
# df = spark.read.format("snowflake").options(**snowflake_connection).load("<table>")

# # Process the DataFrame using PySpark operations
# df.show()  # Display the data
# df.filter("column_name > 10").write.format("parquet").save("output_data")  # Filter and write to Parquet

# # Stop the SparkSession
# spark.stop()


# COMMAND ----------

query = 'select * from  MYDB.DYNAMIC.employee_table_scdType1;'
cur.execute(query)

# for i in cur.fetch_arrow_all():
#     print(i)

# for i in cur.fetchone():
#     print(i)

# for i in cur.fetchall():
#     print(i)

for i in cur.fetch_arrow_batches():
    print(i)

# COMMAND ----------

for i in cur.fetch_arrow_all():
    print(i)


# COMMAND ----------


for i in cur.fetch_arrow_batches():
    print(i)

# COMMAND ----------

display(dbutils.fs.ls('/mnt/formula-one/bronze/circuits/circuits.csv'))

# COMMAND ----------

circuit_df = spark.read.csv('/mnt/formula-one/bronze/circuits/circuits.csv', header = True, inferSchema=True)
display(circuit_df)


circuit_df.write.format('snowflake').options(**sfOptions).option('dbtable', 'circuit_table').mode('append').options(header = True).save()


# COMMAND ----------




