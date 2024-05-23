# Databricks notebook source
dbutils.widgets.text('storage_account', 'stproject123')
dbutils.widgets.text('container_name', 'dataricks-formula-one')

# COMMAND ----------

dbutils.secrets.listScopes()


# COMMAND ----------


dbutils.secrets.list('stproject')

# COMMAND ----------

app_id = dbutils.secrets.get('stproject', 'app-id')
directory_id = dbutils.secrets.get('stproject', 'directory-id')
screte = dbutils.secrets.get('stproject', 'secrete')



# COMMAND ----------

storage_account = dbutils.widgets.get('storage_account')
container_name = dbutils.widgets.get('container_name')
print(container_name)

# COMMAND ----------


configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": app_id,
          "fs.azure.account.oauth2.client.secret": screte ,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{directory_id}/oauth2/token"}


# COMMAND ----------

dbutils.fs.mount(
  source = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/",
  mount_point = f"/mnt/{container_name}",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.mounts())


# COMMAND ----------

display(dbutils.fs.ls(f"/mnt/{container_name}/"))

# COMMAND ----------


#spark.read.csv("dbfs:/mnt/dataricks-formula-one/bronze/pit_stops.json", multiLine=True).display()



# COMMAND ----------


