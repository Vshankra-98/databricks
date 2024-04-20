# Databricks notebook source
# MAGIC %md
# MAGIC ## Mounting Bronze layer data to ADB 

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.text('layer_name', 'bronze')
layer_name = dbutils.widgets.get('layer_name')

# COMMAND ----------

print(layer_name)

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list("service_principle_attributes_scope")

# COMMAND ----------

application_id =dbutils.secrets.get('service_principle_attributes_scope', 'application-id')
directory_id = dbutils.secrets.get('service_principle_attributes_scope','directory-id')
secretes = dbutils.secrets.get('service_principle_attributes_scope', 'secrete')


# COMMAND ----------

for i in dbutils.fs.mounts():
    print(i.mountPoint)

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": application_id,
          "fs.azure.account.oauth2.client.secret": secretes,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{directory_id}/oauth2/token"}

Mount_Point = f"/mnt/{layer_name}"

if not any(Mount_Point == MountInfo.mountPoint for MountInfo in dbutils.fs.mounts()):

    # Optionally, you can add <directory-name> to the source URI of your mount point.
    dbutils.fs.mount(
    source = f"abfss://{layer_name}@formulaonevenkat.dfs.core.windows.net/",
    mount_point = Mount_Point,
    extra_configs = configs)
    print(f'mount point --> {Mount_Point} seccusfully created ..!')

else:
    print("mount point already exists..!")

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

display(dbutils.fs.ls(r"/mnt/{layer_name}"))

# COMMAND ----------


