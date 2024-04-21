# Databricks notebook source
dbutils.widgets.text("layer_nm","formulaone")
layer_nm = dbutils.widgets.get("layer_nm")

# COMMAND ----------

dbutils.secrets.list("bwt-session")

# COMMAND ----------

application_id = dbutils.secrets.get(scope="bwt-session",key="kv-formulaone-application-id")
directory_id = dbutils.secrets.get(scope="bwt-session",key="kv-formulaone-directory-id")
service_credential = dbutils.secrets.get(scope="bwt-session",key="kv-formulaone-service-credential")

# COMMAND ----------

# Configuration for mount with service principle:
configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": application_id,
           "fs.azure.account.oauth2.client.secret": service_credential,
           f"fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{directory_id}/oauth2/token"}

if not any(mount.mountPoint==f"/mnt/{layer_nm}" for mount in dbutils.fs.mounts()):
  # mount the adls gen2 storage
  dbutils.fs.mount(
    source = f"abfss://{layer_nm}@apiformulaone.dfs.core.windows.net/",
    mount_point = f"/mnt/{layer_nm}",
    extra_configs = configs)
else:
  print(f"Mount point: '/mnt/{layer_nm}' already exists.")
