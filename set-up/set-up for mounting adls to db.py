# Databricks notebook source
storage_account_name='f1racedlwg'
client_id= dbutils.secrets.get('db-course-scope','client-id')
client_secret=dbutils.secrets.get('db-course-scope','client-secret')
tenant_id=dbutils.secrets.get('db-course-scope','tenant-id')


# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
        "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider", 
        "fs.azure.account.oauth2.client.id": f"{client_id}",
        "fs.azure.account.oauth2.client.secret": f"{client_secret}",
        "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

def mount_adls(container_name):
    dbutils.fs.mount(
        source= f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point=f"/mnt/{storage_account_name}/{container_name}",
        extra_configs=configs
        )

# COMMAND ----------

mount_adls('raw')
mount_adls('processed')
mount_adls('presentation')

# COMMAND ----------

dbutils.fs.ls('/mnt/f1racedlwg/raw')
dbutils.fs.ls('/mnt/f1racedlwg/processed')
dbutils.fs.ls('/mnt/f1racedlwg/presentation')

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

