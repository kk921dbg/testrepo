# Databricks notebook source
# DBTITLE 1,Read Databricks Dataset IoT Devices JSON
df = spark.read.json("dbfs:/databricks-datasets/iot/iot_devices.json")

# COMMAND ----------

# DBTITLE 1,Configure passthrough
configs = { 
"fs.azure.account.auth.type": "CustomAccessToken",
"fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
}

# COMMAND ----------

# DBTITLE 1,List filesystem
dbutils.fs.ls("abfss://<file_system>@<storage-account-name>.dfs.core.windows.net/")

# COMMAND ----------

# DBTITLE 1,Write IoT Devices JSON
df.write.json("abfss://<file_system>@<storage-account-name>.dfs.core.windows.net/iot_devices.json")

# COMMAND ----------

# DBTITLE 1,Read IoT Devices JSON from ADLS Gen2
df2 = spark.read.json("abfss://<file_system>@<storage-account-name>.dfs.core.windows.net/iot_devices.json")
display(df2)

# COMMAND ----------

# DBTITLE 1,Mount filesystem
dbutils.fs.mount(
  source = "abfss://<file_system>@<storage-account-name>.dfs.core.windows.net/",
  mount_point = "/mnt/mymount",
  extra_configs = configs)

# COMMAND ----------

# DBTITLE 1,List mount
dbutils.fs.ls("/mnt/mymount") 

# COMMAND ----------

# DBTITLE 1,Unmount filesystem
dbutils.fs.unmount("/mnt/mymount") 
