# Databricks notebook source
spark.conf.set(
    f"fs.azure.account.key.fatimazahraestorage.dfs.core.windows.net", 
    "r79bAXn+0oNWEryOKirl7waacXZKXNXWsRtfyvZiXIdna7XdtzKtlD2zJJATQ+5u7Nn7wFGtSmri+AStWhoqLg=="
)


# COMMAND ----------

dbutils.fs.ls("abfss://publictransportdata@fatimazahraestorage.dfs.core.windows.net/raw/")


# COMMAND ----------

file_location = "abfss://publictransportdata@fatimazahraestorage.dfs.core.windows.net/raw/"


# COMMAND ----------

df = spark.read.format("csv").option("inferSchema", "True").option("header",
"True").option("delimeter",",").load(file_location)

display(df)

# COMMAND ----------

df.count()

# COMMAND ----------

display(df)

# COMMAND ----------


