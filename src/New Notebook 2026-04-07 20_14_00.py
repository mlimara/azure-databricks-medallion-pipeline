# Databricks notebook source
dbutils.fs.rm("abfss://imp-data@imptestwesteurope.dfs.core.windows.net/data/Team_C/lemara_mukhamedyarova/silver/orders", True)




# COMMAND ----------

path = "abfss://imp-data@imptestwesteurope.dfs.core.windows.net/data/raw_data/"
order_details = spark.read.parquet(path + "customers")
order_details.filter("type = 'affiliate'").display()