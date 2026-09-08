# Databricks notebook source
#1. Define function to upsert data into Delta Lake
from delta.tables import DeltaTable

def upsert_to_delta(df, table_path, primary_key):
    # Check if the Delta table already exists
    if DeltaTable.isDeltaTable(spark, table_path):
        delta_table = DeltaTable.forPath(spark, table_path)
        
        # Define the merge condition
        merge_condition = f"target.{primary_key} = source.{primary_key}"
        
        # Perform the Merge/Upsert
        delta_table.alias("target").merge(
            df.alias("source"),
            merge_condition
        ).whenMatchedUpdateAll(
        ).whenNotMatchedInsertAll(
        ).execute()
        print(f"Upserted data into {table_path}")
    else:
        # Initial load: write the dataframe as a new Delta table
        df.write.format("delta").mode("overwrite").save(table_path)
        print(f"Created new Delta table at {table_path}")

# COMMAND ----------

# task 3
#2. Read data from the raw_data folder
from pyspark.sql.functions import sum
from delta.tables import DeltaTable

path = "abfss://imp-data@imptestwesteurope.dfs.core.windows.net/data/raw_data/"
bronze_path = "abfss://imp-data@imptestwesteurope.dfs.core.windows.net/data/Team_C/lemara_mukhamedyarova/bronze/"
addresses = spark.read.parquet(path + "address")
orders = spark.read.parquet(path + "orders")
customers = spark.read.parquet(path + "customers")
items = spark.read.parquet(path + "items")
order_details = spark.read.parquet(path + "orderDetails")
# Run the upsert function for each table using their specific primary keys
upsert_to_delta(customers, f"{bronze_path}customers", "id")
upsert_to_delta(addresses, f"{bronze_path}addresses", "id")
upsert_to_delta(items, f"{bronze_path}items", "id")
upsert_to_delta(orders, f"{bronze_path}orders", "id")

# Order Details has a composite key, so the merge condition is slightly different.
order_details.write.format("delta").mode("overwrite").save(f"{bronze_path}order_details")
print(f"Created new Delta table at {bronze_path}order_details")
