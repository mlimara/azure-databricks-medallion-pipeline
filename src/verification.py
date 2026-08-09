# Databricks notebook source
from pyspark.sql.functions import current_timestamp

# Define the path for your new audit log table (e.g., in the Bronze or Silver layer)
log_table_path = "abfss://imp-data@imptestwesteurope.dfs.core.windows.net/data/Team_C/lemara_mukhamedyarova/bronze/logs"

def verify_completeness(spark, source_df, target_df, table_name):
    """
    Compares row counts between source and target DataFrames, 
    evaluates completeness, and appends the result to a Delta log table.
    """
    # 1. Calculate metrics
    source_count = source_df.count()
    target_count = target_df.count()
    difference = source_count - target_count
    
    # 2. Determine pipeline status
    if difference == 0:
        status = "SUCCESS (100% Match)"
    elif difference > 0:
        status = "WARNING (Data Loss)"
    else:
        status = "WARNING (Data Duplication)"
        
    # 3. Create a single-row DataFrame with the audit data
    audit_data = [(table_name, source_count, target_count, difference, status)]
    audit_columns = ["table_name", "source_count", "target_count", "difference", "status"]
    
    audit_df = spark.createDataFrame(audit_data, schema=audit_columns) \
        .withColumn("log_timestamp", current_timestamp())
    
    # 4. Append the record to the Delta log table
    audit_df.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .save(log_table_path)
        
    print(f"Log recorded for '{table_name}': {status} | Source: {source_count}, Target: {target_count}")

# Run verification for all tables
path = "abfss://imp-data@imptestwesteurope.dfs.core.windows.net/data/raw_data/"
addresses = spark.read.parquet(path + "address")
orders = spark.read.parquet(path + "orders")
customers = spark.read.parquet(path + "customers")
items = spark.read.parquet(path + "items")
order_details = spark.read.parquet(path + "orderDetails")
bronze_path = "abfss://imp-data@imptestwesteurope.dfs.core.windows.net/data/Team_C/lemara_mukhamedyarova/bronze/"
verify_completeness(spark, customers, spark.read.format("delta").load(f"{bronze_path}customers"), "Customers")
verify_completeness(spark, addresses, spark.read.format("delta").load(f"{bronze_path}addresses"), "Addresses")
verify_completeness(spark, orders, spark.read.format("delta").load(f"{bronze_path}orders"), "Orders")
verify_completeness(spark, items, spark.read.format("delta").load(f"{bronze_path}items"), "Items")
verify_completeness(spark, order_details, spark.read.format("delta").load(f"{bronze_path}order_details"), "Order Details")