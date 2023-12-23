# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
import pandas as pd
from datetime import datetime
import os

# Step 1: Data Ingestion to Data Warehouse using PySpark or Pandas
def ingest_data(source_folder, day, no):

    # Load data files into Data Warehouse
    item_df.append(spark.read.csv(f".{source_folder}/items.csv", header=True))
    customer_df.append(spark.read.csv(f".{source_folder}/customers.csv", header=True))
    order_header_df.append(spark.read.csv(f".{source_folder}/order_headers.csv", header=True))
    order_line_df.append(spark.read.csv(f".{source_folder}/order_lines.csv", header=True))

    # Data Ingestion audit
    start_time = datetime.now()
    numberrow_treatment = item_df[no].count() + customer_df[no].count() + order_header_df[no].count() + order_line_df[no].count()
    
    # Archive data files
    archive_folder = f"{day}_archive"
    os.makedirs(archive_folder, exist_ok=True)
    for file_name in os.listdir(f".{source_folder}"):
        os.rename(os.path.join(f".{source_folder}", file_name), os.path.join(archive_folder, file_name))

    status = "Success"
    end_time = datetime.now()

    audit_df = pd.DataFrame({
        'start_time': [start_time],
        'end_time': [end_time],
        'numberrow_treatment': [numberrow_treatment],
        'status': [status]
    })

    audit_df.to_csv(f"./{archive_folder}/Audit_{day}.csv", index=False)
   

# Step 2: Data Ingestion to Data Warehouse using PySpark or Pandas
def concatenate(df):
    for i in range(len(df)-1):
        all_item_df = df[i].union(df[i+1])
    return all_item_df

def tranformation(df):
    spark = SparkSession.builder.master("local").appName("EssilorDatawareHouse").getOrCreate()

    # drop duplicated values
    df = df.dropDuplicates()

    # Iterate through all columns and apply the date format conversion dynamically
    for column_name, data_type in df.dtypes:
        if 'date' in column_name.lower():
            df = df.withColumn(column_name, to_date(col(column_name), 'M/d/yyyy'))

    # Show the result
    df = df.orderBy(df.columns[0])
    return df

spark = SparkSession.builder.master("local").appName("EssilorDatawareHouse").getOrCreate()

source_folder_day1 = "/day1_files"
source_folder_day2 = "/day2_files"

item_df = []
customer_df = []
order_header_df = []
order_line_df = []

# Data Ingerstion
ingest_data(source_folder_day1, "day1", 0)
ingest_data(source_folder_day2, "day2", 1)

# Concatenate Data
all_item_df = concatenate(item_df)
all_customer_df = concatenate(customer_df)
all_order_headers_df = concatenate(order_header_df)
all_order_lines_df = concatenate(order_line_df)

# Data Transformation
item_transform_df = tranformation(all_item_df)
customer_transform_df = tranformation(all_customer_df)
oder_header_transform_df = tranformation(all_order_headers_df)
order_line_transform_df = tranformation(all_order_lines_df)

# Data Mart
order_line_selected = order_line_transform_df.select(
    col("order_id"),
    col("item_id"),
    col("ship_date"),
    col("promise_date"),
    col("ordered_quantity")
)
customers_selected = customer_transform_df.select(
    col("customer_id"),
    col("customer_number"),
    col("customer_name"),
    col("address"),
    col("city"),
    col("country_code"),
    col("postal_code")
)

items_selected = item_transform_df.select(
    col("item_id"),
    col("item_description"),
    col("item_status")
)

order_header_selected = oder_header_transform_df.select(
    col("order_id"),
    col("order_number"),
    col("order_date"),
    col("currency"),
    col("customer_id")
)

# Join tables to create the data mart
dataMartDF = (
    order_line_selected
    .join(order_header_selected, "order_id", "inner")
    .join(customers_selected, "customer_id", "inner")
    .join(items_selected, "item_id", "inner")   
)

# Export all table to parquet file
# output_path = "/parquet"
# item_transform_df.write.parquet(output_path + "/items.parquet")
# customer_transform_df.write.parquet(output_path + "/customers.parquet")
# oder_header_transform_df.write.parquet(output_path + "/orderHeader.parquet")
# order_line_transform_df.write.parquet(output_path + "/orderLine.parquet")
# dataMartDF.write.parquet(output_path + "/dataMart.parquet") 

spark.stop()