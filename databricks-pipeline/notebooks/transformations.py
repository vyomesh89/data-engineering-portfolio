# transformations.py

# Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Databricks Pipeline") \
    .getOrCreate()

# Function to read data from Bronze Layer

def read_bronze_data(path):
    return spark.read.format("parquet").load(path)

# Function for data cleaning

def clean_data(df):
    # Drop null values
    df_cleaned = df.na.drop()
    # Drop duplicates
    df_cleaned = df_cleaned.dropDuplicates()
    return df_cleaned

# Function for type casting

def cast_data_types(df):
    df_casted = df \
        .withColumn("id", col("id").cast("integer")) \
        .withColumn("amount", col("amount").cast("float"))
    return df_casted

# Function for partitioning

def partition_data(df, partition_column):
    return df.repartition(partition_column)

# Function for bucketing

def bucket_data(df, bucket_column, num_buckets):
    return df.write.bucketBy(num_buckets, bucket_column).saveAsTable("bucketed_table")

# Function for caching

def cache_data(df):
    df.cache()
    return df

# Function for optimization

def optimize_dataframe(df):
    # Example optimization strategies
    df_optimized = df.coalesce(2)  # Reduce partitions
    return df_optimized

# Main processing function

def process_data(bronze_path, partition_column, bucket_column, num_buckets):
    # Read data
    df = read_bronze_data(bronze_path)
    
    # Clean data
    df_cleaned = clean_data(df)
    
    # Cast data types
    df_casted = cast_data_types(df_cleaned)
    
    # Cache the cleaned and casted DataFrame
    df_cached = cache_data(df_casted)
    
    # Partition data
    df_partitioned = partition_data(df_cached, partition_column)
    
    # Optimizing DataFrame
    df_optimized = optimize_dataframe(df_partitioned)
    
    # Bucket data
    bucket_data(df_optimized, bucket_column, num_buckets)

# Usage example (to be removed in production)
# process_data("path/to/bronze_data", "date", "id", 10)
