from pyspark.sql import SparkSession

def main():
    # Initialize a Spark session
    spark = SparkSession.builder \
        .appName("Data Ingestion with Auto Loader") \
        .getOrCreate()

    # Define the source path and target Delta table path
    source_path = "dbfs:/mnt/data/source/"  # Update with your actual source path
    delta_table_path = "dbfs:/mnt/data/delta/bronze_layer/"

    # Read data using Auto Loader
    df = spark.readStream \
        .format("cloudFiles") \
        .option("cloudFiles.format", "json") \  # Update the format as needed
        .option("inferSchema", "true") \
        .load(source_path)

    # Add metadata columns
    from pyspark.sql.functions import current_timestamp
    df = df.withColumn("ingestion_timestamp", current_timestamp()) \
           .withColumn("data_source", "your_data_source_identifier")  # Replace with actual identifier

    # Write to Delta Lake
    df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", delta_table_path + "_checkpoint") \
        .start(delta_table_path)

    # Await termination
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()