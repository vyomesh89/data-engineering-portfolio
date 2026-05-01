from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    """
    Data ingestion pipeline for Bronze layer using Databricks Auto Loader
    Reads streaming data from cloud storage and writes to Delta Lake
    """
    try:
        # Initialize a Spark session
        spark = SparkSession.builder \
            .appName("Data Ingestion with Auto Loader") \
            .getOrCreate()
        
        logger.info("Spark session initialized successfully")

        # Define the source path and target Delta table path
        source_path = "dbfs:/mnt/data/source/"
        delta_table_path = "dbfs:/mnt/data/delta/bronze_layer/"
        checkpoint_path = delta_table_path + "_checkpoint"

        logger.info(f"Source path: {source_path}")
        logger.info(f"Target Delta table: {delta_table_path}")

        # Read data using Auto Loader with schema inference
        df = spark.readStream \
            .format("cloudFiles") \
            .option("cloudFiles.format", "json") \
            .option("cloudFiles.schemaLocation", checkpoint_path + "/schema") \
            .option("inferSchema", "true") \
            .option("rescuedDataColumn", "_rescuedData") \
            .load(source_path)

        logger.info("Auto Loader configured successfully")

        # Add metadata columns for data lineage
        df = df.withColumn("ingestion_timestamp", current_timestamp()) \
               .withColumn("data_source", "cloud_storage") \
               .withColumn("ingestion_date", current_timestamp().cast("date"))

        logger.info("Metadata columns added")

        # Write to Delta Lake with append mode
        query = df.writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", checkpoint_path) \
            .option("mergeSchema", "true") \
            .start(delta_table_path)

        logger.info("Delta Lake write stream started")
        logger.info(f"Writing to: {delta_table_path}")

        # Await termination
        query.awaitTermination()

    except Exception as e:
        logger.error(f"Error in data ingestion pipeline: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    main()
