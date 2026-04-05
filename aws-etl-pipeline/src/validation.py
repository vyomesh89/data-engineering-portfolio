import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

class DataValidator:
    def __init__(self, schema):
        self.schema = schema
        self.spark = SparkSession.builder.appName("DataValidator").getOrCreate()

    def validate_schema(self, df):
        if df.schema != self.schema:
            raise ValueError("Schema validation failed!")
        print("Schema validation passed!")

    def validate_business_rules(self, df):
        # Add business rules validation logic here
        # For example: 
        # Ensure that 'age' column values are non-negative
        invalid_rows = df.filter(df.age < 0)
        if invalid_rows.count() > 0:
            raise ValueError("Business rules validation failed: Found invalid age values!")
        print("Business rules validation passed!")

    def detect_duplicates(self, df, subset):
        duplicates = df[df.duplicated(subset=subset, keep=False)]
        if not duplicates.empty:
            raise ValueError("Duplicates detected!")
        print("No duplicates found!")

    def validate(self, df):
        self.validate_schema(df)
        self.validate_business_rules(df)
        self.detect_duplicates(df, subset=['id'])  # Assuming 'id' is the primary key

# Example usage:
if __name__ == '__main__':
    # Define the schema
    schema = StructType([
        StructField('id', IntegerType(), True),
        StructField('name', StringType(), True),
        StructField('age', IntegerType(), True)
    ])

    # Create an instance of the validator
    validator = DataValidator(schema)
    
    # Sample DataFrame (replace with your data)
    sample_data = [(1, 'Alice', 30), (2, 'Bob', -5), (3, 'Charlie', 25), (1, 'Alice', 30)]
    df = pd.DataFrame(sample_data, columns=['id', 'name', 'age'])

    # Validate the DataFrame
    validator.validate(df)