import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

@pytest.fixture(scope="session")
def spark_session():
    """Create Spark session for testing"""
    spark = SparkSession.builder \
        .appName("TestPipeline") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
    
    yield spark
    spark.stop()

@pytest.fixture
def sample_data(spark_session):
    """Create sample test data"""
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("value", DoubleType(), True),
        StructField("category", StringType(), True)
    ])
    
    data = [(1, "Alice", 100.5, "A"), (2, "Bob", 200.75, "B"), (3, "Charlie", 150.0, "A")]
    return spark_session.createDataFrame(data, schema=schema)

class TestDataQuality:
    """Data quality tests"""
    
    def test_data_not_empty(self, sample_data):
        assert sample_data.count() > 0, "DataFrame should not be empty"
    
    def test_required_columns_exist(self, sample_data):
        required_cols = ["id", "name", "value", "category"]
        assert all(col in sample_data.columns for col in required_cols)
    
    def test_no_null_in_id(self, sample_data):
        null_count = sample_data.filter(sample_data.id.isNull()).count()
        assert null_count == 0, "ID column should not contain nulls"

class TestTransformations:
    """Spark transformation tests"""
    
    def test_column_aggregation(self, sample_data):
        result = sample_data.groupBy("category").sum("value")
        assert result.count() > 0
    
    def test_filter_transformation(self, sample_data):
        filtered = sample_data.filter(sample_data.value > 150)
        assert filtered.count() > 0
        assert filtered.count() < sample_data.count()
    
    def test_deduplication(self, sample_data):
        deduplicated = sample_data.dropDuplicates(["id"])
        assert deduplicated.count() == sample_data.count()

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
