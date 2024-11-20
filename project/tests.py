import pytest
from pyspark.sql import SparkSession
from src.extraction import extract_csv, extract_db
from src.transformation import transform_data
from src.validation import validate_data
from src.load import load_data
import yaml
import os
from pyspark.sql import Row
from pyspark.sql.functions import col
@pytest.fixture(scope="module")
def setup_spark():
    """Fixture to initialize and provide a SparkSession for testing."""
    spark = SparkSession.builder.appName("ETL Testing").getOrCreate()
    with open('../project/config/config.yaml', 'r') as file:
        config = yaml.safe_load(file)
    yield spark, config
    spark.stop()

### Functional Test Cases

def test_extract_csv(setup_spark):
    """Test data extraction from CSV."""
    spark, _ = setup_spark
    csv_df = extract_csv(spark, '../project/data/input/data_file.csv')
    
    # Check if the DataFrame is not empty
    assert csv_df.count() > 0, "CSV extraction failed, DataFrame is empty"
    
    # Check if the DataFrame schema matches expected structure
    expected_columns = ["id", "name", "age", "salary"]
    assert csv_df.columns == expected_columns, "CSV DataFrame schema does not match expected structure"

def test_extract_db(setup_spark):
    """Test data extraction from MySQL database."""
    spark, config = setup_spark
    query = "SELECT * FROM employee"
    db_df = extract_db(query, config)
    
    # Check if the DataFrame is not empty
    assert db_df.count() > 0, "DB extraction failed, DataFrame is empty"
    
    # Check if DataFrame has expected columns
    expected_columns = ["id", "occupation"]
    assert set(expected_columns).issubset(set(db_df.columns)), "DB DataFrame schema does not match expected structure"

def test_transform_data(setup_spark):
    """Test data transformation."""
    spark, _ = setup_spark
    csv_df = extract_csv(spark, '../project/data/input/data_file.csv')
    query = "SELECT * FROM employee"
    
    with open('../project/config/config.yaml', 'r') as file:
        config = yaml.safe_load(file)
    db_df = extract_db(query, config)
    
    transformed_df = transform_data(csv_df, db_df)
    
    # Check if the transformed DataFrame has the expected columns
    expected_columns = ["id", "name", "age", "salary", "occupation"]
    assert set(expected_columns).issubset(set(transformed_df.columns)), "Transformed DataFrame columns missing"

def test_validate_data(setup_spark):
    """Test data validation."""
    spark, _ = setup_spark
    csv_df = extract_csv(spark, '../project/data/input/data_file.csv')
    query = "SELECT * FROM employee"
    
    with open('../project/config/config.yaml', 'r') as file:
        config = yaml.safe_load(file)
    db_df = extract_db(query, config)
    
    transformed_df = transform_data(csv_df, db_df)
    
    # Test validation for null values
    try:
        validate_data(transformed_df)
    except ValueError:
        pytest.fail("Validation failed unexpectedly for valid data")
    
def test_load_data(setup_spark):
    """Test data loading to CSV."""
    spark, _ = setup_spark
    csv_df = extract_csv(spark, '../project/data/input/data_file.csv')
    query = "SELECT * FROM employee"
    
    with open('../project/config/config.yaml', 'r') as file:
        config = yaml.safe_load(file)
    db_df = extract_db(query, config)
    
    transformed_df = transform_data(csv_df, db_df)
    
    # Test loading data
    output_path = "../project/data/output/"
    custom_filename = "custom_output.csv"
    load_data(transformed_df, output_path, custom_filename)
    
    # Check if the file was created
    assert os.path.exists(os.path.join(output_path, custom_filename)), "Output file was not created"



def test_age_filter(setup_spark):
    """Test that only rows with age > 26 are retained after the transformation."""
    spark, _ = setup_spark
    
    # Mock CSV input data
    mock_csv_data = [
        Row(id=1, name="Alice", age=24, salary=5000),
        Row(id=2, name="Bob", age=28, salary=6000),
        Row(id=3, name="Charlie", age=27, salary=7000),
        Row(id=4, name="David", age=26, salary=8000)
    ]
    
    csv_df = spark.createDataFrame(mock_csv_data)
    
    # Mock DB data (if relevant to the transformation)
    mock_db_data = [Row(id=1, occupation="Engineer"), Row(id=2, occupation="Doctor")]
    db_df = spark.createDataFrame(mock_db_data)
    
    transformed_df = transform_data(csv_df, db_df)

    # Assert that no rows with age <= 26 exist in the transformed DataFrame
    assert transformed_df.filter(col("age") <= 26).count() == 0, "Transformation failed: Rows with age <= 26 found"


def test_salary_increase(setup_spark):
    """Test that salary has been increased by 30% for the filtered rows."""
    spark, _ = setup_spark
    
    # Mock CSV input data
    mock_csv_data = [
        Row(id=1, name="Alice", age=24, salary=5000),
        Row(id=2, name="Bob", age=28, salary=6000),
        Row(id=3, name="Charlie", age=27, salary=7000),
        Row(id=4, name="David", age=26, salary=8000)
    ]
    
    csv_df = spark.createDataFrame(mock_csv_data)
    
    # Mock DB data (if relevant to the transformation)
    mock_db_data = [Row(id=1, occupation="Engineer"), Row(id=2, occupation="Doctor")]
    db_df = spark.createDataFrame(mock_db_data)
    
    # Apply the transformation
    transformed_df = transform_data(csv_df, db_df)
    
    # Check if the salary increase has been correctly applied
    transformed_df = transformed_df.withColumn("expected_salary", (col("salary") / 1.3).cast("int"))
    
    # Verify that the salary was increased by 30%
    assert transformed_df.filter(col("salary") != col("expected_salary") * 1.3).count() == 0, \
        "Transformation failed: Salary was not increased by 30%"

