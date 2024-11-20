from extraction import extract_csv, extract_db
from transformation import transform_data
from validation import validate_data
from load import load_data
from pyspark.sql import SparkSession
import yaml
import pandas as pd

if __name__ == "__main__":
    # Initialize Spark
    spark = SparkSession.builder.appName("ETL Project").getOrCreate()

    # Extract
    csv_df = extract_csv(spark, "../data/input/data_file.csv")
    with open('../config/config.yaml', 'r') as file:
        config = yaml.safe_load(file)
    db_df = extract_db("SELECT * FROM employee", config)

    # Transform
    transformed_df = transform_data(csv_df, db_df)

    # Validate
    validate_data(transformed_df)

    # Load
    load_data(transformed_df, "../data/output/", "custom_output.csv")


    spark.stop()
