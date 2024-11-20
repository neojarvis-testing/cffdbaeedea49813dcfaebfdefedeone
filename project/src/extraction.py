from pyspark.sql import SparkSession, DataFrame

import mysql.connector
import yaml
import pandas as pd

def extract_csv(spark: SparkSession, file_path: str) -> DataFrame:
    """Extract data from a CSV file."""
    return spark.read.csv(file_path, header=True, inferSchema=True)

def extract_db(query: str, config: dict) -> DataFrame:
    """Extract data from a MySQL database."""
    conn = mysql.connector.connect(**config['mysql'])
    # Use pandas to read the SQL data
    df = pd.read_sql(query, conn)
    conn.close()

    # Create a PySpark DataFrame from the Pandas DataFrame
    spark = SparkSession.builder.getOrCreate()
    return spark.createDataFrame(df)

if __name__ == "__main__":
    # Ensure the Spark session is created only when this script is run directly
    spark = SparkSession.builder.appName("ETL Project").getOrCreate()

