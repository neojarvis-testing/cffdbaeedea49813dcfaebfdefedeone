from pyspark.sql import DataFrame
from pyspark.sql.functions import col

def transform_data(csv_df: DataFrame, db_df: DataFrame) -> DataFrame:
    """Transform the extracted data."""
    # Filter the csv_df based on age
    csv_filtered = csv_df.filter(col("age") > 26)

    # Example transformation: Increase salary by 30% for filtered records
    csv_filtered = csv_filtered.withColumn("salary", (col("salary") * 1.3).cast("int"))


    # Perform a join operation (ensure "id" column exists in both DataFrames)
    final_df = csv_filtered.join(db_df, "id", "inner")

    return final_df
