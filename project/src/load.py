import os
import shutil
from pyspark.sql import DataFrame

def load_data(df: DataFrame, output_path: str, custom_filename: str):
    """Load the transformed DataFrame to a CSV file with a custom name."""
    # Ensure the output path exists
    os.makedirs(output_path, exist_ok=True)

    # Coalesce the DataFrame to a single partition to write a single CSV file
    df.coalesce(1).write.csv(os.path.join(output_path, "temp_output"), header=True, mode="overwrite")

    # Rename the part file to the desired filename
    part_file_path = os.path.join(output_path, "temp_output")
    
    # Find and rename the part file
    for filename in os.listdir(part_file_path):
        if filename.startswith("part-"):
            # Construct the full path of the old and new file
            old_file = os.path.join(part_file_path, filename)
            new_file = os.path.join(output_path, custom_filename)
            os.rename(old_file, new_file)

    # Remove the temporary output directory and its contents
    shutil.rmtree(part_file_path)
    
    print(f"Data successfully loaded to {os.path.join(output_path, custom_filename)}")
