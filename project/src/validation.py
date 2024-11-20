def validate_data(df):
    # Example validation: check for null values in critical columns
    if df.filter(df['id'].isNull()).count() > 0:
        raise ValueError("Validation Error: 'id' column contains null values.")
    print("Validation Successful: No null values found in critical columns.")
