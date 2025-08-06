# data_types.py

def get_column_data_types(spark_df):
    """
    Returns a dictionary of column names and their corresponding data types.
    
    Args:
        spark_df (pyspark.sql.DataFrame): Spark DataFrame to profile.

    Returns:
        dict: {column_name: data_type}
    """
    if not spark_df:
        raise ValueError("[ERROR] No DataFrame provided to get_column_data_types")

    return {field.name: field.dataType.simpleString() for field in spark_df.schema.fields}



# def get_data_types(df):
#     return dict(df.dtypes)
