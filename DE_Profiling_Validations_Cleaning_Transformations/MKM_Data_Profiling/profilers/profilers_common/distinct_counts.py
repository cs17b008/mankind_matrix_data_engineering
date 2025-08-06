# distinct_counts.py

from pyspark.sql.functions import countDistinct

def get_distinct_counts(spark_df):
    """
    Returns the number of distinct values per column in the given DataFrame.

    Args:
        spark_df (pyspark.sql.DataFrame): Spark DataFrame to analyze.

    Returns:
        dict: {column_name: distinct_count}
    """
    if not spark_df:
        raise ValueError("[ERROR] No DataFrame provided to get_distinct_counts")

    result = {}
    for col in spark_df.columns:
        distinct_val = spark_df.select(countDistinct(col).alias("distinct_count")).collect()[0]["distinct_count"]
        result[col] = distinct_val

    return result


# def get_distinct_counts(df):
#     return {col: df.select(col).distinct().count() for col in df.columns}
