# value_frequencies.py

from pyspark.sql.functions import col, count

def get_value_frequencies(spark_df, top_n=5):
    """
    Returns the top N most frequent values for each column in the DataFrame.

    Args:
        spark_df (pyspark.sql.DataFrame): The input Spark DataFrame.
        top_n (int): Number of top frequent values to return per column.

    Returns:
        dict: {
            column_name: [(value, count), ...],
            ...
        }
    """
    if not spark_df:
        raise ValueError("[ERROR] No DataFrame provided to get_value_frequencies")

    result = {}

    for column in spark_df.columns:
        try:
            freq_df = spark_df.groupBy(col(column)) \
                              .agg(count("*").alias("count")) \
                              .orderBy(col("count").desc()) \
                              .limit(top_n)

            result[column] = [(row[column], row["count"]) for row in freq_df.collect()]
        except Exception as e:
            result[column] = f"[ERROR] Failed to get value frequencies: {str(e)}"

    return result


# def get_value_frequencies(df, top_n=5):
#     result = {}
#     for col in df.columns:
#         # Skip TimestampType or problematic types if needed
#         try:
#             freq_df = (
#                 df.groupBy(col)
#                   .count()
#                   .orderBy("count", ascending=False)
#                   .limit(top_n)
#                   .toPandas()
#             )
#             result[col] = freq_df.to_dict(orient="records")
#         except Exception as e:
#             result[col] = f"[ERROR] Could not compute frequencies: {str(e)}"
#     return result
