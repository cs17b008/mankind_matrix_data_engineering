# MKM_Data_Profiling/profilers/profilers_common/column_stats.py

from pyspark.sql.functions import col
from pyspark.sql.types import NumericType

def safe_float(val):
    try:
        return float(val) if val is not None else None
    except:
        return None

def get_column_stats(spark_df):
    """
    Returns summary statistics (min, max, avg, stddev, etc.) for numeric columns in the DataFrame.
    """
    if not spark_df:
        raise ValueError("[ERROR] No DataFrame provided to get_column_stats")

    result = {}
    numeric_cols = [f.name for f in spark_df.schema.fields if isinstance(f.dataType, NumericType)]

    if not numeric_cols:
        return result

    stats_df = spark_df.select([col(c) for c in numeric_cols]).summary("count", "mean", "stddev", "min", "max")
    summary_dict = {row["summary"]: row.asDict() for row in stats_df.collect()}

    for col_name in numeric_cols:
        result[col_name] = {
            "count": safe_float(summary_dict.get("count", {}).get(col_name)),
            "mean": safe_float(summary_dict.get("mean", {}).get(col_name)),
            "stddev": safe_float(summary_dict.get("stddev", {}).get(col_name)),
            "min": safe_float(summary_dict.get("min", {}).get(col_name)),
            "max": safe_float(summary_dict.get("max", {}).get(col_name)),
        }

    return result



# def get_column_stats(df):
#     numeric_cols = [f.name for f in df.schema.fields if str(f.dataType) in ['IntegerType', 'LongType', 'DoubleType', 'FloatType', 'DecimalType']]
#     return df.select(numeric_cols).summary("count", "min", "max", "mean", "stddev").toPandas().to_dict(orient="list")

