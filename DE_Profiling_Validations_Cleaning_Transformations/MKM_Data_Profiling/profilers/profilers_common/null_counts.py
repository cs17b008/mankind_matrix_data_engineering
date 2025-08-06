# MKM_Data_Profiling/profilers/profilers_common/null_counts.py
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum as spark_sum, when

def get_null_counts(df: DataFrame) -> dict:
    """
    Returns a dictionary with the count of nulls per column.
    """
    null_counts = (
        df.select([
            spark_sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
            for c in df.columns
        ])
        .first()
        .asDict()
    )
    return {"null_counts": null_counts}


# def get_null_counts(df):
#     null_exprs = [
#         _sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
#         for c in df.columns
#     ]
#     null_counts_row = df.agg(*null_exprs).collect()[0].asDict()
#     return null_counts_row
