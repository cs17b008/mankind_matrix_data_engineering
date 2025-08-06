# MKM_Data_Profiling/profilers/all_common_profilers.py

import os
import json
import math
from datetime import datetime, date

# 👇 This file should never be run directly. It's a helper.
# No need for temporary path injection here.

from src.utils.config_loader import load_env_and_get
from src.connections.db_connections import spark_session_for_JDBC

from MKM_Data_Profiling.profilers.profilers_common.data_types import get_column_data_types
from MKM_Data_Profiling.profilers.profilers_common.null_counts import get_null_counts
from MKM_Data_Profiling.profilers.profilers_common.column_stats import get_column_stats
from MKM_Data_Profiling.profilers.profilers_common.distinct_counts import get_distinct_counts
from MKM_Data_Profiling.profilers.profilers_common.value_frequencies import get_value_frequencies


def profile_table(table_name):
    """
    For advanced usage: run full profiling from scratch by connecting and loading table.
    Usually prefer run_common_profilers(df, table_name) from another controller script.
    """
    spark = spark_session_for_JDBC()

    db_name = load_env_and_get("DB_NAME")
    full_table = f"{db_name}.{table_name}"

    print(f"[INFO] Running profiling for table: {full_table}")
    df = spark.read \
        .format("jdbc") \
        .option("url", f"jdbc:mysql://{load_env_and_get('DB_HOST')}:{load_env_and_get('DB_PORT')}/{db_name}") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", table_name) \
        .option("user", load_env_and_get("DB_USERNAME")) \
        .option("password", load_env_and_get("DB_PASSWORD")) \
        .load()

    result = run_common_profilers(df, table_name)
    spark.stop()
    return result


def run_common_profilers(df, table_name):
    """
    Shared profiling logic for Spark DataFrames.
    Call this from your controller script after loading the table.
    """
    return {
        "table": table_name,
        "timestamp": datetime.now().isoformat(),
        "data_types": get_column_data_types(df),
        "null_counts": get_null_counts(df),
        "column_stats": get_column_stats(df),
        "distinct_counts": get_distinct_counts(df),
        "value_frequencies": get_value_frequencies(df)
    }


def sanitize_summary(summary_dict):
    """
    Recursively sanitize summary to be JSON serializable.
    Converts datetime, NaN, and complex objects.
    """
    def clean_value(val):
        if val is None:
            return None
        elif isinstance(val, float) and math.isnan(val):
            return None
        elif isinstance(val, (datetime, date)):
            return val.isoformat()
        elif isinstance(val, dict):
            return {k: clean_value(v) for k, v in val.items()}
        elif isinstance(val, list):
            return [clean_value(v) for v in val]
        elif hasattr(val, "__dict__"):
            return clean_value(vars(val))
        else:
            try:
                json.dumps(val)
                return val
            except (TypeError, OverflowError):
                return str(val)

    return {k: clean_value(v) for k, v in summary_dict.items()}


def save_profiling_output(table_name, result_dict):
    """
    Save sanitized profiling summary to JSON file.
    """
    output_dir = os.path.join("reports", "profiling")
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"{table_name}_profile.json")

    with open(output_path, "w") as f:
        json.dump(result_dict, f, indent=2)
    print(f"[✅] Profiling output saved to: {output_path}")


# 🔒 Block direct execution — this file is only meant to be imported
if __name__ == "__main__":
    raise RuntimeError("[❌] This is a helper module and should not be run directly.")



    # if __name__ == "__main__":
    #     # For now, run a single example
    #     table = "products"
    #     result = profile_table(table)
    #     save_profiling_output(table, result)






    # from MKM_Data_Profiling.profilers.profilers_common.column_stats import get_column_stats
    # from MKM_Data_Profiling.profilers.profilers_common.null_counts import get_null_counts
    # from MKM_Data_Profiling.profilers.profilers_common.data_types import get_data_types
    # from MKM_Data_Profiling.profilers.profilers_common.distinct_counts import get_distinct_counts
    # from MKM_Data_Profiling.profilers.profilers_common.value_frequencies import get_value_frequencies



    # # Function to run common profilers on a DataFrame

    # def run_common_profilers(df, table_name):
    #     return {
    #         "table": table_name,
    #         "column_stats": get_column_stats(df),
    #         "null_counts": get_null_counts(df),
    #         "distinct_counts": get_distinct_counts(df),
    #         "data_types": get_data_types(df),
    #         "value_frequencies": get_value_frequencies(df)
    #     }

    # import math

    # def sanitize_summary(summary_dict):
    #     def clean_value(val):
    #         if isinstance(val, float) and math.isnan(val):
    #             return None
    #         elif isinstance(val, dict):
    #             return {k: clean_value(v) for k, v in val.items()}
    #         elif isinstance(val, list):
    #             return [clean_value(v) for v in val]
    #         return val

    #     return {k: clean_value(v) for k, v in summary_dict.items()}
