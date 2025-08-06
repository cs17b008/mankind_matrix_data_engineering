# src/utils/cleaning_rules.py

import os
import yaml
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import *

# Supported Spark types
TYPE_MAP = {
    "StringType": StringType(),
    "IntegerType": IntegerType(),
    "DoubleType": DoubleType(),
    "LongType": LongType(),
    "BooleanType": BooleanType(),
    "TimestampType": TimestampType(),
}

# ✅ Cleaning logic — modular, reusable
def clean_dataframe_with_rules(df: DataFrame, rules: dict) -> DataFrame:
    if not rules:
        return df
    cleaned_df = df

    # Rename columns
    rename_map = rules.get("rename_columns", {})
    for old_col, new_col in rename_map.items():
        if old_col in cleaned_df.columns:
            cleaned_df = cleaned_df.withColumnRenamed(old_col, new_col)

    # Type conversions
    type_map = rules.get("type_conversions", {})
    for col, target_type in type_map.items():
        if col in cleaned_df.columns:
            spark_type = TYPE_MAP.get(target_type)
            if spark_type:
                cleaned_df = cleaned_df.withColumn(col, cleaned_df[col].cast(spark_type))

    # Null replacements
    null_map = rules.get("null_replacements", {})
    for col, replacement in null_map.items():
        if col in cleaned_df.columns:
            cleaned_df = cleaned_df.withColumn(
                col, F.when(F.col(col).isNull(), F.lit(replacement)).otherwise(F.col(col))
            )

    # Case formatting
    case_rules = rules.get("case_formatting", {})
    for col, fmt in case_rules.items():
        if col in cleaned_df.columns:
            if fmt == "lower":
                cleaned_df = cleaned_df.withColumn(col, F.lower(F.col(col)))
            elif fmt == "upper":
                cleaned_df = cleaned_df.withColumn(col, F.upper(F.col(col)))
            elif fmt == "capitalize":
                cleaned_df = cleaned_df.withColumn(col, F.initcap(F.col(col)))

    # Unit standardization
    unit_rules = rules.get("standardize_units", {})
    for col, replacement in unit_rules.items():
        if col in cleaned_df.columns:
            cleaned_df = cleaned_df.withColumn(col, F.regexp_replace(F.col(col), r"\s+", ""))
            cleaned_df = cleaned_df.withColumn(col, F.regexp_replace(F.col(col), replacement["from"], replacement["to"]))

    return cleaned_df

# ✅ Config loader (newly added)
def load_cleaning_config(table_name: str, config_path: str = "src/config/master_schema_cleaning_rules.yaml") -> dict:
    with open(config_path, "r", encoding="utf-8") as f:
        all_rules = yaml.safe_load(f)
    return all_rules.get(table_name, {})















# import os
# import yaml
# from pyspark.sql import DataFrame
# from pyspark.sql.functions import col, when

# from src.utils.path_utils import get_project_root

# def clean_dataframe_with_rules(df: DataFrame, table_name: str) -> DataFrame:
#     # Step 1: Load the YAML cleaning rules
#     project_root = get_project_root()
#     config_path = os.path.join(project_root, "src", "config", "master_schema_cleaning_rules.yaml")

#     print(f"[DEBUG] Project root: {project_root}")
#     print(f"[DEBUG] Full config path: {config_path}")

#     if not os.path.exists(config_path):
#         raise FileNotFoundError(f"Cleaning rules YAML not found at: {config_path}")

#     with open(config_path, "r") as f:
#         all_rules = yaml.safe_load(f)

#     # Step 2: Apply rules specific to the table
#     rules = all_rules.get(table_name, {})
#     cleaned_df = df

#     # Rename columns
#     rename_map = rules.get("rename_columns", {})
#     for old_name, new_name in rename_map.items():
#         if old_name in cleaned_df.columns:
#             print(f"[RENAME] {old_name} → {new_name}")
#             cleaned_df = cleaned_df.withColumnRenamed(old_name, new_name)

#     # Replace nulls
#     null_replacements = rules.get("null_replacements", {})
#     for col_name, replacement in null_replacements.items():
#         if col_name in cleaned_df.columns:
#             print(f"[NULL REPLACE] {col_name} → {replacement}")
#             cleaned_df = cleaned_df.withColumn(col_name, when(col(col_name).isNull(), replacement).otherwise(col(col_name)))

#     # Standardize units (example: converting cm to m, °F to °C, etc.)
#     unit_standards = rules.get("unit_standardization", {})
#     for col_name, conversion in unit_standards.items():
#         if col_name in cleaned_df.columns:
#             factor = conversion.get("factor", 1)
#             offset = conversion.get("offset", 0)
#             print(f"[UNIT STANDARDIZE] {col_name}: apply (value * {factor}) + {offset}")
#             cleaned_df = cleaned_df.withColumn(col_name, (col(col_name) * factor + offset))

#     # Cast data types
#     type_casts = rules.get("cast_types", {})
#     for col_name, new_type in type_casts.items():
#         if col_name in cleaned_df.columns:
#             print(f"[CAST] {col_name} → {new_type}")
#             cleaned_df = cleaned_df.withColumn(col_name, col(col_name).cast(new_type))

#     return cleaned_df
