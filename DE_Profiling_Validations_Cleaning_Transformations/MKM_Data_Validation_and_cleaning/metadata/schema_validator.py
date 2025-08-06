import os
import yaml

def validate_schema(df, table_name, schema_folder="MKM_Data_Validation_and_cleaning/metadata/expected_schemas"):
    """
    Validates the schema of the given Spark DataFrame against an expected schema defined in a YAML file.

    Parameters:
    - df: Spark DataFrame to validate.
    - table_name: Name of the table (used to locate the YAML file).
    - schema_folder: Folder path containing the schema YAML files.
    """

    schema_file = os.path.join(schema_folder, f"{table_name}_schema.yaml")

    if not os.path.exists(schema_file):
        raise FileNotFoundError(f"[ERROR] Schema file not found: {schema_file}")

    with open(schema_file, "r") as f:
        schema = yaml.safe_load(f)

    required = schema.get("required", [])
    optional = schema.get("optional", [])   
    disallowed = schema.get("disallowed", [])

    df_columns = set(df.columns)

    # Check for required columns
    missing_required = [col for col in required if col not in df_columns]

    # Check for disallowed columns
    present_disallowed = [col for col in disallowed if col in df_columns]

    if missing_required:
        raise ValueError(f"[SCHEMA ERROR] Missing required columns: {missing_required}")

    if present_disallowed:
        raise ValueError(f"[SCHEMA ERROR] Disallowed columns present: {present_disallowed}")
    
    # --- Extra: Detect new/unexpected columns not listed in YAML ---
    expected = set(required + optional + disallowed)
    unexpected_cols = df_columns - expected

    if unexpected_cols:
        print(f"[SCHEMA NOTICE] New/unexpected columns detected: {sorted(unexpected_cols)}")

    print(f"[SCHEMA VALIDATION] ✅ {table_name}: PASSED")
