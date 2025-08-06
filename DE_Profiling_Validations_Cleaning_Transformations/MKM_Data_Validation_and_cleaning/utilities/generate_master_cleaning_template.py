import os
import sys
import json
import yaml

# --- Path injection ---
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# --- Imports ---
from src.utils.path_utils import get_local_output_path

# --- Constants ---
SCHEMA_JSON_PATH = os.path.join("MKM_Data_Validation_and_cleaning", "reports", "latest_mysql_schema_summary.json")
PROFILING_DIR = os.path.join("MKM_Data_Profiling", "profiling_reports", "profiling")
OUTPUT_YAML_PATH = os.path.join("src", "config", "master_schema_cleaning_rules.yaml")


def enrich_with_profiling(table_name, schema_rules):
    profile_path = os.path.join(PROFILING_DIR, f"{table_name}_profile.json")
    if not os.path.exists(profile_path):
        print(f"⚠️ Skipping profiling for: {table_name}")
        return schema_rules

    with open(profile_path, "r") as f:
        profile = json.load(f)

    enriched = schema_rules.copy()

    # --- Preserve existing type_conversions (e.g., TimestampType)
    enriched.setdefault("type_conversions", {})
    type_conversions = enriched["type_conversions"]

    # --- Rename "id" to table-specific name if not already done
    enriched.setdefault("rename_columns", {})
    if "id" in profile.get("data_types", {}) and "id" not in enriched["rename_columns"]:
        enriched["rename_columns"]["id"] = f"{table_name}_id"

    # --- Null Replacements ---
    enriched["null_replacements"] = {}
    null_counts = profile.get("null_counts", {})
    row_count = profile.get("row_count", 1)

    for col, val in null_counts.items():
        nulls = val.get("nulls", val) if isinstance(val, dict) else val
        try:
            null_pct = (nulls / row_count) * 100
            if null_pct > 50.0:
                enriched["null_replacements"][col] = "UNKNOWN"
        except Exception as e:
            print(f"[WARN] Failed null_pct calc for {col}: {e}")

    # --- Boolean Detection (ONLY if not already set)
    freq_data = profile.get("value_frequencies", {})
    for col, freqs in freq_data.items():
        if col in type_conversions:
            continue
        if isinstance(freqs, list):
            values = [item.get("value") for item in freqs if isinstance(item, dict) and "value" in item]
            str_vals = {str(v).strip().lower() for v in values}
            if str_vals and str_vals.issubset({"0", "1", "true", "false", "yes", "no"}):
                type_conversions[col] = "BooleanType"

    # --- Default other fields ---
    enriched.setdefault("standardize_units", {})
    enriched.setdefault("case_formatting", {})

    return enriched


# --- Main function ---
def main():
    print("📄 Generating YAML cleaning rules from JSON summary and profiling...")

    if not os.path.exists(SCHEMA_JSON_PATH):
        raise FileNotFoundError(f"❌ Schema summary not found: {SCHEMA_JSON_PATH}")

    with open(SCHEMA_JSON_PATH, "r") as f:
        schema_summary = json.load(f)

    enriched_rules = {}

    for table_name, rules in schema_summary.get("tables", {}).items():
        print(f"🔍 Processing table: {table_name}")
        enriched_rules[table_name] = enrich_with_profiling(table_name, rules)

    yaml_obj = {
        "version": schema_summary.get("version", "v1"),
        "generated_at": schema_summary.get("generated_at"),
        "tables": enriched_rules
    }

    os.makedirs(os.path.dirname(OUTPUT_YAML_PATH), exist_ok=True)
    with open(OUTPUT_YAML_PATH, "w") as f:
        yaml.dump(yaml_obj, f, sort_keys=False)

    print(f"\n✅ Cleaning YAML created: {OUTPUT_YAML_PATH}")


if __name__ == "__main__":
    main()
