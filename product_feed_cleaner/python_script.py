import pandas as pd
import re
import os
from typing import Tuple

# File paths
INPUT_FILE = 'Product_Feed_Validator_Task_List.xlsx'
CLEAN_OUTPUT_FILE = 'cleaned_task_list.xlsx'
ERROR_OUTPUT_FILE = 'error_task_list.xlsx'
REQUIRED_COLUMNS = ["Task ID", "Task Name", "Description", "Owner", "Estimated Time"]

def load_excel(file_path: str) -> pd.DataFrame:
    return pd.read_excel(file_path)

def validate_row(row: pd.Series) -> Tuple[bool, str]:
    issues = []

    for col in REQUIRED_COLUMNS:
        value = str(row.get(col, "")).strip()
        if not value or value.lower() == 'nan':
            issues.append(f"Missing {col}")

    # Check Estimated Time format (e.g., 2h, 3 h)
    time_str = str(row.get("Estimated Time", "")).strip().lower()
    if not re.fullmatch(r"\d+\s*h", time_str):
        issues.append("Invalid Estimated Time format (expected e.g., '2h')")

    return (len(issues) == 0, "; ".join(issues))

def separate_valid_invalid(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    valid_rows, invalid_rows = [], []

    for _, row in df.iterrows():
        is_valid, issue = validate_row(row)
        if is_valid:
            valid_rows.append(row)
        else:
            row_copy = row.copy()
            row_copy["Issues"] = issue
            invalid_rows.append(row_copy)

    return pd.DataFrame(valid_rows), pd.DataFrame(invalid_rows)

def safe_save_excel(df: pd.DataFrame, file_path: str):
    # If the file exists and is open, we avoid the error by removing first
    if os.path.exists(file_path):
        try:
            os.remove(file_path)
        except PermissionError:
            print(f"⚠️ Please close '{file_path}' and run the script again.")
            return

    with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
        df.to_excel(writer, index=False)

def main():
    df = load_excel(INPUT_FILE)
    cleaned_df, error_df = separate_valid_invalid(df)

    safe_save_excel(cleaned_df, CLEAN_OUTPUT_FILE)
    print(f"✅ Cleaned data saved to '{CLEAN_OUTPUT_FILE}'")

    if not error_df.empty:
        safe_save_excel(error_df, ERROR_OUTPUT_FILE)
        print(f"⚠️ Invalid data saved to '{ERROR_OUTPUT_FILE}'")

if __name__ == "__main__":
    main()