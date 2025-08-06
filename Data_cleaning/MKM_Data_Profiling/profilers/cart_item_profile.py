# MKM_Data_Profiling/profilers/cart_item_profile.py

import os
import sys
import json

# --- Project path bootstrapping ---
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from project_bootstrap import bootstrap_project_paths
bootstrap_project_paths(__file__)
# --- End bootstrapping ---

# ✅ Imports
from src.utils.config_loader import load_env_and_get
from src.utils.path_utils import get_local_output_path
from src.connections.db_connections import spark_session_for_JDBC
from MKM_Data_Profiling.profilers.all_common_profilers import run_common_profilers, sanitize_summary

def profile_cart_item_table():
    # Step 1: Load environment
    load_env_and_get()

    # Step 2: Spark session
    spark = spark_session_for_JDBC()

    jdbc_url = f"jdbc:mysql://{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    props = {
        "user": os.getenv("DB_USERNAME"),
        "password": os.getenv("DB_PASSWORD"),
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    try:
        # Step 3: Load cart_item table
        df = spark.read.jdbc(url=jdbc_url, table="cart_item", properties=props)
        print("[INFO] Loaded 'cart_item' table")

        # Step 4: Run common profilers
        raw_summary = run_common_profilers(df, table_name="cart_item")

        # Step 5: Clean/flatten JSON
        summary = sanitize_summary(raw_summary)

        # Step 6: Output path
        output_dir = get_local_output_path("profiling_reports", "profiling")
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, "cart_item_profile.json")
        print(f"[INFO] Saving profiling report to: {output_path}")

        with open(output_path, "w") as f:
            json.dump(summary, f, indent=2)

        print(f"[SUCCESS] Profiling complete: cart_item")
        print(f"[INFO] Output saved to: {output_path}")

    except Exception as e:
        print(f"[ERROR] Failed profiling 'cart_item': {e}")

    finally:
        spark.stop()
        print("[INFO] Spark session stopped.")

# Entry point
if __name__ == "__main__":
    profile_cart_item_table()
