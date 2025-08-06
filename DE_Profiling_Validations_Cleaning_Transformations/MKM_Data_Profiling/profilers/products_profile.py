# MKM_Data_Profiling/profilers/products_profile.py

import sys
import os
import json

# --- Temporary path injection ---
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from project_bootstrap import bootstrap_project_paths
bootstrap_project_paths(__file__)
# --- End path injection ---

from src.connections.db_connections import spark_session_for_JDBC
from src.utils.config_loader import load_env_and_get
from src.utils.path_utils import get_local_output_path
from MKM_Data_Profiling.profilers.all_common_profilers import run_common_profilers, sanitize_summary


def profile_products_table():
    # Load environment variables
    load_env_and_get()

    # Start Spark session with JDBC support
    spark = spark_session_for_JDBC()

    jdbc_url = f"jdbc:mysql://{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    props = {
        "user": os.getenv("DB_USERNAME"),
        "password": os.getenv("DB_PASSWORD"),
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    try:
        # Step 1: Read the `products` table
        df = spark.read.jdbc(url=jdbc_url, table="products", properties=props)
        print("[INFO] Loaded 'products' table")

        # Step 2: Run reusable profilers
        raw_summary = run_common_profilers(df, table_name="products")

        # Step 3: Sanitize complex datatypes for JSON compatibility
        summary = sanitize_summary(raw_summary)

        # Step 4: Determine output path
        output_dir = get_local_output_path("profiling_reports/profiling")
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, "products_profile.json")

        # Step 5: Save as JSON
        with open(output_path, "w") as f:
            json.dump(summary, f, indent=2)
        print(f"[SUCCESS] Profiling report saved to: {output_path}")

    except Exception as e:
        print(f"[ERROR] Profiling failed for products: {e}")

    finally:
        spark.stop()
        print("[INFO] Spark session stopped.")


if __name__ == "__main__":
    profile_products_table()
