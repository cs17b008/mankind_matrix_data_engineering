# clean_cart_item.py

import os
import sys
import json
from datetime import datetime

# --- Path setup ---
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from project_bootstrap import bootstrap_project_paths
bootstrap_project_paths(__file__)

from src.utils.config_loader import load_env_and_get
from src.connections.db_connections import spark_session_for_JDBC
from src.utils.path_utils import cleaning_output_paths, get_project_root

# --- Bookmark config ---
BOOKMARK_FILE = os.path.join(get_project_root(), "MKM_Data_Bookmarks", "cart_item_bookmark.json")
DEFAULT_TIMESTAMP = "2000-01-01T00:00:00"

def load_bookmark():
    if not os.path.exists(BOOKMARK_FILE):
        return DEFAULT_TIMESTAMP
    with open(BOOKMARK_FILE, "r") as f:
        return json.load(f).get("last_processed_ts", DEFAULT_TIMESTAMP)

def update_bookmark(new_ts):
    os.makedirs(os.path.dirname(BOOKMARK_FILE), exist_ok=True)
    with open(BOOKMARK_FILE, "w") as f:
        json.dump({"last_processed_ts": new_ts}, f, indent=2)

def clean_cart_item():
    load_env_and_get()
    spark = spark_session_for_JDBC()

    jdbc_url = f"jdbc:mysql://{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    props = {"user": os.getenv("DB_USERNAME"), "password": os.getenv("DB_PASSWORD"), "driver": "com.mysql.cj.jdbc.Driver"}

    try:
        # Load bookmark
        last_ts = load_bookmark()
        print(f"[INFO] Last processed timestamp: {last_ts}")

        # Load only new/updated rows
        df = spark.read.jdbc(url=jdbc_url, table="cart_item", properties=props)
        df_filtered = df.filter(f"updated_at > '{last_ts}'")

        print(f"[INFO] Loaded {df_filtered.count()} new rows from 'cart_item'")

        # Clean: Remove negative values
        df_cleaned = (
            df_filtered
            .filter("quantity >= 0 AND price >= 0")
            .na.drop(subset=["price", "quantity", "cart_id"])
        )

        # Determine latest timestamp for bookmark update
        new_max_ts = df_cleaned.agg({"updated_at": "max"}).collect()[0][0]
        new_max_ts_str = new_max_ts.strftime("%Y-%m-%dT%H:%M:%S") if new_max_ts else last_ts

        # Write cleaned output
        batch_date = datetime.today().strftime("%Y-%m-%d")
        output_path = cleaning_output_paths(f"cart_item_cleaned_{batch_date}", "json")

        df_cleaned.coalesce(1).write.mode("overwrite").json(output_path)
        print(f"[SUCCESS] Cleaned data written to: {output_path}")

        # Update bookmark
        update_bookmark(new_max_ts_str)
        print(f"[INFO] Bookmark updated to: {new_max_ts_str}")

    except Exception as e:
        print(f"[ERROR] Cleaning failed: {e}")
    finally:
        spark.stop()
        print("[INFO] Spark session stopped.")

if __name__ == "__main__":
    clean_cart_item()










# import sys, os
# from pyspark.sql.functions import col, when, trim, lower, lit, current_date, datediff, isnan

# # --- Path Injection ---
# project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
# sys.path.insert(0, project_root)

# from src.utils.config_loader import load_env_and_get
# from src.utils.path_utils import get_local_output_path
# from src.connections.db_connections import spark_session_for_JDBC
# from MKM_Data_Validation_and_cleaning.metadata.schema_validator import validate_schema

# # --- Init ---
# load_env_and_get()
# spark = spark_session_for_JDBC()

# # --- JDBC Info ---
# jdbc_url = f"jdbc:mysql://{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
# props = {
#     "user": os.getenv("DB_USERNAME"),
#     "password": os.getenv("DB_PASSWORD"),
#     "driver": "com.mysql.cj.jdbc.Driver"
# }

# try:
#     # --- Read ---
#     df = spark.read.jdbc(url=jdbc_url, table="cart_item", properties=props)
#     validate_schema(df, table_name="cart_item")

#     # --- Cleaning ---
#     cleaned_df = (
#         df
#         .withColumn("product_id", trim(col("product_id")))
#         .withColumn("cart_id", trim(col("cart_id")))
#         .withColumn("status", lower(trim(col("status"))))

#         .withColumn("quantity", when(col("quantity").isNull() | isnan(col("quantity")), lit(0)).otherwise(col("quantity")))

#         .withColumn("quantity_flag", when(col("quantity") <= 0, lit("invalid"))
#                                      .when(col("quantity") > 100, lit("too_high"))
#                                      .otherwise(lit("ok")))

#         .withColumn("price", trim(col("price")).cast("float"))

#         .withColumn("cart_age_days", datediff(current_date(), col("last_updated")))
#     )

#     # --- Save ---
#     output_path = os.path.join(get_local_output_path("cleaned_outputs"), "cart_item_cleaned.json")
#     cleaned_df.coalesce(1).write.mode("overwrite").json(output_path)

#     print("[SUCCESS] Cart item cleaning completed.")
#     print(f"[INFO] Cleaned data saved at: {output_path}")

# except Exception as e:
#     print(f"[ERROR] Cart item cleaning failed: {e}")

# finally:
#     spark.stop()
