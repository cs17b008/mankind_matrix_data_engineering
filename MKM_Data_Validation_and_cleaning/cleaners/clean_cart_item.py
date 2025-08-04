import sys, os
from pyspark.sql.functions import col, when, trim, lower, lit, current_date, datediff, isnan

# --- Path Injection ---
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, project_root)

from src.utils.config_loader import load_env_and_get
from src.utils.path_utils import get_local_output_path
from src.connections.db_connections import spark_session_for_JDBC
from MKM_Data_Validation_and_cleaning.metadata.schema_validator import validate_schema

# --- Init ---
load_env_and_get()
spark = spark_session_for_JDBC()

# --- JDBC Info ---
jdbc_url = f"jdbc:mysql://{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
props = {
    "user": os.getenv("DB_USERNAME"),
    "password": os.getenv("DB_PASSWORD"),
    "driver": "com.mysql.cj.jdbc.Driver"
}

try:
    # --- Read ---
    df = spark.read.jdbc(url=jdbc_url, table="cart_item", properties=props)
    validate_schema(df, table_name="cart_item")

    # --- Cleaning ---
    cleaned_df = (
        df
        .withColumn("product_id", trim(col("product_id")))
        .withColumn("cart_id", trim(col("cart_id")))
        .withColumn("status", lower(trim(col("status"))))

        .withColumn("quantity", when(col("quantity").isNull() | isnan(col("quantity")), lit(0)).otherwise(col("quantity")))

        .withColumn("quantity_flag", when(col("quantity") <= 0, lit("invalid"))
                                     .when(col("quantity") > 100, lit("too_high"))
                                     .otherwise(lit("ok")))

        .withColumn("price", trim(col("price")).cast("float"))

        .withColumn("cart_age_days", datediff(current_date(), col("last_updated")))
    )

    # --- Save ---
    output_path = os.path.join(get_local_output_path("cleaned_outputs"), "cart_item_cleaned.json")
    cleaned_df.coalesce(1).write.mode("overwrite").json(output_path)

    print("[SUCCESS] Cart item cleaning completed.")
    print(f"[INFO] Cleaned data saved at: {output_path}")

except Exception as e:
    print(f"[ERROR] Cart item cleaning failed: {e}")

finally:
    spark.stop()
