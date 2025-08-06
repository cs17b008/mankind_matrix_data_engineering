import sys
import os

# ✅ Dynamically set path for both src/ and validators/
current_file_path = os.path.abspath(__file__)
validation_root = os.path.abspath(os.path.join(current_file_path, "..", ".."))
sys.path.insert(0, validation_root)

project_root = os.path.abspath(os.path.join(validation_root, ".."))
sys.path.insert(0, project_root)

# ✅ Imports
from src.connections.db_connections import spark_session_for_JDBC
from validators.validators_common import validation_checks as vc
from pyspark.sql.functions import col

# ✅ Start Spark session
spark = spark_session_for_JDBC()

# ✅ Load cleaned users JSON
cleaned_path = "MKM_Data_Validation_and_cleaning/cleaned_outputs/dummy_cleaned_users.json"
df = spark.read.option("multiline", True).json(cleaned_path)

# ✅ Run validations
df = vc.check_nulls(df, "id")
df = vc.check_positive(df, "id")

df = vc.check_non_empty_string(df, "email")
df = vc.check_non_empty_string(df, "username")
df = vc.check_non_empty_string(df, "first_name")
df = vc.check_non_empty_string(df, "last_name")
df = vc.check_non_empty_string(df, "password")

df = vc.check_allowed_values(df, "active", [True, False])
df = vc.check_allowed_values(df, "role", ["admin", "user", "guest"])

df = vc.check_timestamp_castable(df, "create_time")
df = vc.check_timestamp_castable(df, "update_time")

# ✅ Collect invalid rows
invalid_df = df.filter(
    (~col("id_not_null")) |
    (~col("id_positive")) |
    (~col("email_non_empty")) |
    (~col("username_non_empty")) |
    (~col("first_name_non_empty")) |
    (~col("last_name_non_empty")) |
    (~col("password_non_empty")) |
    (~col("active_allowed")) |
    (~col("role_allowed")) |
    (~col("create_time_is_timestamp")) |
    (~col("update_time_is_timestamp"))
)

# ✅ Write or report
output_path = "MKM_Data_Validation_and_cleaning/reports/validation_reports/users_post_validation_failed.json"

if invalid_df.count() == 0:
    print("[✅] No validation errors found in cleaned users dataset.")
else:
    invalid_df.write.mode("overwrite").json(output_path)
    print(f"[✅] Post-cleaning validation report saved to: {output_path}")
