# --- Temporary path injection ---
import sys, os
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
# --- End path injection ---

from project_bootstrap import bootstrap_project_paths
bootstrap_project_paths(__file__) 

from src.utils.config_loader import load_env_and_get
from MKM_Data_Profiling.profilers.run_common_profilers import run_common_profilers, sanitize_summary
from src.utils.path_utils import get_local_output_path  
from src.connections.db_connections import spark_session_for_JDBC
import json

load_env_and_get()
spark = spark_session_for_JDBC()

jdbc_url = f"jdbc:mysql://{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
props = {
    "user": os.getenv("DB_USERNAME"),
    "password": os.getenv("DB_PASSWORD"),
    "driver": "com.mysql.cj.jdbc.Driver"
}

try:
    df = spark.read.jdbc(url=jdbc_url, table="cart_item", properties=props)
    raw_summary = run_common_profilers(df, "cart_item")
    summary = sanitize_summary(raw_summary)

    output_path = os.path.join(get_local_output_path("profiling"), "cart_item_profile.json")
    with open(output_path, "w") as f:
        json.dump(summary, f, indent=2)

    print("[SUCCESS] Profiling complete: cart_item")
    print(f"[INFO] Output saved to: {output_path}")

finally:
    spark.stop()
