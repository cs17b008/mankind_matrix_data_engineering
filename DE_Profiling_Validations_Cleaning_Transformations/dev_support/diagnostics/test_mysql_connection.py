# --- TEMP: Add root to sys.path for module discovery ---
import os
import sys

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from project_bootstrap import bootstrap_project_paths
bootstrap_project_paths(__file__)
# --- END TEMP PATH PATCH ---

# Now safely import internal modules
from src.utils.config_loader import load_env_and_get
from src.connections.db_connections import spark_session_for_JDBC

# Load .env and test connection
load_env_and_get("DB_HOST")  # Trigger loading

spark = spark_session_for_JDBC()

try:
    test_df = spark.read \
        .format("jdbc") \
        .option("url", f"jdbc:mysql://{load_env_and_get('DB_HOST')}:{load_env_and_get('DB_PORT')}/{load_env_and_get('DB_NAME')}") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", "(SELECT 1) AS test") \
        .option("user", load_env_and_get("DB_USERNAME")) \
        .option("password", load_env_and_get("DB_PASSWORD")) \
        .load()

    test_df.show()
    print("✅ Database connection successful.")
except Exception as e:
    print("❌ Failed to connect to the database.")
    print(e)








# import mysql.connector

# try:
#     conn = mysql.connector.connect(
#         host="mankind-matrix-db.cd0qkick6gy2.us-west-1.rds.amazonaws.com",
#         port=3306,
#         database="mankind_matrix_db",
#         user="matrix_user",
#         password="matrix_pass"
#     )
#     print("✅ MySQL connection successful!")
#     conn.close()
# except mysql.connector.Error as err:
#     print("❌ MySQL connection failed:", err)


