# src/connections/db_connections.py

import os
from pyspark.sql import SparkSession
from dotenv import get_key

def spark_session_for_JDBC(env_path=".env") -> SparkSession:
    """
    Creates a Spark session with JDBC driver configured from .env.
    Includes Windows-safe configs to avoid native I/O crashes.

    """
    jdbc_path = get_key(env_path, "JDBC_PATH")

    if not jdbc_path:
        raise ValueError("[ERROR] JDBC_PATH not found in .env")

    # Normalize path (especially for local dev)
    if not jdbc_path.lower().startswith("file:///"):
        jdbc_path = "file:///" + os.path.abspath(jdbc_path).replace("\\", "/")

    spark = (
        SparkSession.builder
        .appName("MKM_DB_Connections")
        .config("spark.jars", jdbc_path)
        .config("spark.driver.extraClassPath", jdbc_path)
        .config("spark.executor.extraClassPath", jdbc_path)

        # ✅ Windows-safe Spark configs to bypass NativeIO crash
        .config("spark.hadoop.io.native.lib.available", "false")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "1")
        .config("spark.sql.sources.commitProtocolClass", "org.apache.spark.sql.execution.datasources.SQLHadoopMapRedCommitProtocol")
        

        .getOrCreate()
    )

    print("[INFO] Spark session created with JDBC driver")
    return spark



# # To test the function, you can uncomment the following lines:
# if __name__ == "__main__":
#     spark_session_for_JDBC()