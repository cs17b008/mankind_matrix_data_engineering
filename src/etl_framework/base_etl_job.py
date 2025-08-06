import os
import json
import traceback
from datetime import datetime
from typing import Dict
from pyspark.sql import SparkSession, DataFrame
from dotenv import load_dotenv

class BaseETLJob:
    def __init__(self, job_name: str, required_tables: list, output_path: str):
        load_dotenv()
        self.job_name = job_name
        self.required_tables = required_tables
        self.output_path = output_path
        self.spark = SparkSession.builder                 .appName(job_name)                 .config("spark.jars", "file:///" + os.getenv("JDBC_DRIVER_PATH").replace('\\', '/'))                 .config("spark.driver.extraClassPath", "file:///" + os.getenv("JDBC_DRIVER_PATH").replace('\\', '/'))                 .getOrCreate()
        self.jdbc_url = f"jdbc:mysql://{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
        self.connection_props = {
            "user": os.getenv("DB_USERNAME"),
            "password": os.getenv("DB_PASSWORD"),
            "driver": "com.mysql.cj.jdbc.Driver"
        }

    def read_table(self, table_name: str) -> DataFrame:
        for attempt in range(3):
            try:
                df = self.spark.read.jdbc(url=self.jdbc_url, table=table_name, properties=self.connection_props)
                print(f"✅ Loaded table: {table_name}")
                return df
            except Exception as e:
                print(f"⚠️ Retry {attempt+1} for table {table_name} failed: {e}")
        raise Exception(f"❌ Failed to load table: {table_name} after 3 attempts")

    def load_all_tables(self) -> Dict[str, DataFrame]:
        tables = {}
        for table in self.required_tables:
            tables[table] = self.read_table(table)
        return tables

    def generate_validation_summary(self, df: DataFrame, run_path: str):
        summary = {
            "job": self.job_name,
            "record_count": df.count(),
            "columns": df.columns,
            "null_counts": {col: df.filter(df[col].isNull()).count() for col in df.columns},
            "generated_at": datetime.utcnow().isoformat(),
            "output_path": run_path
        }
        os.makedirs(run_path, exist_ok=True)
        with open(os.path.join(run_path, "validation_summary.json"), "w") as f:
            json.dump(summary, f, indent=2)
        print("📄 Validation summary saved.")

    def run(self):
        try:
            tables = self.load_all_tables()
            enriched_df = self.transform(tables)

            enriched_df = enriched_df.dropna(subset=["PRODUCT_ID"]).dropDuplicates()
            enriched_df = enriched_df.repartition(4)

            run_date = datetime.today().strftime('%Y-%m-%d')
            run_path = os.path.join(self.output_path, f"run_date={run_date}")
            enriched_df.write.mode("overwrite").parquet(run_path)

            self.generate_validation_summary(enriched_df, run_path)
            print(f"✅ Job '{self.job_name}' completed successfully.")

        except Exception as err:
            print(f"❌ Job '{self.job_name}' failed with error: {err}")
            traceback.print_exc()
            self.notify_failure(str(err))

    def notify_failure(self, message):
        print(f"🚨 ALERT: {self.job_name} failed — {message}")

    def transform(self, tables: Dict[str, DataFrame]) -> DataFrame:
        raise NotImplementedError("You must implement the transform() method in your job subclass.")