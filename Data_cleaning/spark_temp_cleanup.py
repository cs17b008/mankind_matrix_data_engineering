# spark_temp_cleanup.py
import os
import shutil
import glob
from datetime import datetime

def cleanup_spark_temp_dirs(days_old: int = 1):
    temp_dir = os.path.join(os.environ.get('TEMP', '/tmp'))  # Windows or Linux
    pattern = os.path.join(temp_dir, "spark-*")
    deleted = 0

    for folder in glob.glob(pattern):
        try:
            folder_mtime = datetime.fromtimestamp(os.path.getmtime(folder))
            age_days = (datetime.now() - folder_mtime).days
            if age_days >= days_old:
                shutil.rmtree(folder)
                print(f"[INFO] Deleted: {folder}")
                deleted += 1
        except Exception as e:
            print(f"[WARN] Could not delete: {folder} — {str(e)}")

    print(f"\n[SUMMARY] Total folders deleted: {deleted}")

if __name__ == "__main__":
    cleanup_spark_temp_dirs(days_old=1)
# This script cleans up Spark temporary directories older than a specified number of days.