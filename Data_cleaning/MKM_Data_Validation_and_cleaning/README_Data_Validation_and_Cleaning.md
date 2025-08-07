# ✅ MKM Data Validation and Cleaning

This folder manages all validation checks (pre & post) and cleaning rules for every table in the database.

---

## 📁 Folder Structure

```
MKM_Data_Validation_and_cleaning/
├── validators/
│   └── validate_<table>_pre.py      # Optional pre-cleaning checks
│   └── validate_<table>_post.py     # Post-cleaning validations
├── cleaners/
│   └── clean_<table>.py             # Cleans raw data using YAML rules
├── src/
│   ├── config/                      # master_schema_cleaning_rules.yaml lives here
│   └── utils/                       # Common logic (loaders, rules, renames, etc.)
├── reports/
│   └── latest_mysql_schema_summary.json
└── cleaned_outputs/
    └── <table>_cleaned.json
```

---

## 🧪 🔁 Sample Script: `validate_users_pre.py` (Optional Pre-Cleaning)

```python
# ✅ File: validators/validate_users_pre.py

from validators.validators_common.validation_checks import run_basic_schema_checks
from src.utils.project_bootstrap import get_project_root
from src.utils.db_connection import spark_session_for_JDBC
import os
from dotenv import load_dotenv

load_dotenv()
spark = spark_session_for_JDBC()
project_root = get_project_root()
table_name = "users"

# Step 1: Read data from MySQL
df = spark.read.format("jdbc") \
    .option("url", os.getenv("DB_URL")) \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", table_name) \
    .option("user", os.getenv("DB_USERNAME")) \
    .option("password", os.getenv("DB_PASSWORD")) \
    .load()

# Step 2: Run pre-clean validation
run_basic_schema_checks(df, table_name)
```

---

## 🧹 🔁 Sample Script: `clean_users.py`

```python
# ✅ File: cleaners/clean_users.py

from src.utils.cleaning_rules import clean_dataframe_with_rules, load_cleaning_config
from src.utils.db_connection import spark_session_for_JDBC
from src.utils.project_bootstrap import get_project_root
import os
from dotenv import load_dotenv

load_dotenv()
spark = spark_session_for_JDBC()
project_root = get_project_root()
table_name = "users"

# Step 1: Load cleaning config
rules = load_cleaning_config(table_name)

# Step 2: Load raw data from MySQL
df = spark.read.format("jdbc") \
    .option("url", os.getenv("DB_URL")) \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", table_name) \
    .option("user", os.getenv("DB_USERNAME")) \
    .option("password", os.getenv("DB_PASSWORD")) \
    .load()

# Step 3: Apply cleaning logic
cleaned_df = clean_dataframe_with_rules(df, rules)

# Step 4: Save cleaned output
output_path = os.path.join(project_root, "MKM_Data_Validation_and_cleaning", "cleaned_outputs", f"{table_name}_cleaned.json")
cleaned_df.write.mode("overwrite").json(output_path)
```

---

## ✅ 🔁 Sample Script: `validate_users_post.py`

```python
# ✅ File: validators/validate_users_post.py

from validators.validators_common.validation_checks import run_post_clean_validations
from src.utils.project_bootstrap import get_project_root
from src.utils.db_connection import spark_session_for_JDBC
import os
from dotenv import load_dotenv

load_dotenv()
spark = spark_session_for_JDBC()
project_root = get_project_root()
table_name = "users"

# Step 1: Load cleaned output
input_path = os.path.join(project_root, "MKM_Data_Validation_and_cleaning", "cleaned_outputs", f"{table_name}_cleaned.json")
df = spark.read.json(input_path)

# Step 2: Run post-cleaning validations
run_post_clean_validations(df, table_name)
```

---

## 🧠 Tips

- These scripts are modular and repeatable — just copy and rename for any other table.
- All logic is driven by the config file:
  ```
  MKM_Data_Validation_and_cleaning/src/config/master_schema_cleaning_rules.yaml
  ```

---

## 📤 Outputs

- Cleaned data: `cleaned_outputs/<table>_cleaned.json`
- Post validation results: usually printed or logged per table
