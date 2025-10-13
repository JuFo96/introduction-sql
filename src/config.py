from pathlib import Path

# Directories
BASE_DIR = Path(__file__).parent.parent.resolve()
DATA_DIR = BASE_DIR / "data"
TEST_DIR = BASE_DIR / "tests"
SQL_DIR  = BASE_DIR / "sql"
DATA_DIR.mkdir(exist_ok=True, parents=True)
TEST_DIR.mkdir(exist_ok=True, parents=True)
SQL_DIR.mkdir(exist_ok=True, parents=True)

# Data Files
CUSTOMERS_CSV = DATA_DIR / "customers.csv"
ORDERS_CSV = DATA_DIR / "orders.csv"
PRODUCTS_CSV = DATA_DIR / "products.csv"
COMBINED_CSV = DATA_DIR / "orders_combined.csv"
CREATE_DB = BASE_DIR / SQL_DIR / "create_db.sql"
CREATE_ORDERS_COMBINED = BASE_DIR / SQL_DIR / "create_orders_combined.sql"

# DB
DB_NAME = "dbs"