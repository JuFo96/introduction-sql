import pytest
from src.crud import CRUD
from config import dbconfig
from mysql_db import DatabaseConnection

@pytest.fixture
def crud():
    """Fixture that creates an in-memory CRUD instance."""
    connection = DatabaseConnection(dbconfig)
    cursor = connection.cursor()

    # Create a simple table matching your valid columns
    cursor.execute("""
        CREATE TABLE sales (
            id INTEGER PRIMARY KEY,
            date_time TEXT,
            customer_name TEXT,
            customer_email TEXT,
            product_name TEXT,
            product_price REAL
        )
    """)
    connection.commit()

    crud_obj = CRUD("sales", connection)
    yield crud_obj

    connection.close()

def test_insert(crud):
    """Test inserting a row using CRUD.insert()."""
    data = [
        2, 2025, 3, 20, 11, 45, 43,
        "Jess Stanton", "jess.stanton@yahoo.com", "Mouse", "443.55"
    ]

    result = crud.insert(data)

    # You can verify behavior depending on your CRUD implementation:
    assert result is not None, "Insert returned None"
    
    # Check that it actually inserted
    rows = crud.connection.execute("SELECT * FROM sales").fetchall()
    assert len(rows) == 1, "No rows were inserted"

