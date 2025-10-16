import pytest
from crud import CRUD, DatabaseConnection
from config import DatabaseConnectionConfig

@pytest.fixture
def db_connection():
    """Fixture to provide database connection for tests."""
    config = DatabaseConnectionConfig(
        host="127.0.0.1",
        port=3306,
        user="root",
        password="mypassword",
        database="test_db"
    )
    with DatabaseConnection(config) as conn:
        yield conn


@pytest.fixture
def crud(db_connection):
    """Fixture to provide CRUD instance."""
    return CRUD(connection=db_connection, table_name="test_orders")


@pytest.fixture(autouse=True)
def setup_teardown(db_connection):
    """Setup and teardown test table for each test."""
    # Setup: Create test table
    with db_connection.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS test_orders (
                id INT PRIMARY KEY,
                customer_name VARCHAR(255),
                customer_email VARCHAR(255),
                product_name VARCHAR(255)
            )
        """)
        db_connection.commit()
    
    yield
    
    # Teardown: Clean up test data
    with db_connection.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS test_orders")
        db_connection.commit()


def test_crud_full_workflow(crud):
    """Integration test for complete CRUD workflow."""
    
    # INSERT
    crud.insert(data={
        "id": 1,
        "customer_name": "egan",
        "customer_email": "egan@B.com",
        "product_name": "laptop"
    })
    
    # SELECT - verify insert
    results = crud.select(cols=["id", "customer_name", "customer_email"])
    assert len(results) == 1
    assert results[0][0] == 1
    assert results[0][1] == "egan"
    
    # INSERT more data
    crud.insert(data={
        "id": 2,
        "customer_name": "Jess Stanton",
        "customer_email": "jess@test.com",
        "product_name": "USB Drive"
    })
    
    # SELECT with limit
    results = crud.select(cols=["id", "customer_name"], limit=2)
    assert len(results) == 2
    
    # UPDATE
    crud.update(
        data={"customer_name": "nage", "product_name": "skateboard"},
        filters={"id": 1}
    )
    
    # SELECT with filter - verify update
    results = crud.select(
        cols=["id", "customer_name", "product_name"],
        filters={"id": 1}
    )
    assert results[0][1] == "nage"
    assert results[0][2] == "skateboard"
    
    # SELECT with different filter
    results = crud.select(
        cols=["customer_name", "product_name"],
        filters={"customer_name": "Jess Stanton"}
    )
    assert len(results) == 1
    assert results[0][1] == "USB Drive"
    
    # DELETE
    crud.delete(filters={"product_name": "USB Drive"})
    
    # SELECT - verify delete
    results = crud.select(
        cols=["id"],
        filters={"customer_name": "Jess Stanton"}
    )
    assert len(results) == 0
    
    # Final count check
    all_results = crud.select(cols=["id"])
    assert len(all_results) == 1  # Only egan's updated record remains