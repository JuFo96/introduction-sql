"""
Simple unit tests for CRUD class.

Install:
    uv add --dev pytest

Run:
    pytest tests/test_crud.py -v
"""

from unittest.mock import Mock

import pytest

from table import Table


@pytest.fixture
def mock_connection():
    """Create a fake database connection."""
    mock_conn = Mock()
    mock_cursor = Mock()
    
    # Make cursor work with 'with' statement
    mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = Mock(return_value=None)
    
    return mock_conn, mock_cursor


@pytest.fixture
def crud(mock_connection):
    """Create CRUD instance with fake connection."""
    mock_conn, mock_cursor = mock_connection
    crud_instance = Table(table_name="orders_combined", connection=mock_conn)
    return crud_instance, mock_cursor, mock_conn


# ============================================
# Tests for CRUD initialization
# ============================================

def test_crud_initialization():
    """Test that CRUD initializes correctly."""
    mock_conn = Mock()
    
    crud = Table(table_name="test_table", connection=mock_conn)
    
    assert crud.table_name == "test_table"
    assert crud.connection == mock_conn
    assert len(crud.valid_columns) == 12
    assert "customer_name" in crud.valid_columns

# ============================================
# Tests for validate_columns
# ============================================

def test_validate_columns_with_valid_columns(crud):
    """Test that valid columns don't raise errors."""
    crud_instance, _, _ = crud
    
    # These should not raise any errors
    crud_instance.validate_columns(["id", "customer_name"])
    crud_instance.validate_columns(["product_price"])
    crud_instance.validate_columns(["date_time", "customer_email"])


def test_validate_columns_with_invalid_column(crud):
    """Test that invalid columns raise ValueError."""
    crud_instance, _, _ = crud
    
    # This should raise ValueError
    with pytest.raises(ValueError, match="Invalid column: {'bad_column'}"):
        crud_instance.validate_columns({"bad_column"})


def test_validate_columns_with_mixed_columns(crud):
    """Test that mixed valid/invalid columns raise ValueError."""
    crud_instance, _, _ = crud
    
    # Should fail because of the invalid column
    with pytest.raises(ValueError):
        crud_instance.validate_columns(["id", "invalid_col", "customer_name"])


def test_validate_columns_with_empty_list(crud):
    """Test that empty list doesn't raise errors."""
    crud_instance, _, _ = crud
    
    # Should not raise any errors
    crud_instance.validate_columns([])


# ============================================
# Tests for insert method
# ============================================

def test_insert_with_all_cols(crud):
    """Test insert with all cols in dictinary."""
    crud_instance, mock_cursor, mock_conn = crud
    
    data = {"id": 1, "date_time": "2024-01-01", "customer_name": "egan", "customer_email": "egan@B.com", "product_name": "widget", "product_price": 19.99}
    
    # Call insert
    crud_instance.insert(data)
    
    # Check that execute was called once
    mock_cursor.execute.assert_called_once()
    
    # Check the SQL string
    call_args = mock_cursor.execute.call_args[0]
    sql = call_args[0]
    params = call_args[1]
    
    
    assert "INSERT INTO orders_combined" in sql
    assert "VALUES (%s, %s, %s, %s, %s, %s)" in sql
    assert "egan" in params
    assert params[0] == 1
    assert len(params) == 6
    # Check commit was called
    mock_conn.commit.assert_called_once()


def test_insert_with_specific_columns(crud):
    """Test insert with specific columns."""
    crud_instance, mock_cursor, mock_conn = crud
    
    data = {"customer_name": "Jane Smith", "customer_email": "jane@example.com"}

    
    # Call insert
    crud_instance.insert(data)
    
    # Check execute was called
    mock_cursor.execute.assert_called_once()
    
    # Check the SQL
    call_args = mock_cursor.execute.call_args[0]
    sql = call_args[0]
    params = call_args[1]
    
    assert "INSERT INTO orders_combined" in sql
    assert "customer_name, customer_email" in sql
    assert "VALUES (%s, %s)" in sql
    assert "Jane Smith" in params
    assert params[1] == "jane@example.com"
    
    # Check commit was called
    mock_conn.commit.assert_called_once()


def test_insert_with_invalid_column_raises_error(crud):
    """Test that insert with invalid column raises ValueError."""
    crud_instance, mock_cursor, mock_conn = crud
    
    data = {"DROP TABLE": "Jane Doe"}
    
    # Should raise ValueError before executing SQL
    with pytest.raises(ValueError, match="Invalid column"):
        crud_instance.insert(data)
    
    # Execute should NOT have been called
    mock_cursor.execute.assert_not_called()
    mock_conn.commit.assert_not_called()

def test_insert_with_empty_data_raises_error(crud):
    """Test that insert with no data doesn't execute and raises ValueError"""
    crud_instance, mock_cursor, mock_conn = crud

    data = {}

    with pytest.raises(ValueError, match="Cannot insert empty data dictionary"):
        crud_instance.insert(data)

    mock_cursor.execute.assert_not_called()
    mock_conn.commit.assert_not_called()

# ============================================
# Tests for read method
# ============================================

def test_select_returns_num_rows(crud):
    """Tests that select respects the num_rows parameter to return"""

    crud_instance, mock_cursor, mock_conn = crud
    mock_cursor.fetchall.return_value = [(1,), (2,), (3,), (4,)]

    data = ["id"]
    results = crud_instance.select(data, limit=4)

    mock_cursor.fetchall.assert_called_once_with()
    assert len(results) == 4

def test_select_returns_empty_result(crud):
    """Tests that selects returns empty result"""

    crud_instance, mock_cursor, mock_conn = crud
    mock_cursor.fetchall.return_value = []

    data = ["id"]
    results = crud_instance.select(data)

    mock_cursor.fetchall.assert_called_once_with()
    assert len(results) == 0

def test_select_returns_all_result(crud):
    """Tests that selects returns empty result"""

    crud_instance, mock_cursor, mock_conn = crud
    mock_cursor.fetchall.return_value = [(1, "name"), (2, "name"), (3, "name"), (4, "name")]

    data = ["id", "customer_name"]
    results = crud_instance.select(data)

    mock_cursor.fetchall.assert_called_once_with()
    assert len(results) == 4

def test_select_with_all_options(crud):
    """Full featured case - tests everything together."""
    crud_instance, mock_cursor, _ = crud
    mock_cursor.fetchall.return_value = []
    
    crud_instance.select(
        ["id", "customer_name"],
        filters={"customer_name": "name"},
        limit=2
    )
    
    sql_string = mock_cursor.execute.call_args[0][0]
    params = mock_cursor.execute.call_args[0][1]
    
    assert sql_string == "SELECT id, customer_name FROM orders_combined WHERE customer_name = %s LIMIT %s"
    assert params == ["name", 2]
    
    


# ============================================
# Tests for update method
# ============================================


# ============================================
# Tests for delete method
# ============================================