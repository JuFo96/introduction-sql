"""
Simple unit tests for CRUD class.

Install:
    uv add --dev pytest

Run:
    pytest tests/test_crud.py -v
"""

import pytest
from unittest.mock import Mock
from crud import CRUD


# Simple fixtures
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
    crud_instance = CRUD(table_name="orders_combined", connection=mock_conn)
    return crud_instance, mock_cursor, mock_conn


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

def test_insert_with_no_cols_specified(crud):
    """Test insert when cols is None (all columns)."""
    crud_instance, mock_cursor, mock_conn = crud
    
    values = [1, "2024-01-01", "John Doe", "john@example.com", "Widget", 19.99]
    
    # Call insert
    crud_instance.insert(values, cols=None)
    
    # Check that execute was called once
    mock_cursor.execute.assert_called_once()
    
    # Check the SQL string
    call_args = mock_cursor.execute.call_args[0]
    sql = call_args[0]
    params = call_args[1]
    
    assert "INSERT INTO orders_combined" in sql
    assert "VALUES (%s, %s, %s, %s, %s, %s)" in sql
    assert params == values
    
    # Check commit was called
    mock_conn.commit.assert_called_once()


def test_insert_with_specific_columns(crud):
    """Test insert with specific columns."""
    crud_instance, mock_cursor, mock_conn = crud
    
    cols = ["customer_name", "customer_email"]
    values = ["Jane Smith", "jane@example.com"]
    
    # Call insert
    crud_instance.insert(values, cols=cols)
    
    # Check execute was called
    mock_cursor.execute.assert_called_once()
    
    # Check the SQL
    call_args = mock_cursor.execute.call_args[0]
    sql = call_args[0]
    params = call_args[1]
    
    assert "INSERT INTO orders_combined" in sql
    assert "customer_name, customer_email" in sql
    assert "VALUES (%s, %s)" in sql
    assert params == values
    
    # Check commit was called
    mock_conn.commit.assert_called_once()


def test_insert_with_single_column(crud):
    """Test insert with just one column."""
    crud_instance, mock_cursor, mock_conn = crud
    
    cols = ["product_price"]
    values = [29.99]
    
    crud_instance.insert(values, cols=cols)
    
    call_args = mock_cursor.execute.call_args[0]
    sql = call_args[0]
    
    assert "product_price" in sql
    assert "VALUES (%s)" in sql


def test_insert_with_invalid_column_raises_error(crud):
    """Test that insert with invalid column raises ValueError."""
    crud_instance, mock_cursor, mock_conn = crud
    
    cols = ["invalid_column"]
    values = ["some value"]
    
    # Should raise ValueError before executing SQL
    with pytest.raises(ValueError, match="Invalid column"):
        crud_instance.insert(values, cols=cols)
    
    # Execute should NOT have been called
    mock_cursor.execute.assert_not_called()
    mock_conn.commit.assert_not_called()


def test_insert_multiple_rows_scenario(crud):
    """Test inserting multiple times (simulating multiple inserts)."""
    crud_instance, mock_cursor, mock_conn = crud
    
    # First insert
    crud_instance.insert(["John", "john@test.com"], cols=["customer_name", "customer_email"])
    
    # Second insert
    crud_instance.insert(["Jane", "jane@test.com"], cols=["customer_name", "customer_email"])
    
    # Check execute was called twice
    assert mock_cursor.execute.call_count == 2
    
    # Check commit was called twice
    assert mock_conn.commit.call_count == 2


# ============================================
# Tests for CRUD initialization
# ============================================

def test_crud_initialization():
    """Test that CRUD initializes correctly."""
    mock_conn = Mock()
    
    crud = CRUD(table_name="test_table", connection=mock_conn)
    
    assert crud.table_name == "test_table"
    assert crud.connection == mock_conn
    assert len(crud.valid_columns) == 6
    assert "customer_name" in crud.valid_columns


def test_valid_columns_list(crud):
    """Test that valid_columns contains expected columns."""
    crud_instance, _, _ = crud
    
    expected_cols = {"id", "date_time", "customer_name", "customer_email", "product_name", "product_price"}
    
    assert crud_instance.valid_columns == expected_cols


# ============================================
# Parametrized tests (testing multiple cases at once)
# ============================================

@pytest.mark.parametrize("columns", [
    ["id"],
    ["customer_name", "customer_email"],
    ["product_name", "product_price"],
    ["id", "date_time", "customer_name", "customer_email", "product_name", "product_price"],
])
def test_validate_all_valid_columns(crud, columns):
    """Test validation with different combinations of valid columns."""
    crud_instance, _, _ = crud
    
    # Should not raise any errors
    crud_instance.validate_columns(columns)


@pytest.mark.parametrize("invalid_column", [
    "invalid_col",
    "hacker_column",
    "drop_table",
    "'; DROP TABLE users; --",
])
def test_validate_rejects_invalid_columns(crud, invalid_column):
    """Test that various invalid columns are rejected."""
    crud_instance, _, _ = crud
    
    with pytest.raises(ValueError):
        crud_instance.validate_columns([invalid_column])