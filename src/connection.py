# Inspired by https://stackoverflow.com/questions/38076220/python-mysqldb-connection-in-a-class

from contextlib import contextmanager
from dataclasses import asdict

import mysql.connector

from config import DatabaseConnectionConfig


class DatabaseConnection:
    def __init__(self, config: DatabaseConnectionConfig) -> None:
        """Initialize DatabaseConnection connection.

        Args:
            config: DatabaseConnectionConfig object with connection parameters.

        Raises:
            mysql.connector.Error: If connection fails.
        """
        self.config = config
        config_dict = asdict(self.config)
        self.connection = mysql.connector.connect(**config_dict)

    def __enter__(self) -> "DatabaseConnection":
        """Enter context manager.

        Returns:
            Self for use with statement.
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        """Exit context manager and close connection automatic commits if there's no exceptions and rollbacks to last commit if there were.

        Args:
            exc_type: Exception type if an exception occurred.
            exc_val: Exception value if an exception occurred.
            exc_tb: Exception traceback if an exception occurred.

        Returns:
            False to propagate any exception that occurred.
        """
        if exc_type is None:
            self.commit()
        else:
            self.connection.rollback()
        self.connection.close()
        return False

    def is_connected(self) -> bool:
        """Check if DatabaseConnection connection is active.

        Returns:
            True if connected, False otherwise.
        """
        return self.connection is not None and self.connection.is_connected()

    @contextmanager
    def cursor(self, buffered: bool = True):
        """Create a cursor with automatic cleanup.

        This context manager ensures cursors are properly closed after use,
        preventing "Commands out of sync" errors.

        Args:
            buffered: If True, fetch all results immediately. Defaults to True
                to prevent synchronization issues.

        Yields:
            MySQLCursor: DatabaseConnection cursor for executing queries.

        Raises:
            RuntimeError: If connection is not established.

        Example:
            with db.cursor() as cursor:
                cursor.execute("SELECT * FROM users")
                results = cursor.fetchall()
        """
        if not self.connection:
            raise RuntimeError("DatabaseConnection connection is not established")

        cursor = self.connection.cursor(buffered=buffered)
        try:
            yield cursor
        finally:
            cursor.close()

    def commit(self) -> None:
        """Commit the current transaction.

        Raises:
            mysql.connector.Error: If commit fails.
        """
        if self.is_connected():
            self.connection.commit()