# Inspired by https://stackoverflow.com/questions/38076220/python-mysqldb-connection-in-a-class

from typing import Self
from config import DatabaseConfig
import config
from contextlib import contextmanager
from typing import Optional, List, Tuple, Any
import mysql.connector
from mysql.connector.abstracts import MySQLConnectionAbstract
from mysql.connector.connection import MySQLConnection
from mysql.connector.cursor import MySQLCursor


class Database:  
    def __init__(self, config: DatabaseConfig) -> None:
        """Initialize database connection.
        
        Args:
            config: DatabaseConfig object with connection parameters.
            
        Raises:
            mysql.connector.Error: If connection fails.
        """
        self.config = config
        self._connection: MySQLConnection  | None = None
        self.connect()

    
    def __enter__(self) -> 'Database':
        """Enter context manager.
        
        Returns:
            Self for use in with statement.
        """
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        """Exit context manager and close connection.
        
        Args:
            exc_type: Exception type if an exception occurred.
            exc_val: Exception value if an exception occurred.
            exc_tb: Exception traceback if an exception occurred.
            
        Returns:
            False to propagate any exception that occurred.
        """
        self._connection.close()
        return False
    
    def connect(self) -> None:
        """Establish connection to MySQL database.

        
        Raises:
            mysql.connector.Error: If connection fails.
        """
        config_dict = config.asdict(self.config)
        self._connection = mysql.connector.connect(**config_dict)

       
    @property
    def connection(self) -> MySQLConnection:
        """Get the underlying MySQL connection object.
        
        Returns:
            The mysql.connector connection object.
            
        Raises:
            RuntimeError: If connection is not established.
        """
        if not self._connection:
            raise RuntimeError("Database connection is not established")
        return self._connection
    
    def is_connected(self) -> bool:
        """Check if database connection is active.
        
        Returns:
            True if connected, False otherwise.
        """
        return self._connection is not None and self._connection.is_connected()
    
    @contextmanager
    def cursor(self, buffered: bool = True):
        """Create a cursor with automatic cleanup.
        
        This context manager ensures cursors are properly closed after use,
        preventing "Commands out of sync" errors.
        
        Args:
            buffered: If True, fetch all results immediately. Defaults to True
                to prevent synchronization issues.
                
        Yields:
            MySQLCursor: Database cursor for executing queries.
            
        Raises:
            RuntimeError: If connection is not established.
            
        Example:
            with db.cursor() as cursor:
                cursor.execute("SELECT * FROM users")
                results = cursor.fetchall()
        """
        if not self._connection:
            raise RuntimeError("Database connection is not established")
            
        cursor = self._connection.cursor(buffered=buffered)
        try:
            yield cursor
        finally:
            cursor.close()
    
        
    def commit(self) -> None:
        """Commit the current transaction.
        
        Raises:
            mysql.connector.Error: If commit fails.
        """
        if self._connection:
            self._connection.commit()

    
