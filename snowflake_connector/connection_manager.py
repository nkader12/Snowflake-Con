"""
Connection Manager for module-level API

Handles connection management with credential prompting.
"""

import logging
from typing import Optional
from .connection import SnowflakeConnection
from .data_retriever import DataRetriever
from .utils import get_input, get_password

logger = logging.getLogger(__name__)


class ConnectionManager:
    """
    Manages a single Snowflake connection at the module level.
    Prompts for credentials if not provided.
    """
    
    def __init__(self):
        """Initialize the connection manager."""
        self._connection: Optional[SnowflakeConnection] = None
        self._retriever: Optional[DataRetriever] = None
    
    def connect(
        self,
        account: Optional[str] = "GAIADLH-HPA09261",
        user: Optional[str] = None,
        password: Optional[str] = None,
        warehouse: Optional[str] = None,
        role: Optional[str] = None,
        **kwargs
    ):
        """
        Connect to Snowflake with interactive credential prompting.
        
        If credentials are not provided, they will be prompted interactively.
        MFA will be handled automatically if required.
        
        Args:
            account: Snowflake account identifier (default: 'GAIADLH-HPA09261').
                Can be overridden with a different account if needed.
            user: Snowflake username (prompted if not provided)
            password: Snowflake password (prompted securely if not provided)
            warehouse: Default warehouse to use (optional)
            role: Default role to use (optional)
            **kwargs: Additional connection parameters
        """
        # Account is hardcoded to GAIADLH-HPA09261 by default
        # Use default if explicitly set to None (allows override)
        if account is None:
            account = "GAIADLH-HPA09261"
        
        if user is None:
            user = get_input("Enter Snowflake username: ")
        if password is None:
            password = get_password("Enter Snowflake password: ")
        
        # Create connection (warehouse, database, schema are optional)
        self._connection = SnowflakeConnection(
            account=account,
            user=user,
            password=password,
            warehouse=warehouse,
            database=None,  # Not needed - user uses fully qualified names
            schema=None,    # Not needed - user uses fully qualified names
            role=role,
            authenticator="snowflake",
            **kwargs
        )
        
        # Connect (MFA will be prompted here if required)
        self._connection.connect()
        
        # Create data retriever
        self._retriever = DataRetriever(self._connection)
        
        print("Successfully connected to Snowflake!")
    
    def disconnect(self):
        """Disconnect from Snowflake."""
        if self._connection is not None:
            self._connection.disconnect()
            self._connection = None
            self._retriever = None
            print("Disconnected from Snowflake.")
    
    def is_connected(self) -> bool:
        """Check if currently connected."""
        return self._connection is not None and self._connection._is_connected
    
    def _ensure_connected(self):
        """Ensure we have an active connection."""
        if not self.is_connected():
            raise ConnectionError(
                "Not connected to Snowflake. Please call connect() first."
            )
        return self._retriever
    
    def query(self, sql: str, chunk_size: Optional[int] = None, **kwargs):
        """
        Execute a SQL query and return results as a pandas DataFrame.
        
        Args:
            sql: SQL query to execute
            chunk_size: If specified, fetch data in chunks of this size
            **kwargs: Additional parameters for cursor.execute()
            
        Returns:
            pandas DataFrame with query results
        """
        retriever = self._ensure_connected()
        return retriever.fetch_pandas(sql, chunk_size=chunk_size, **kwargs)
    
    def query_batches(self, sql: str, batch_size: int = 100000, **kwargs):
        """
        Execute a SQL query and return results as an iterator of pandas DataFrames.
        
        Memory-efficient for very large datasets.
        
        Args:
            sql: SQL query to execute
            batch_size: Number of rows per batch (default: 100,000)
            **kwargs: Additional parameters for cursor.execute()
            
        Yields:
            pandas DataFrame batches
        """
        retriever = self._ensure_connected()
        return retriever.fetch_pandas_batches(sql, batch_size=batch_size, **kwargs)
    
    def query_dict(self, sql: str, chunk_size: Optional[int] = None, **kwargs):
        """
        Execute a SQL query and return results as a list of dictionaries.
        
        Args:
            sql: SQL query to execute
            chunk_size: If specified, fetch data in chunks of this size
            **kwargs: Additional parameters for cursor.execute()
            
        Returns:
            List of dictionaries, each representing a row
        """
        retriever = self._ensure_connected()
        return retriever.fetch_dict(sql, chunk_size=chunk_size, **kwargs)
    
    def query_dict_batches(self, sql: str, batch_size: int = 100000, **kwargs):
        """
        Execute a SQL query and return results as an iterator of dictionary lists.
        
        Args:
            sql: SQL query to execute
            batch_size: Number of rows per batch (default: 100,000)
            **kwargs: Additional parameters for cursor.execute()
            
        Yields:
            Lists of dictionaries, each representing a batch of rows
        """
        retriever = self._ensure_connected()
        return retriever.fetch_dict_batches(sql, batch_size=batch_size, **kwargs)
    
    def execute(self, sql: str, **kwargs):
        """
        Execute a SQL query without returning results (e.g., DDL, DML operations).
        
        Args:
            sql: SQL query to execute
            **kwargs: Additional parameters for cursor.execute()
            
        Returns:
            Cursor object
        """
        retriever = self._ensure_connected()
        return retriever.execute(sql, **kwargs)
    
    def fetch_one(self, sql: str, **kwargs) -> Optional[tuple]:
        """
        Execute a SQL query and return a single row.
        
        Args:
            sql: SQL query to execute
            **kwargs: Additional parameters for cursor.execute()
            
        Returns:
            Single row as tuple, or None if no results
        """
        retriever = self._ensure_connected()
        return retriever.fetch_one(sql, **kwargs)
    
    def fetch_all(self, sql: str, **kwargs):
        """
        Execute a SQL query and return all rows as tuples.
        
        Args:
            sql: SQL query to execute
            **kwargs: Additional parameters for cursor.execute()
            
        Returns:
            List of tuples, each representing a row
        """
        retriever = self._ensure_connected()
        return retriever.fetch_all(sql, **kwargs)

