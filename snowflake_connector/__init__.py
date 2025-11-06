"""
Snowflake Connector Package

A custom Python package for connecting to Snowflake with MFA authentication
and efficient data retrieval capabilities.

Simple usage:
    import snowflake_connector as con
    con.connect()  # Prompts for credentials
    df = con.query("SELECT * FROM my_table")
"""

from typing import Optional, Iterator, List, Dict, Any
import pandas as pd
import logging

from .connection_manager import ConnectionManager
from .utils import get_input, get_password

# Module-level connection manager
_connection_manager: Optional[ConnectionManager] = None

logger = logging.getLogger(__name__)


def connect(
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
    global _connection_manager
    
    # Account is hardcoded to GAIADLH-HPA09261 by default
    # Use default if explicitly set to None (allows override)
    if account is None:
        account = "GAIADLH-HPA09261"
    
    if user is None:
        user = get_input("Enter Snowflake username: ")
    if password is None:
        password = get_password("Enter Snowflake password: ")
    
    # Create connection manager
    _connection_manager = ConnectionManager()
    
    # Connect (MFA will be prompted here if required)
    _connection_manager.connect(
        account=account,
        user=user,
        password=password,
        warehouse=warehouse,
        role=role,
        **kwargs
    )


def disconnect():
    """Disconnect from Snowflake."""
    global _connection_manager
    if _connection_manager is not None:
        _connection_manager.disconnect()
        _connection_manager = None


def _ensure_connected():
    """Ensure we have an active connection."""
    global _connection_manager
    if _connection_manager is None or not _connection_manager.is_connected():
        raise ConnectionError(
            "Not connected to Snowflake. Please call connect() first."
        )
    return _connection_manager


def query(
    sql: str,
    chunk_size: Optional[int] = None,
    **kwargs
) -> pd.DataFrame:
    """
    Execute a SQL query and return results as a pandas DataFrame.
    
    Args:
        sql: SQL query to execute
        chunk_size: If specified, fetch data in chunks of this size
        **kwargs: Additional parameters for cursor.execute()
        
    Returns:
        pandas DataFrame with query results
    """
    manager = _ensure_connected()
    return manager.query(sql, chunk_size=chunk_size, **kwargs)


def query_batches(
    sql: str,
    batch_size: int = 100000,
    **kwargs
) -> Iterator[pd.DataFrame]:
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
    manager = _ensure_connected()
    return manager.query_batches(sql, batch_size=batch_size, **kwargs)


def query_dict(
    sql: str,
    chunk_size: Optional[int] = None,
    **kwargs
) -> List[Dict[str, Any]]:
    """
    Execute a SQL query and return results as a list of dictionaries.
    
    Args:
        sql: SQL query to execute
        chunk_size: If specified, fetch data in chunks of this size
        **kwargs: Additional parameters for cursor.execute()
        
    Returns:
        List of dictionaries, each representing a row
    """
    manager = _ensure_connected()
    return manager.query_dict(sql, chunk_size=chunk_size, **kwargs)


def query_dict_batches(
    sql: str,
    batch_size: int = 100000,
    **kwargs
) -> Iterator[List[Dict[str, Any]]]:
    """
    Execute a SQL query and return results as an iterator of dictionary lists.
    
    Args:
        sql: SQL query to execute
        batch_size: Number of rows per batch (default: 100,000)
        **kwargs: Additional parameters for cursor.execute()
        
    Yields:
        Lists of dictionaries, each representing a batch of rows
    """
    manager = _ensure_connected()
    return manager.query_dict_batches(sql, batch_size=batch_size, **kwargs)


def execute(
    sql: str,
    **kwargs
) -> Any:
    """
    Execute a SQL query without returning results (e.g., DDL, DML operations).
    
    Args:
        sql: SQL query to execute
        **kwargs: Additional parameters for cursor.execute()
        
    Returns:
        Cursor object
    """
    manager = _ensure_connected()
    return manager.execute(sql, **kwargs)


def fetch_one(sql: str, **kwargs) -> Optional[tuple]:
    """
    Execute a SQL query and return a single row.
    
    Args:
        sql: SQL query to execute
        **kwargs: Additional parameters for cursor.execute()
        
    Returns:
        Single row as tuple, or None if no results
    """
    manager = _ensure_connected()
    return manager.fetch_one(sql, **kwargs)


def fetch_all(sql: str, **kwargs) -> List[tuple]:
    """
    Execute a SQL query and return all rows as tuples.
    
    Args:
        sql: SQL query to execute
        **kwargs: Additional parameters for cursor.execute()
        
    Returns:
        List of tuples, each representing a row
    """
    manager = _ensure_connected()
    return manager.fetch_all(sql, **kwargs)


__version__ = "0.1.0"
__all__ = [
    "connect",
    "disconnect",
    "query",
    "query_batches",
    "query_dict",
    "query_dict_batches",
    "execute",
    "fetch_one",
    "fetch_all",
]
