"""
Snowflake Connection Manager

Handles connection to Snowflake with MFA authentication.
MFA is only required once during initial connection.
"""

import snowflake.connector
from typing import Optional, Dict, Any
import logging

logger = logging.getLogger(__name__)


class SnowflakeConnection:
    """
    Manages Snowflake connection with MFA authentication.
    
    The connection is cached so MFA is only required once per session,
    not for each query execution.
    """
    
    def __init__(
        self,
        account: str,
        user: str,
        password: str,
        warehouse: Optional[str] = None,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        role: Optional[str] = None,
        authenticator: str = "snowflake",
        **kwargs
    ):
        """
        Initialize Snowflake connection parameters.
        
        Args:
            account: Snowflake account identifier
            user: Snowflake username
            password: Snowflake password
            warehouse: Default warehouse to use
            database: Default database to use
            schema: Default schema to use
            role: Default role to use
            authenticator: Authentication method (default: 'snowflake' for MFA)
            **kwargs: Additional connection parameters
        """
        self.account = account
        self.user = user
        self.password = password
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self.role = role
        self.authenticator = authenticator
        self.extra_params = kwargs
        
        self._connection: Optional[snowflake.connector.SnowflakeConnection] = None
        self._is_connected = False
    
    def connect(self) -> snowflake.connector.SnowflakeConnection:
        """
        Establish connection to Snowflake with MFA authentication.
        
        This method will prompt for MFA token if required.
        The connection is cached and reused for subsequent queries.
        
        Returns:
            Snowflake connection object
            
        Raises:
            Exception: If connection fails
        """
        if self._is_connected and self._connection is not None:
            try:
                # Test if connection is still alive
                self._connection.cursor().execute("SELECT 1")
                return self._connection
            except Exception:
                logger.warning("Cached connection is dead, reconnecting...")
                self._is_connected = False
                self._connection = None
        
        try:
            logger.info(f"Connecting to Snowflake account: {self.account} as user: {self.user}")
            
            connection_params = {
                "account": self.account,
                "user": self.user,
                "password": self.password,
                "authenticator": self.authenticator,
                **self.extra_params
            }
            
            # Add optional parameters if provided
            if self.warehouse:
                connection_params["warehouse"] = self.warehouse
            if self.database:
                connection_params["database"] = self.database
            if self.schema:
                connection_params["schema"] = self.schema
            if self.role:
                connection_params["role"] = self.role
            
            self._connection = snowflake.connector.connect(**connection_params)
            self._is_connected = True
            
            logger.info("Successfully connected to Snowflake")
            return self._connection
            
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {str(e)}")
            self._is_connected = False
            self._connection = None
            raise
    
    def get_connection(self) -> snowflake.connector.SnowflakeConnection:
        """
        Get the active Snowflake connection.
        Connects automatically if not already connected.
        
        Returns:
            Snowflake connection object
        """
        return self.connect()
    
    def disconnect(self):
        """
        Close the Snowflake connection.
        """
        if self._connection is not None:
            try:
                self._connection.close()
                logger.info("Disconnected from Snowflake")
            except Exception as e:
                logger.warning(f"Error during disconnect: {str(e)}")
            finally:
                self._connection = None
                self._is_connected = False
    
    def execute_query(self, query: str, **kwargs) -> Any:
        """
        Execute a SQL query using the cached connection.
        MFA is not required for this operation.
        
        Args:
            query: SQL query to execute
            **kwargs: Additional parameters for cursor.execute()
            
        Returns:
            Cursor object with query results
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(query, **kwargs)
        return cursor
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()
    
    def __del__(self):
        """Cleanup on object deletion."""
        self.disconnect()

